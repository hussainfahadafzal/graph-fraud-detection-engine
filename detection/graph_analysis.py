import time
from datetime import timedelta

import networkx as nx
import numpy as np
import pandas as pd


def _validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strict validation for the RIFT 2026 Money Muling schema.
    Raises ValueError with human-readable messages on failure.
    """
    df = df.dropna(how="all")

    required_columns = [
        "transaction_id",
        "sender_id",
        "receiver_id",
        "amount",
        "timestamp",
    ]

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    if df["amount"].isna().any():
        bad_rows = df[df["amount"].isna()].index.tolist()[:10]
        raise ValueError(
            f"Found non-numeric values in 'amount' column (example row indices: {bad_rows})."
        )

    df["timestamp"] = pd.to_datetime(
        df["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )
    if df["timestamp"].isna().any():
        bad_rows = df[df["timestamp"].isna()].index.tolist()[:10]
        raise ValueError(
            "Timestamps must use format YYYY-MM-DD HH:MM:SS. "
            f"Invalid timestamp(s) detected (example row indices: {bad_rows})."
        )

    df = df.dropna(
        subset=["transaction_id", "sender_id", "receiver_id", "amount", "timestamp"]
    )

    return df


def _build_graph(df: pd.DataFrame) -> nx.DiGraph:
    """Build directed graph with aggregated edge metadata."""
    G = nx.DiGraph()

    grouped = (
        df.groupby(["sender_id", "receiver_id"])
        .agg(
            transaction_ids=("transaction_id", list),
            total_amount=("amount", "sum"),
            count=("transaction_id", "count"),
            first_timestamp=("timestamp", "min"),
            last_timestamp=("timestamp", "max"),
        )
        .reset_index()
    )

    for _, row in grouped.iterrows():
        G.add_edge(
            row["sender_id"],
            row["receiver_id"],
            transaction_ids=row["transaction_ids"],
            total_amount=float(row["total_amount"]),
            count=int(row["count"]),
            first_timestamp=row["first_timestamp"],
            last_timestamp=row["last_timestamp"],
        )

    return G


def _detect_cycles_3_to_5(G: nx.DiGraph):
    """
    Detect circular fund routing: cycles of length 3 to 5 only.
    Spec: A→B→C→A; all accounts in cycle = same ring.
    """
    fraud_rings = []
    node_to_ring: dict[str, str] = {}
    ring_counter = 1
    seen_rings: set[frozenset] = set()

    try:
        cycles = nx.simple_cycles(G)
    except (nx.NetworkXNoCycle, nx.NetworkXError):
        return fraud_rings, node_to_ring

    for cycle in cycles:
        if not (3 <= len(cycle) <= 5):
            continue

        members = frozenset(cycle)
        if members in seen_rings:
            continue
        seen_rings.add(members)

        ring_id = f"RING_{ring_counter:03d}"
        ring_counter += 1
        member_list = sorted(cycle)

        subgraph = G.subgraph(cycle)
        density = nx.density(subgraph) if len(cycle) > 1 else 0.0
        risk_score = float(
            min(100.0, 50.0 + 30.0 * density + 20.0 * (len(cycle) / 5.0))
        )

        fraud_rings.append(
            {
                "ring_id": ring_id,
                "member_count": len(member_list),
                "member_accounts": member_list,
                "risk_score": round(risk_score, 2),
                "pattern_type": "cycle",
            }
        )

        for node in cycle:
            node_to_ring.setdefault(node, ring_id)

    return fraud_rings, node_to_ring


def _detect_smurfing_fan_in(df: pd.DataFrame, window_hours: int = 72, min_senders: int = 10):
    """
    Fan-in: 10+ senders → 1 receiver within 72-hour window.
    Spec: Many small deposits aggregated into one account.
    """
    flagged_receivers: set[str] = set()
    window = timedelta(hours=window_hours)

    for receiver, group in df.groupby("receiver_id"):
        rows = group[["sender_id", "timestamp"]].drop_duplicates()
        rows = rows.sort_values("timestamp").reset_index(drop=True)
        if len(rows) < min_senders:
            continue

        times = rows["timestamp"].tolist()
        senders = rows["sender_id"].tolist()

        start = 0
        for end in range(len(times)):
            while times[end] - times[start] > window:
                start += 1
            window_senders = set(senders[i] for i in range(start, end + 1))
            if len(window_senders) >= min_senders:
                flagged_receivers.add(receiver)
                break

    return flagged_receivers


def _detect_smurfing_fan_out(df: pd.DataFrame, window_hours: int = 72, min_receivers: int = 10):
    """
    Fan-out: 1 sender → 10+ receivers within 72-hour window.
    Spec: One account disperses to many to avoid thresholds.
    """
    flagged_senders: set[str] = set()
    window = timedelta(hours=window_hours)

    for sender, group in df.groupby("sender_id"):
        rows = group[["receiver_id", "timestamp"]].drop_duplicates()
        rows = rows.sort_values("timestamp").reset_index(drop=True)
        if len(rows) < min_receivers:
            continue

        times = rows["timestamp"].tolist()
        receivers = rows["receiver_id"].tolist()

        start = 0
        for end in range(len(times)):
            while times[end] - times[start] > window:
                start += 1
            window_receivers = set(receivers[i] for i in range(start, end + 1))
            if len(window_receivers) >= min_receivers:
                flagged_senders.add(sender)
                break

    return flagged_senders


def _detect_layered_shell(G: nx.DiGraph, df: pd.DataFrame):
    """
    Layered shell networks: chains of 3+ hops where intermediate
    accounts have only 2-3 total transactions.
    """
    tx_count = (
        df.groupby("sender_id").size().add(
            df.groupby("receiver_id").size(), fill_value=0
        ).astype(int)
    )

    shell_accounts: set[str] = set()

    for node in G.nodes():
        total_tx = int(tx_count.get(node, 0))
        if not (2 <= total_tx <= 3):
            continue

        in_deg = G.in_degree(node)
        out_deg = G.out_degree(node)
        if in_deg < 1 or out_deg < 1:
            continue

        preds_dist1 = set(G.predecessors(node))
        succs_dist1 = set(G.successors(node))

        preds_dist2: set[str] = set()
        for p in preds_dist1:
            preds_dist2.update(G.predecessors(p))

        succs_dist2: set[str] = set()
        for s in succs_dist1:
            succs_dist2.update(G.successors(s))

        has_3hop = (
            (preds_dist1 and succs_dist2) or
            (preds_dist2 and succs_dist1)
        )
        if has_3hop:
            shell_accounts.add(node)

    return shell_accounts


def _is_likely_legitimate(
    node: str,
    G: nx.DiGraph,
    df: pd.DataFrame,
    tx_count: pd.Series,
    incoming_amounts: dict,
    outgoing_amounts: dict,
) -> bool:
    """
    Avoid flagging high-volume merchants or payroll accounts.
    Heuristics: very high tx count, balanced flow, high avg amount.
    """
    total_tx = int(tx_count.get(node, 0))
    if total_tx > 200:
        return True

    total_in = float(incoming_amounts.get(node, 0.0))
    total_out = float(outgoing_amounts.get(node, 0.0))
    total_flow = total_in + total_out

    if total_flow <= 0:
        return False

    ratio = min(total_in, total_out) / max(total_in, total_out)
    in_deg = G.in_degree(node)
    out_deg = G.out_degree(node)

    if total_tx > 80 and ratio > 0.85 and (in_deg + out_deg) > 15:
        return True

    avg_amount = total_flow / total_tx
    if avg_amount > 50000 and total_tx > 30:
        return True

    return False


def analyze_transactions(df: pd.DataFrame) -> dict:
    """
    Core graph-based fraud analysis. Implements RIFT 2026 spec:
    - Cycles 3-5, Smurfing (Fan-in/Fan-out, 72h), Layered Shell.
    - False positive control for merchants/payroll.
    """
    start_time = time.time()

    df = _validate_and_clean(df)
    if df.empty:
        raise ValueError("CSV has no valid rows after cleaning.")

    G = _build_graph(df)
    if G.number_of_nodes() == 0:
        raise ValueError("No accounts found in the transaction network.")

    tx_count = (
        df.groupby("sender_id").size().add(
            df.groupby("receiver_id").size(), fill_value=0
        )
    )
    incoming_amounts = df.groupby("receiver_id")["amount"].sum().to_dict()
    outgoing_amounts = df.groupby("sender_id")["amount"].sum().to_dict()

    fraud_rings, node_to_ring = _detect_cycles_3_to_5(G)
    fan_in_accounts = _detect_smurfing_fan_in(df)
    fan_out_accounts = _detect_smurfing_fan_out(df)
    shell_accounts = _detect_layered_shell(G, df)

    nodes_payload = []
    suspicious_accounts = []

    for node in G.nodes():
        in_deg = G.in_degree(node)
        out_deg = G.out_degree(node)
        degree = in_deg + out_deg

        total_in = float(incoming_amounts.get(node, 0.0))
        total_out = float(outgoing_amounts.get(node, 0.0))

        patterns: list[str] = []
        score = 0.0

        skip_patterns = _is_likely_legitimate(
            node, G, df, tx_count, incoming_amounts, outgoing_amounts
        )

        if not skip_patterns and node in node_to_ring:
            cycle_len = next(
                len(r["member_accounts"])
                for r in fraud_rings
                if r["ring_id"] == node_to_ring[node]
            )
            patterns.append(f"cycle_length_{cycle_len}")
            score += 40.0

        if not skip_patterns and node in fan_in_accounts:
            patterns.append("smurfing_fan_in")
            score += 28.0

        if not skip_patterns and node in fan_out_accounts:
            patterns.append("smurfing_fan_out")
            score += 28.0

        if not skip_patterns and node in shell_accounts:
            patterns.append("layered_shell")
            score += 25.0

        if not skip_patterns and not patterns and degree > 0:
            score += min(8.0, 1.5 * float(np.log1p(degree)))

        score = float(min(100.0, round(score, 2)))
        is_suspicious = score > 0.0

        node_payload = {
            "account_id": node,
            "suspicion_score": score,
            "patterns": patterns,
            "ring_id": node_to_ring.get(node),
            "in_degree": in_deg,
            "out_degree": out_deg,
            "total_in_amount": round(total_in, 2),
            "total_out_amount": round(total_out, 2),
            "is_suspicious": is_suspicious,
        }
        nodes_payload.append(node_payload)

        if is_suspicious:
            suspicious_accounts.append(node_payload)

    nodes_payload_sorted = sorted(
        nodes_payload, key=lambda x: x["suspicion_score"], reverse=True
    )
    suspicious_accounts_sorted = sorted(
        suspicious_accounts, key=lambda x: x["suspicion_score"], reverse=True
    )

    edges_payload = []
    for u, v, data in G.edges(data=True):
        edges_payload.append(
            {
                "source": u,
                "target": v,
                "transaction_count": int(data.get("count", 1)),
                "total_amount": float(data.get("total_amount", 0.0)),
                "first_timestamp": data.get("first_timestamp").strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if data.get("first_timestamp")
                else None,
                "last_timestamp": data.get("last_timestamp").strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if data.get("last_timestamp")
                else None,
            }
        )

    processing_time = round(time.time() - start_time, 3)

    summary_stats = {
        "total_transactions": int(len(df)),
        "total_accounts": int(G.number_of_nodes()),
        "suspicious_accounts": int(len(suspicious_accounts_sorted)),
        "fraud_rings": int(len(fraud_rings)),
        "highest_risk_score": float(
            max((ring["risk_score"] for ring in fraud_rings), default=0.0)
        ),
        "max_suspicion_score": float(
            max((n["suspicion_score"] for n in nodes_payload_sorted), default=0.0)
        ),
        "processing_time_seconds": processing_time,
    }

    return {
        "nodes": nodes_payload_sorted,
        "edges": edges_payload,
        "suspicious_accounts": suspicious_accounts_sorted,
        "fraud_rings": fraud_rings,
        "summary_stats": summary_stats,
    }
