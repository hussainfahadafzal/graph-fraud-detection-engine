import time
from datetime import timedelta

import networkx as nx
import numpy as np
import pandas as pd


def _time_up(deadline: float | None) -> bool:
    return deadline is not None and time.perf_counter() >= deadline


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


def _detect_cycles_3_to_5(
    G: nx.DiGraph,
    max_runtime_seconds: float = 6.0,
    max_rings: int = 500,
):
    """
    Detect circular fund routing: cycles of length 3 to 5 only.
    Spec: A→B→C→A; all accounts in cycle = same ring.
    """
    fraud_rings = []
    node_to_ring: dict[str, str] = {}
    ring_counter = 1
    seen_rings: set[frozenset] = set()

    deadline = (
        time.perf_counter() + max_runtime_seconds
        if max_runtime_seconds > 0
        else None
    )
    successors = {node: set(G.successors(node)) for node in G.nodes()}

    def add_cycle(cycle_nodes: list[str]):
        nonlocal ring_counter
        # Keep ring assignment stable: one account belongs to at most one ring.
        # Without this, dense subgraphs can generate many overlapping cycles and
        # inflate ring counts far beyond meaningful values.
        if any(node in node_to_ring for node in cycle_nodes):
            return

        # Require stronger cycle support to reduce false positives from
        # incidental sparse loops in large graphs.
        cycle_edges = []
        for i in range(len(cycle_nodes)):
            src = cycle_nodes[i]
            dst = cycle_nodes[(i + 1) % len(cycle_nodes)]
            edge_data = G.get_edge_data(src, dst, default={})
            cycle_edges.append(int(edge_data.get("count", 1)))

        # Keep cycle quality guard light to avoid dropping true positives
        # in sparse datasets where each loop edge appears once.
        if sum(cycle_edges) < len(cycle_nodes):
            return

        members = frozenset(cycle_nodes)
        if members in seen_rings:
            return
        seen_rings.add(members)

        ring_id = f"RING_{ring_counter:03d}"
        ring_counter += 1
        member_list = sorted(cycle_nodes)

        subgraph = G.subgraph(cycle_nodes)
        density = nx.density(subgraph) if len(cycle_nodes) > 1 else 0.0
        risk_score = float(
            min(100.0, 50.0 + 30.0 * density + 20.0 * (len(cycle_nodes) / 5.0))
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

        for node in member_list:
            node_to_ring.setdefault(node, ring_id)

    nodes_sorted = sorted(G.nodes(), key=str)
    for start in nodes_sorted:
        if _time_up(deadline) or len(fraud_rings) >= max_rings:
            break

        start_key = str(start)
        stack: list[tuple[str, list[str], set[str]]] = [(start, [start], {start})]

        while stack:
            if _time_up(deadline) or len(fraud_rings) >= max_rings:
                return fraud_rings, node_to_ring

            current, path, visited = stack.pop()
            path_len = len(path)

            if 3 <= path_len <= 5 and start in successors.get(current, set()):
                add_cycle(path.copy())
                if len(fraud_rings) >= max_rings:
                    return fraud_rings, node_to_ring

            if path_len == 5:
                continue

            for nxt in successors.get(current, set()):
                if nxt in visited:
                    continue
                if str(nxt) < start_key:
                    continue
                stack.append((nxt, path + [nxt], visited | {nxt}))

    return fraud_rings, node_to_ring


def _detect_smurfing_fan_in(
    df: pd.DataFrame,
    window_hours: int = 72,
    min_senders: int = 10,
    deadline: float | None = None,
):
    """
    Fan-in: 10+ senders → 1 receiver within 72-hour window.
    Spec: Many small deposits aggregated into one account.
    """
    flagged_receivers: set[str] = set()
    window = timedelta(hours=window_hours)

    for receiver, group in df.groupby("receiver_id"):
        if _time_up(deadline):
            break
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


def _detect_smurfing_fan_out(
    df: pd.DataFrame,
    window_hours: int = 72,
    min_receivers: int = 10,
    deadline: float | None = None,
):
    """
    Fan-out: 1 sender → 10+ receivers within 72-hour window.
    Spec: One account disperses to many to avoid thresholds.
    """
    flagged_senders: set[str] = set()
    window = timedelta(hours=window_hours)

    for sender, group in df.groupby("sender_id"):
        if _time_up(deadline):
            break
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


def _detect_layered_shell(
    G: nx.DiGraph,
    df: pd.DataFrame,
    deadline: float | None = None,
):
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
        if _time_up(deadline):
            break
        total_tx = int(tx_count.get(node, 0))
        if not (2 <= total_tx <= 3):
            continue

        in_deg = G.in_degree(node)
        out_deg = G.out_degree(node)
        # Stricter shell condition to avoid over-flagging low-activity noise.
        if in_deg < 2 or out_deg < 2:
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


def analyze_transactions(df: pd.DataFrame, max_runtime_seconds: float = 10.0) -> dict:
    """
    Core graph-based fraud analysis. Implements RIFT 2026 spec:
    - Cycles 3-5, Smurfing (Fan-in/Fan-out, 72h), Layered Shell.
    - False positive control for merchants/payroll.
    """
    start_time = time.perf_counter()
    deadline = (
        start_time + max_runtime_seconds
        if max_runtime_seconds and max_runtime_seconds > 0
        else None
    )

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

    cycle_budget = max(2.0, min(6.0, max_runtime_seconds * 0.6))
    fraud_rings, node_to_ring = _detect_cycles_3_to_5(
        G, max_runtime_seconds=cycle_budget
    )
    fan_in_accounts = (
        _detect_smurfing_fan_in(df, deadline=deadline) if not _time_up(deadline) else set()
    )
    fan_out_accounts = (
        _detect_smurfing_fan_out(df, deadline=deadline) if not _time_up(deadline) else set()
    )
    shell_accounts = (
        _detect_layered_shell(G, df, deadline=deadline) if not _time_up(deadline) else set()
    )
    ring_length_map = {
        ring["ring_id"]: ring["member_count"] for ring in fraud_rings
    }

    nodes_payload = []
    scored_accounts: list[tuple[dict, bool, int, int]] = []

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
            cycle_len = int(ring_length_map.get(node_to_ring[node], 0))
            patterns.append(f"cycle_length_{cycle_len}")
            score += 50.0 if cycle_len == 3 else 46.0

        if not skip_patterns and node in fan_in_accounts:
            patterns.append("smurfing_fan_in")
            score += 20.0

        if not skip_patterns and node in fan_out_accounts:
            patterns.append("smurfing_fan_out")
            score += 20.0

        if not skip_patterns and node in shell_accounts:
            patterns.append("layered_shell")
            score += 16.0

        total_tx = int(tx_count.get(node, 0))
        if not skip_patterns and degree > 0:
            # Behavioral risk signals complement pattern hits without overwhelming them.
            score += min(10.0, 1.2 * float(np.log1p(degree)))
            score += min(8.0, 1.4 * float(np.log1p(total_tx)))

            total_flow = total_in + total_out
            if total_flow > 0:
                imbalance = abs(total_in - total_out) / total_flow
                score += 8.0 * float(imbalance)

        pattern_count = len(patterns)
        if pattern_count >= 2:
            score += 6.0
        if pattern_count >= 3:
            score += 6.0

        if skip_patterns:
            # Legitimate merchant/payroll-like profiles are strongly down-weighted.
            score *= 0.35

        score = float(min(100.0, round(score, 2)))

        has_cycle = any(p.startswith("cycle_length_") for p in patterns)
        high_confidence = (
            (has_cycle and total_tx >= 2 and score >= 45.0)
            or (pattern_count >= 2 and total_tx >= 4 and score >= 52.0)
            or (pattern_count >= 3 and total_tx >= 3 and score >= 50.0)
            or (pattern_count == 1 and total_tx >= 8 and score >= 68.0)
        )

        node_payload = {
            "account_id": node,
            "suspicion_score": score,
            "patterns": patterns,
            "ring_id": node_to_ring.get(node),
            "in_degree": in_deg,
            "out_degree": out_deg,
            "total_in_amount": round(total_in, 2),
            "total_out_amount": round(total_out, 2),
            "is_suspicious": False,
        }
        nodes_payload.append(node_payload)
        scored_accounts.append((node_payload, high_confidence, pattern_count, total_tx))

    # Dataset-adaptive thresholding to avoid over/under-flagging across
    # different CSV distributions while preserving high-confidence rules.
    high_conf_scores = [
        item["suspicion_score"] for item, is_high, _, _ in scored_accounts if is_high
    ]
    dynamic_threshold = 100.0
    max_suspicious_ratio = 0.2
    if high_conf_scores:
        percentile_75 = float(np.percentile(high_conf_scores, 75))
        dynamic_threshold = max(55.0, percentile_75)

    suspicious_accounts = []
    if high_conf_scores:
        for item, is_high, _, _ in scored_accounts:
            is_suspicious = is_high and item["suspicion_score"] >= dynamic_threshold
            item["is_suspicious"] = is_suspicious
            if is_suspicious:
                suspicious_accounts.append(item)
    else:
        # Fallback path: allow strongest single-pattern entities when no
        # high-confidence matches exist in the dataset.
        fallback_scores = [
            item["suspicion_score"]
            for item, _, pattern_count, total_tx in scored_accounts
            if pattern_count >= 1 and total_tx >= 3
        ]
        fallback_threshold = (
            max(58.0, float(np.percentile(fallback_scores, 90)))
            if fallback_scores
            else 100.0
        )
        dynamic_threshold = fallback_threshold

        for item, _, pattern_count, total_tx in scored_accounts:
            is_suspicious = (
                pattern_count >= 1
                and total_tx >= 3
                and item["suspicion_score"] >= dynamic_threshold
            )
            item["is_suspicious"] = is_suspicious
            if is_suspicious:
                suspicious_accounts.append(item)

    # Precision guardrail: cap suspicious accounts to top N by score.
    # This prevents broad over-flagging in noisy, high-connectivity datasets.
    max_allowed = max(1, int(len(nodes_payload) * max_suspicious_ratio))
    if len(suspicious_accounts) > max_allowed:
        suspicious_accounts = sorted(
            suspicious_accounts,
            key=lambda x: x["suspicion_score"],
            reverse=True,
        )[:max_allowed]
        allowed_ids = {acc["account_id"] for acc in suspicious_accounts}
        for item in nodes_payload:
            item["is_suspicious"] = item["account_id"] in allowed_ids

    # Safety net: if rings exist but thresholding removed all suspicious flags,
    # keep strongest ring-linked accounts so true cycle behavior is surfaced.
    if not suspicious_accounts and fraud_rings:
        ring_linked = [
            item for item in nodes_payload
            if item.get("ring_id") and item.get("suspicion_score", 0.0) >= 45.0
        ]
        ring_linked = sorted(
            ring_linked,
            key=lambda x: x["suspicion_score"],
            reverse=True,
        )[:max(1, min(10, len(ring_linked)))]
        allowed_ids = {acc["account_id"] for acc in ring_linked}
        for item in nodes_payload:
            item["is_suspicious"] = item["account_id"] in allowed_ids
        suspicious_accounts = ring_linked

    nodes_payload_sorted = sorted(
        nodes_payload, key=lambda x: x["suspicion_score"], reverse=True
    )
    suspicious_accounts_sorted = sorted(
        suspicious_accounts, key=lambda x: x["suspicion_score"], reverse=True
    )

    edges_payload = []
    for u, v, data in G.edges(data=True):
        tx_ids = data.get("transaction_ids", []) or []
        edges_payload.append(
            {
                "source": u,
                "target": v,
                "transaction_count": int(data.get("count", 1)),
                "total_amount": float(data.get("total_amount", 0.0)),
                "sample_transaction_ids": tx_ids[:5],
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

    processing_time = round(time.perf_counter() - start_time, 3)

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
        "suspicion_threshold": float(round(dynamic_threshold, 2)),
        "processing_time_seconds": processing_time,
    }

    return {
        "nodes": nodes_payload_sorted,
        "edges": edges_payload,
        "suspicious_accounts": suspicious_accounts_sorted,
        "fraud_rings": fraud_rings,
        "summary_stats": summary_stats,
    }
