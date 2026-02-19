import pandas as pd
import networkx as nx
import time
from datetime import timedelta

def analyze_graph(df):

    start_time = time.time()

    # 🔹 Ensure correct columns exist
    required_columns = [
        "transaction_id",
        "sender_id",
        "receiver_id",
        "amount",
        "timestamp"
    ]

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # 🔹 Clean Data
    df = df.dropna()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    # 🔹 Build Directed Graph
    G = nx.DiGraph()

    for _, row in df.iterrows():
        G.add_edge(
            row["sender_id"],
            row["receiver_id"],
            weight=row["amount"],
            timestamp=row["timestamp"]
        )

    suspicious_accounts = {}
    fraud_rings = []
    ring_counter = 1

    # 🔥 1️⃣ Cycle Detection (3–5 length)
    cycles = list(nx.simple_cycles(G))

    for cycle in cycles:
        if 3 <= len(cycle) <= 5:
            ring_id = f"RING_{ring_counter:03d}"
            ring_counter += 1

            fraud_rings.append({
                "ring_id": ring_id,
                "member_accounts": cycle,
                "pattern_type": "cycle",
                "risk_score": 90.0
            })

            for node in cycle:
                suspicious_accounts[node] = suspicious_accounts.get(node, 0) + 40

    # 🔥 2️⃣ Smurfing Detection (≥10 within 72 hours)
    for node in G.nodes():
        in_edges = list(G.in_edges(node, data=True))
        timestamps = [data["timestamp"] for _, _, data in in_edges]

        for i in range(len(timestamps)):
            count = sum(
                1 for t in timestamps
                if abs((t - timestamps[i]).total_seconds()) <= 72 * 3600
            )

            if count >= 10:
                suspicious_accounts[node] = suspicious_accounts.get(node, 0) + 30
                break

    # 🔥 3️⃣ High Connectivity
    for node in G.nodes():
        degree = G.in_degree(node) + G.out_degree(node)
        if degree > 6:
            suspicious_accounts[node] = suspicious_accounts.get(node, 0) + 20

        # 🔹 Prepare full node list
    all_nodes = list(G.nodes())

    suspicious_list = []

    for node in all_nodes:
        score = suspicious_accounts.get(node, 0)

        suspicious_list.append({
            "account_id": node,
            "suspicion_score": min(100, score),
            "detected_patterns": [],
            "ring_id": None
        })

    suspicious_list = sorted(
        suspicious_list,
        key=lambda x: x["suspicion_score"],
        reverse=True
    )

    # 🔹 Prepare edge list
    edges = [
        {
            "source": u,
            "target": v
        }
        for u, v in G.edges()
    ]

    processing_time = round(time.time() - start_time, 2)

    return {
        "nodes": suspicious_list,
        "edges": edges,
        "fraud_rings": fraud_rings,
        "summary": {
            "total_accounts_analyzed": len(G.nodes()),
            "suspicious_accounts_flagged": len([n for n in suspicious_list if n["suspicion_score"] > 0]),
            "fraud_rings_detected": len(fraud_rings),
            "processing_time_seconds": processing_time
        }
    }

