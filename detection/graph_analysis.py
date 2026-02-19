import networkx as nx

def analyze_graph(df):

    G = nx.DiGraph()

    for _, row in df.iterrows():
        
        sender = row["sender"]
        receiver = row["receiver"]
        amount = float(row["amount"])

        G.add_edge(sender, receiver, weight=amount)

    suspicious_accounts = []
    suspicion_scores = {}

    for node in G.nodes():
        in_deg = G.in_degree(node)
        out_deg = G.out_degree(node)

        score = in_deg + out_deg
        suspicion_scores[node] = score

        if score > 6:
            suspicious_accounts.append(node)

    fraud_rings = list(nx.simple_cycles(G))

    return {
        "total_nodes": len(G.nodes()),
        "total_edges": len(G.edges()),
        "suspicious_accounts": suspicious_accounts,
        "fraud_rings": fraud_rings,
        "suspicion_scores": suspicion_scores
    }
