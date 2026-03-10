"""
graph/graph_builder.py
BlockWatchKE — Phase 4: Transaction Graph Analytics

Builds a directed wallet-to-wallet graph from transactions and runs:
  - Community detection (connected wallet clusters)
  - Centrality analysis (most influential wallets)
  - Suspicious cluster identification
  - Cross-border corridor mapping
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import networkx as nx
from loguru import logger
from sqlalchemy import text
from database.db_connection import engine

OUTPUT_DIR = "data/processed"


def load_high_risk_transactions() -> pd.DataFrame:
    """Load only alerted transactions to keep graph manageable."""
    sql = """
        SELECT t.tx_hash, t.sender, t.receiver, t.amount_usdt, t.timestamp
        FROM transactions t
        JOIN risk_scores r ON t.tx_hash = r.tx_hash
        WHERE r.is_alert = TRUE
        ORDER BY t.timestamp DESC
    """
    df = pd.read_sql(sql, engine)
    logger.info(f"Loaded {len(df):,} high-risk transactions for graph")
    return df


def load_all_transactions() -> pd.DataFrame:
    """Load all transactions for full graph."""
    df = pd.read_sql(
        "SELECT tx_hash, sender, receiver, amount_usdt, timestamp FROM transactions",
        engine
    )
    logger.info(f"Loaded {len(df):,} transactions for full graph")
    return df


def build_graph(df: pd.DataFrame) -> nx.DiGraph:
    """
    Build a directed weighted graph.
    Nodes  = wallet addresses
    Edges  = transfers (sender → receiver)
    Weight = total USDT transferred between pair
    """
    logger.info("Building transaction graph...")
    G = nx.DiGraph()

    for _, row in df.iterrows():
        sender   = row["sender"]
        receiver = row["receiver"]
        amount   = float(row["amount_usdt"])

        if G.has_edge(sender, receiver):
            G[sender][receiver]["weight"]    += amount
            G[sender][receiver]["tx_count"]  += 1
        else:
            G.add_edge(sender, receiver, weight=amount, tx_count=1)

    logger.info(f"Graph built — {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")
    return G


def analyze_centrality(G: nx.DiGraph) -> pd.DataFrame:
    """
    Compute centrality scores to find the most influential wallets.
    High in-degree  = major receiver (potential cash-out point)
    High out-degree = major sender   (potential source)
    High betweenness = key intermediary (potential mixer/broker)
    """
    logger.info("Computing centrality metrics...")

    in_degree   = dict(G.in_degree(weight="weight"))
    out_degree  = dict(G.out_degree(weight="weight"))

    # Betweenness on undirected version for speed
    G_undirected = G.to_undirected()
    if G_undirected.number_of_nodes() < 5000:
        betweenness = nx.betweenness_centrality(G_undirected, normalized=True)
    else:
        # Approximate for large graphs
        betweenness = nx.betweenness_centrality(
            G_undirected, normalized=True, k=500
        )

    nodes = list(G.nodes())
    df = pd.DataFrame({
        "address":          nodes,
        "in_degree_usdt":   [in_degree.get(n, 0)   for n in nodes],
        "out_degree_usdt":  [out_degree.get(n, 0)  for n in nodes],
        "betweenness":      [betweenness.get(n, 0) for n in nodes],
    })

    df["total_volume"] = df["in_degree_usdt"] + df["out_degree_usdt"]
    df = df.sort_values("total_volume", ascending=False)

    logger.info(f"✅ Centrality computed for {len(df):,} wallets")
    return df


def detect_communities(G: nx.DiGraph) -> dict:
    """
    Detect wallet clusters using connected components.
    Each cluster likely represents a related group of wallets
    (exchange + OTC broker + end users).
    """
    logger.info("Detecting wallet communities...")
    G_undirected = G.to_undirected()
    communities  = list(nx.connected_components(G_undirected))
    communities.sort(key=len, reverse=True)

    community_map = {}
    for idx, community in enumerate(communities):
        for wallet in community:
            community_map[wallet] = idx

    logger.info(f"✅ Found {len(communities):,} communities")
    logger.info(f"   Largest community: {len(communities[0]):,} wallets")
    logger.info(f"   Top 5 community sizes: {[len(c) for c in communities[:5]]}")
    return community_map


def find_corridors(df: pd.DataFrame, community_map: dict) -> pd.DataFrame:
    """
    Identify cross-border payment corridors by finding
    high-volume wallet pairs and their community clusters.
    Pattern: Exchange → OTC Broker → Kenyan Wallet
    """
    logger.info("Mapping payment corridors...")

    corridors = (
        df.groupby(["sender", "receiver"])
        .agg(
            total_usdt=("amount_usdt", "sum"),
            tx_count=("tx_hash", "count"),
            avg_usdt=("amount_usdt", "mean"),
        )
        .reset_index()
        .sort_values("total_usdt", ascending=False)
        .head(50)
    )

    corridors["community_sender"]   = corridors["sender"].map(community_map)
    corridors["community_receiver"] = corridors["receiver"].map(community_map)
    corridors["same_cluster"]       = (
        corridors["community_sender"] == corridors["community_receiver"]
    )

    logger.info(f"✅ Top 50 corridors identified")
    return corridors


def save_graph_results(
    centrality: pd.DataFrame,
    corridors: pd.DataFrame,
    community_map: dict,
):
    """Save all graph analysis results to CSV files."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    centrality.to_csv(f"{OUTPUT_DIR}/wallet_centrality.csv", index=False)
    corridors.to_csv(f"{OUTPUT_DIR}/payment_corridors.csv", index=False)

    community_df = pd.DataFrame(
        list(community_map.items()),
        columns=["address", "community_id"]
    )
    community_df.to_csv(f"{OUTPUT_DIR}/wallet_communities.csv", index=False)

    logger.info(f"✅ Graph results saved to {OUTPUT_DIR}/")


def print_graph_summary(
    G: nx.DiGraph,
    centrality: pd.DataFrame,
    corridors: pd.DataFrame,
    community_map: dict,
):
    total_communities = len(set(community_map.values()))

    logger.info("\n── Graph Summary ──────────────────────────────")
    print(f"  Total wallets (nodes)  : {G.number_of_nodes():,}")
    print(f"  Total transfers (edges): {G.number_of_edges():,}")
    print(f"  Wallet communities     : {total_communities:,}")
    print(f"  Largest cluster size   : {max(len(c) for c in nx.connected_components(G.to_undirected())):,}")

    logger.info("\n── Top 10 Most Central Wallets ────────────────")
    print(centrality.head(10)[
        ["address", "total_volume", "betweenness", "in_degree_usdt", "out_degree_usdt"]
    ].to_string(index=False))

    logger.info("\n── Top 10 Highest Volume Corridors ────────────")
    print(corridors.head(10)[
        ["sender", "receiver", "total_usdt", "tx_count", "same_cluster"]
    ].to_string(index=False))
    logger.info("─" * 55)


def run_graph_analytics(use_alerts_only: bool = True):
    logger.info("=" * 55)
    logger.info("  BlockWatchKE — Phase 4: Graph Analytics")
    logger.info("=" * 55)

    df = load_high_risk_transactions() if use_alerts_only else load_all_transactions()

    if df.empty:
        logger.warning("No transactions found. Run Phase 1 and Phase 3 first.")
        return

    G             = build_graph(df)
    centrality    = analyze_centrality(G)
    community_map = detect_communities(G)
    corridors     = find_corridors(df, community_map)

    save_graph_results(centrality, corridors, community_map)
    print_graph_summary(G, centrality, corridors, community_map)

    logger.info("Phase 4 complete.")
    return G, centrality, community_map, corridors


if __name__ == "__main__":
    run_graph_analytics()
