
import os
import random
import math
from collections import Counter
import json

import networkx as nx
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_recall_fscore_support, roc_auc_score, confusion_matrix, classification_report
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import cross_val_score
import joblib

# community detection (Louvain)
import community as community_louvain

# reproducibility
RANDOM_STATE = 42
np.random.seed(RANDOM_STATE)
random.seed(RANDOM_STATE)

# -----------------------------
# CONFIG
# -----------------------------
CONFIG = {
    'DATA_PATH': 'facebook_combined.txt.gz',
    'SAVE_DIR': "",
    'N_SYNTHETIC_BOTS': 100,           # number of injected bot nodes
    'BOT_EDGES_PER_NODE': 15,          # edges each injected bot will have
    'POISON_INJECTION_COUNT': 100,     # number of nodes to inject for poisoning
    'POISON_EDGES_PER_INJECT': 10,
    'EVASION_BUDGET_PER_BOT': 8,       # number of edge operations per bot in evasion
    'TEST_SIZE': 0.3,
}

os.makedirs(CONFIG['SAVE_DIR'], exist_ok=True)

# -----------------------------
# I/O and helpers
# -----------------------------

def load_graph(path=CONFIG['DATA_PATH']):
    """Load undirected edge list into NetworkX Graph.
    Expects the SNAP facebook_combined format (space separated pairs).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found at {path}. Download from SNAP and place it there.")
    print(f"Loading graph from {path} ...")
    G = nx.read_edgelist(path, nodetype=int)
    print(f"Loaded graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G


def save_graph_gexf(G, path):
    nx.write_gexf(G, path)
    print(f"Saved graph to {path} (GEXF)")


# -----------------------------
# EDA and basic stats
# -----------------------------

def compute_basic_stats(G):
    stats = {}
    stats['n_nodes'] = G.number_of_nodes()
    stats['n_edges'] = G.number_of_edges()
    degrees = [d for n, d in G.degree()]
    stats['avg_degree'] = np.mean(degrees)
    stats['median_degree'] = np.median(degrees)
    stats['degree_std'] = np.std(degrees)
    stats['avg_clustering'] = nx.average_clustering(G)
    # connected components
    comps = list(nx.connected_components(G))
    comp_sizes = [len(c) for c in comps]
    stats['n_components'] = len(comps)
    stats['largest_cc'] = max(comp_sizes)
    # approximate diameter on largest component (costly otherwise)
    largest_cc_nodes = max(comps, key=len)
    G_lcc = G.subgraph(largest_cc_nodes)
    try:
        stats['diameter_lcc'] = nx.diameter(G_lcc)
        stats['avg_shortest_path_lcc'] = nx.average_shortest_path_length(G_lcc)
    except Exception:
        stats['diameter_lcc'] = None
        stats['avg_shortest_path_lcc'] = None

    print("Basic graph statistics:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return stats


# -----------------------------
# Feature extraction
# -----------------------------

def extract_features(G, compute_eigenvector=True):
    nodes = list(G.nodes())
    n = len(nodes)
    print(f"Extracting features for {n} nodes ...")

    # basic structural features
    degree = dict(G.degree())
    clustering = nx.clustering(G)
    triangles = nx.triangles(G)
    core_number = nx.core_number(G)
    avg_neighbor_deg = nx.average_neighbor_degree(G)

    # centralities
    deg_cent = nx.degree_centrality(G)
    try:
        closeness = nx.closeness_centrality(G)
    except Exception:
        closeness = {n:0 for n in nodes}

    # betweenness: approximate for speed using k nodes sampling if large
    if n > 2000:
        k = 200  # sample nodes
        betweenness = nx.betweenness_centrality(G, k=k, seed=RANDOM_STATE)
    else:
        betweenness = nx.betweenness_centrality(G)

    pagerank = nx.pagerank(G)

    eigenvector = None
    if compute_eigenvector:
        try:
            eigenvector = nx.eigenvector_centrality_numpy(G)
        except Exception:
            eigenvector = {n:0 for n in nodes}

    # community detection (Louvain)
    print("Computing Louvain communities ...")
    partition = community_louvain.best_partition(G)

    # Build DataFrame
    rows = []
    for v in nodes:
        rows.append({
            'node': v,
            'degree': degree.get(v, 0),
            'log_degree': math.log1p(degree.get(v, 0)),
            'clustering': clustering.get(v, 0.0),
            'triangles': triangles.get(v, 0),
            'core': core_number.get(v, 0),
            'avg_neighbor_deg': avg_neighbor_deg.get(v, 0.0),
            'degree_centrality': deg_cent.get(v, 0.0),
            'closeness': closeness.get(v, 0.0),
            'betweenness': betweenness.get(v, 0.0),
            'pagerank': pagerank.get(v, 0.0),
            'eigenvector': eigenvector.get(v, 0.0) if eigenvector is not None else 0.0,
            'community': partition.get(v, -1),
        })

    df = pd.DataFrame(rows).set_index('node')

    # Additional derived features
    df['degree_to_avg_neighbor_ratio'] = df['degree'] / (df['avg_neighbor_deg'] + 1e-9)
    df['is_core'] = (df['core'] >= np.percentile(df['core'], 75)).astype(int)

    print("Feature extraction complete. Example rows:")
    print(df.head())
    return df


# -----------------------------
# Synthetic bot creation (node-injection)
# -----------------------------

def create_synthetic_bots(G, n_bots=CONFIG['N_SYNTHETIC_BOTS'], edges_per_bot=CONFIG['BOT_EDGES_PER_NODE'], strategy='random'):
    G2 = G.copy()
    existing_nodes = list(G2.nodes())
    labels = {n: 0 for n in existing_nodes}
    bots = []

    # choose high-degree nodes set for targeted strategy
    deg_sorted_nodes = sorted(existing_nodes, key=lambda x: G2.degree(x), reverse=True)
    hub_candidates = deg_sorted_nodes[:max(500, int(0.1 * len(deg_sorted_nodes)))]

    for i in range(n_bots):
        bot_id = f'bot_{i}'
        bots.append(bot_id)
        G2.add_node(bot_id)
        labels[bot_id] = 1
        if strategy == 'random':
            targets = random.sample(existing_nodes, min(edges_per_bot, len(existing_nodes)))
        elif strategy == 'targeted_high_degree':
            targets = random.sample(hub_candidates, min(edges_per_bot, len(hub_candidates)))
        elif strategy == 'community_targeted':
            # pick a random community by Louvain and connect inside it
            partition = community_louvain.best_partition(G2)
            communities = {}
            for node, com in partition.items():
                communities.setdefault(com, []).append(node)
            com = random.choice(list(communities.keys()))
            community_nodes = communities[com]
            targets = random.sample(community_nodes, min(edges_per_bot, len(community_nodes)))
        else:
            targets = random.sample(existing_nodes, min(edges_per_bot, len(existing_nodes)))

        for t in targets:
            G2.add_edge(bot_id, t)

    print(f"Injected {n_bots} synthetic bots (strategy={strategy}).")
    return G2, bots, labels


# -----------------------------
# Baseline classifier
# -----------------------------

def build_dataset_from_features(df_features, labels_dict):
    df = df_features.copy()
    df['label'] = df.index.map(lambda n: labels_dict.get(n, 0))
    X = df.drop(columns=['label'])
    y = df['label']
    return X, y


def train_evaluate_baseline(X, y, test_size=CONFIG['TEST_SIZE'], random_state=RANDOM_STATE, save_model_path=None):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, stratify=y, random_state=random_state)

    # Simple pipeline with scaling and RandomForest
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('clf', RandomForestClassifier(n_estimators=200, random_state=random_state, n_jobs=-1))
    ])

    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1] if hasattr(pipeline.named_steps['clf'], 'predict_proba') else None

    prec, rec, f1, _ = precision_recall_fscore_support(y_test, y_pred, average='binary', zero_division=0)
    auc = roc_auc_score(y_test, y_prob) if y_prob is not None and len(np.unique(y_test)) > 1 else None

    metrics = {
        'precision': prec,
        'recall': rec,
        'f1': f1,
        'roc_auc': auc,
        'confusion_matrix': confusion_matrix(y_test, y_pred).tolist(),
        'classification_report': classification_report(y_test, y_pred, zero_division=0),
    }

    print("Baseline evaluation:")
    print(json.dumps({k: v for k, v in metrics.items() if k != 'classification_report' and k != 'confusion_matrix'}, indent=2))
    print("Classification report:\n", metrics['classification_report'])

    if save_model_path:
        joblib.dump(pipeline, save_model_path)
        print(f"Saved model to {save_model_path}")

    return pipeline, metrics, (X_train, X_test, y_train, y_test)


# -----------------------------
# Structural Evasion (test-time perturbation)
# -----------------------------

def structural_evasion_add_edges_to_increase_clustering(G, target_nodes, budget_per_node=CONFIG['EVASION_BUDGET_PER_BOT']):
    G2 = G.copy()
    for v in target_nodes:
        if v not in G2:
            continue
        additions = 0
        neighbors = set(G2.neighbors(v))
        # candidate nodes: neighbors of neighbors excluding self and existing neighbors
        candidates = set()
        for u in neighbors:
            for w in G2.neighbors(u):
                if w != v and w not in neighbors and not G2.has_edge(v, w):
                    candidates.add(w)
        # compute a simple score: prefer candidates that are in the same community or high-degree
        # we'll sort by degree in descending order as a heuristic
        cand_list = list(candidates)
        cand_list.sort(key=lambda x: G2.degree(x), reverse=True)
        for c in cand_list:
            if additions >= budget_per_node:
                break
            G2.add_edge(v, c)
            additions += 1
        # if not enough candidates, try connecting to random high-degree nodes
        idx = 0
        hubs = sorted(G2.nodes(), key=lambda x: G2.degree(x), reverse=True)
        while additions < budget_per_node and idx < len(hubs):
            hub = hubs[idx]
            idx += 1
            if hub != v and not G2.has_edge(v, hub):
                G2.add_edge(v, hub)
                additions += 1
    print(f"Applied structural evasion: added up to {budget_per_node} edges per target node.")
    return G2


# -----------------------------
# Graph Poisoning (node injection & edge perturbation)
# -----------------------------

def graph_poisoning_node_injection(G, n_inject=CONFIG['POISON_INJECTION_COUNT'], edges_per_inject=CONFIG['POISON_EDGES_PER_INJECT'], strategy='community_targeted'):
    G2 = G.copy()
    existing_nodes = [n for n in G2.nodes()]
    injected = []

    # optional community map for community_targeted
    partition = community_louvain.best_partition(G2)
    communities = {}
    for node, com in partition.items():
        communities.setdefault(com, []).append(node)
    community_ids = list(communities.keys())

    # hub list
    deg_sorted = sorted(existing_nodes, key=lambda x: G2.degree(x), reverse=True)
    hubs = deg_sorted[:max(100, int(0.05 * len(deg_sorted)))]

    for i in range(n_inject):
        inj_id = f'poison_{i}'
        injected.append(inj_id)
        G2.add_node(inj_id)
        if strategy == 'random':
            targets = random.sample(existing_nodes, min(edges_per_inject, len(existing_nodes)))
        elif strategy == 'community_targeted':
            com = random.choice(community_ids)
            com_nodes = communities[com]
            # pick majority from community and some others
            k1 = int(edges_per_inject * 0.8)
            k2 = edges_per_inject - k1
            t1 = random.sample(com_nodes, min(k1, len(com_nodes)))
            t2 = random.sample(existing_nodes, min(k2, len(existing_nodes)))
            targets = t1 + t2
        elif strategy == 'hub_targeted':
            targets = random.sample(hubs, min(edges_per_inject, len(hubs)))
        else:
            targets = random.sample(existing_nodes, min(edges_per_inject, len(existing_nodes)))

        for t in targets:
            G2.add_edge(inj_id, t)

    print(f"Injected {n_inject} poisoning nodes with strategy {strategy}.")
    return G2, injected


# -----------------------------
# Visualization utilities
# -----------------------------

def plot_degree_distribution(G, savepath=None):
    degrees = [d for n, d in G.degree()]
    plt.figure(figsize=(6,4))
    sns.histplot(degrees, bins=50, kde=False)
    plt.xlabel('Degree')
    plt.ylabel('Count')
    plt.title('Degree Distribution')
    if savepath:
        plt.savefig(savepath, dpi=150)
        print(f"Saved degree distribution to {savepath}")
    plt.close()


def draw_subgraph(G, nodes, title=None, savepath=None, node_color_map=None):
    H = G.subgraph(nodes).copy()
    plt.figure(figsize=(8,6))
    pos = nx.spring_layout(H, seed=RANDOM_STATE, k=0.15)
    if node_color_map:
        colors = [node_color_map.get(n, 0.5) for n in H.nodes()]
    else:
        colors = 'skyblue'
    nx.draw_networkx_nodes(H, pos, node_size=80, node_color=colors)
    nx.draw_networkx_edges(H, pos, alpha=0.6)
    nx.draw_networkx_labels(H, pos, font_size=6)
    if title:
        plt.title(title)
    plt.axis('off')
    if savepath:
        plt.savefig(savepath, dpi=200)
        print(f"Saved subgraph figure to {savepath}")
    plt.close()


# -----------------------------
# Putting it all together: end-to-end example
# -----------------------------

def main_workflow():
    # 1. Load graph
    G = load_graph()

    # 2. Basic stats
    stats_before = compute_basic_stats(G)

    # 3. Feature extraction (on original graph)
    df_features = extract_features(G)

    # 4. Inject synthetic bots (for ground truth)
    G_injected, bots, labels_injected = create_synthetic_bots(G, n_bots=CONFIG['N_SYNTHETIC_BOTS'], edges_per_bot=CONFIG['BOT_EDGES_PER_NODE'], strategy='random')

    # 5. Extract features on injected graph and build dataset
    df_features_injected = extract_features(G_injected)
    X, y = build_dataset_from_features(df_features_injected, labels_injected)

    # 6. Train baseline
    model, metrics_baseline, split = train_evaluate_baseline(X, y, save_model_path=os.path.join(CONFIG['SAVE_DIR'], 'baseline_model.joblib'))

    # Save baseline metrics
    with open(os.path.join(CONFIG['SAVE_DIR'], 'baseline_metrics.json'), 'w') as f:
        json.dump(metrics_baseline, f)

    # 7. Structural Evasion: apply to injected bots (test-time)
    G_evasion = structural_evasion_add_edges_to_increase_clustering(G_injected, target_nodes=bots[:20], budget_per_node=CONFIG['EVASION_BUDGET_PER_BOT'])
    df_evasion = extract_features(G_evasion)
    X_evasion, y_evasion = build_dataset_from_features(df_evasion, labels_injected)

    # Evaluate baseline model on evaded nodes (note: pipeline expects same features order)
    # We'll predict only on nodes that were in test split originally. For simplicity, evaluate on entire set here.
    X_all = X.copy()
    y_all = y.copy()
    # Ensure feature columns match model's expectation
    # extract features order
    model_features = X.columns
    X_evasion_ordered = X_evasion[model_features].fillna(0)
    y_pred_evasion = model.predict(X_evasion_ordered)
    prec_e, rec_e, f1_e, _ = precision_recall_fscore_support(y_all, y_pred_evasion, average='binary', zero_division=0)
    print("After structural evasion (global eval): Precision/Recall/F1", prec_e, rec_e, f1_e)

    # 8. Graph Poisoning: node injection
    G_poisoned, injected_nodes = graph_poisoning_node_injection(G_injected, n_inject=CONFIG['POISON_INJECTION_COUNT'], edges_per_inject=CONFIG['POISON_EDGES_PER_INJECT'], strategy='community_targeted')
    # feature extraction after poisoning
    df_poisoned = extract_features(G_poisoned)

    # Note: Labels for poison nodes should be benign (0) if attacker wants to poison training labels
    labels_with_poison = labels_injected.copy()
    for inj in injected_nodes:
        labels_with_poison[inj] = 0  # labeled as benign to confuse classifier

    # Rebuild dataset and retrain on poisoned graph
    X_pois, y_pois = build_dataset_from_features(df_poisoned, labels_with_poison)
    model_pois, metrics_pois, _ = train_evaluate_baseline(X_pois, y_pois, save_model_path=os.path.join(CONFIG['SAVE_DIR'], 'poisoned_model.joblib'))

    # Evaluate poisoned model on clean test set (original injected graph features)
    # We'll evaluate on X (clean) to measure degradation
    X_clean_ordered = X[model_features].fillna(0)
    y_clean = y
    y_pred_pois_on_clean = model_pois.predict(X_clean_ordered)
    prec_p, rec_p, f1_p, _ = precision_recall_fscore_support(y_clean, y_pred_pois_on_clean, average='binary', zero_division=0)
    auc_p = None
    try:
        y_prob_p = model_pois.predict_proba(X_clean_ordered)[:,1]
        if len(np.unique(y_clean)) > 1:
            auc_p = roc_auc_score(y_clean, y_prob_p)
    except Exception:
        auc_p = None

    print("After poisoning, evaluated on clean data: Prec/Rec/F1/AUC", prec_p, rec_p, f1_p, auc_p)

    # 9. Visualizations: pick a small neighborhood around a bot and show before/after
    sample_bot = bots[0]
    ego_nodes_before = list(nx.ego_graph(G_injected, sample_bot, radius=2).nodes())
    draw_subgraph(G_injected, ego_nodes_before, title='Ego around bot (before evasion/poison)', savepath=os.path.join(CONFIG['SAVE_DIR'], 'ego_before.png'))

    ego_nodes_after = list(nx.ego_graph(G_evasion, sample_bot, radius=2).nodes())
    draw_subgraph(G_evasion, ego_nodes_after, title='Ego around bot (after evasion)', savepath=os.path.join(CONFIG['SAVE_DIR'], 'ego_after_evasion.png'))

    ego_nodes_poison = list(nx.ego_graph(G_poisoned, sample_bot, radius=2).nodes())
    draw_subgraph(G_poisoned, ego_nodes_poison, title='Ego around bot (after poisoning)', savepath=os.path.join(CONFIG['SAVE_DIR'], 'ego_after_poison.png'))

    # Save graphs for Gephi if desired
    save_graph_gexf(G_injected, os.path.join(CONFIG['SAVE_DIR'], 'graph_injected.gexf'))
    save_graph_gexf(G_evasion, os.path.join(CONFIG['SAVE_DIR'], 'graph_evasion.gexf'))
    save_graph_gexf(G_poisoned, os.path.join(CONFIG['SAVE_DIR'], 'graph_poisoned.gexf'))

    # Save metrics summary
    summary = {
        'baseline': {k: v for k, v in metrics_baseline.items() if k in ['precision','recall','f1','roc_auc']},
        'after_structural_evasion_sample_eval': {'precision': prec_e, 'recall': rec_e, 'f1': f1_e},
        'after_poisoning_on_clean': {'precision': prec_p, 'recall': rec_p, 'f1': f1_p, 'roc_auc': auc_p}
    }
    with open(os.path.join(CONFIG['SAVE_DIR'], 'summary_metrics.json'), 'w') as f:
        json.dump(summary, f, indent=2)
    print("Workflow complete. Outputs available in the output/ directory.")


if __name__ == '__main__':
    main_workflow()
