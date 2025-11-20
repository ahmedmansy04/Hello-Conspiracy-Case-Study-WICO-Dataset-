import sys
import os
import glob
import pandas as pd
import random

root = sys.argv[1]
out_folder = sys.argv[2]
os.makedirs(out_folder, exist_ok=True)
out_nodes = os.path.join(out_folder, "merged_nodes.csv")
out_edges = os.path.join(out_folder, "merged_edges.csv")

sample_ratio = 0.25

def try_read(filepath):
    # Special handling for TXT edge files without headers
    if filepath.lower().endswith('.txt'):
        try:
            df = pd.read_csv(filepath, sep=r'\s+', header=None, engine='python', dtype=str)
            if df.shape[1] >= 2:
                df = df.iloc[:, :2]  # Just in case there are extra columns
                df.columns = ['source', 'target']
                return df
        except Exception as e:
            print(f"Failed to parse edge TXT file: {filepath}", e)
            return None
    else:
        for sep in [',', '\t', ';', '|']:
            try:
                df = pd.read_csv(filepath, sep=sep, engine='python', dtype=str)
                if df.shape[1] > 1:
                    return df
            except Exception:
                pass
        return pd.read_csv(filepath, sep=None, engine='python', dtype=str)

# Detect all folders containing node files
all_folders = set()
for path in glob.glob(os.path.join(root, '**', 'nodes*.csv'), recursive=True):
    all_folders.add(os.path.dirname(path))

# Random sampling
sample_folders = random.sample(list(all_folders), max(1, int(len(all_folders) * sample_ratio)))
print(f"Using {len(sample_folders)} folders out of {len(all_folders)}")

nodes_list = []
edges_list = []

for folder in sample_folders:
    nodes_path = glob.glob(os.path.join(folder, 'nodes*.csv'))
    edges_path = glob.glob(os.path.join(folder, 'edges*.*'))

    if nodes_path:
        try:
            df = try_read(nodes_path[0])
            if df is not None:
                df['_source_file'] = nodes_path[0]
                nodes_list.append(df)
        except Exception as e:
            print("Failed reading nodes:", nodes_path[0], e)

    if edges_path:
        try:
            df = try_read(edges_path[0])
            if df is not None:
                df['_source_file'] = edges_path[0]
                edges_list.append(df)
        except Exception as e:
            print("Failed reading edges:", edges_path[0], e)

# Merge and save nodes
if nodes_list:
    all_nodes = pd.concat(nodes_list, ignore_index=True, sort=False)
    id_cols = [c for c in all_nodes.columns if c.lower() in ('id', 'node', 'node_id', 'nid')]
    if id_cols:
        all_nodes = all_nodes.rename(columns={id_cols[0]: 'id'})
    else:
        all_nodes = all_nodes.reset_index().rename(columns={'index': 'id'})
    all_nodes = all_nodes.drop_duplicates(subset=['id'], keep='first')
    all_nodes.to_csv(out_nodes, index=False)
    print("Saved merged nodes to", out_nodes)
else:
    print("No nodes files found.")

# Merge and save edges
if edges_list:
    all_edges = pd.concat(edges_list, ignore_index=True, sort=False)
    if 'source' not in all_edges.columns or 'target' not in all_edges.columns:
        s_cols = [c for c in all_edges.columns if c.lower() in ('source', 'from', 'src')]
        t_cols = [c for c in all_edges.columns if c.lower() in ('target', 'to', 'dst')]
        if s_cols and t_cols:
            all_edges = all_edges.rename(columns={s_cols[0]: 'source', t_cols[0]: 'target'})
        else:
            cols = list(all_edges.columns)
            if len(cols) >= 2:
                all_edges = all_edges.rename(columns={cols[0]: 'source', cols[1]: 'target'})
            else:
                raise SystemExit("Could not detect source/target columns in edges files.")
    all_edges = all_edges.dropna(subset=['source', 'target'])
    all_edges.to_csv(out_edges, index=False)
    print("Saved merged edges to", out_edges)
else:
    print("No edges files found.")
