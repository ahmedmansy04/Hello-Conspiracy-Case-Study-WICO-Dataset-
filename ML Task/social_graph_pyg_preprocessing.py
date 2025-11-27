import networkx as nx
import torch
from torch_geometric.data import Data

# Create a small social graph
G = nx.Graph()
edges = [(0,1), (1,2), (2,0),    # normal users community
         (3,4), (4,5), (5, 3)]    # suspicious cluster
G.add_edges_from(edges)

# One-hot node features (6 nodes, 6-dimensional)
num_nodes = 6
features = torch.eye(num_nodes, dtype=torch.float)

# Node labels: first 3 normal (0), last 3 bots (1)
labels = torch.tensor([0,0,0, 1,1,1], dtype=torch.long)

# Convert to PyG Data
edge_index = torch.tensor(list(G.edges())).t().contiguous()
# Make edges bidirectional for undirected graph
edge_index = torch.cat([edge_index, edge_index.flip(0)], dim=1)
data = Data(x=features, edge_index=edge_index, y=labels)

# Define train/test masks (e.g., train on 4 nodes, test on 2 nodes)
data.train = torch.tensor([1,1,1,1,0,0], dtype=torch.bool)
data.test = torch.tensor([0,0,0,0,1,1], dtype=torch.bool)


print("Node features:\n", data.x)
print("Edge index:\n", data.edge_index)
print("Labels:\n", data.y)
print("Train:\n", data.train)
print("Test:\n", data.test)