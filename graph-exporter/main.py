import argparse
from collections import defaultdict

from falkordb import FalkorDB
import pandas as pd

def export_graph(graph_name, host, port):
    # Connect to FalkorDB
    db = FalkorDB(host=host, port=port)
    g = db.select_graph(graph_name)

    # Export Nodes by Label
    nodes_result = g.ro_query("MATCH (n) RETURN ID(n), labels(n), properties(n)")
    nodes_by_label = defaultdict(list)

    for record in nodes_result.result_set:
        node_id = record[0]
        labels = record[1]
        props = record[2] or {}

        # Handle nodes with multiple labels or no labels
        if labels:
            for label in labels:
                node = {"id": node_id}
                node.update(props)
                nodes_by_label[label].append(node)
        else:
            # Handle nodes without labels
            node = {"id": node_id}
            node.update(props)
            nodes_by_label["unlabeled"].append(node)

    # Export each node label to its own CSV file
    for label, nodes in nodes_by_label.items():
        filename = f"nodes_{label}.csv"
        pd.DataFrame(nodes).to_csv(filename, index=False)
        print(f"✅ Exported {len(nodes)} nodes with label '{label}' to {filename}")

    # Export Edges by Type
    edges_result = g.ro_query(
        "MATCH (a)-[e]->(b) RETURN ID(e), TYPE(e), ID(a), ID(b), properties(e)"
    )
    edges_by_type = defaultdict(list)

    for record in edges_result.result_set:
        edge_id = record[0]
        edge_type = record[1]
        from_id = record[2]
        to_id = record[3]
        props = record[4] or {}

        edge = {
            "id": edge_id,
            "from_id": from_id,
            "to_id": to_id
        }
        edge.update(props)
        edges_by_type[edge_type].append(edge)

    # Export each edge type to its own CSV file
    for edge_type, edges in edges_by_type.items():
        filename = f"edges_{edge_type}.csv"
        pd.DataFrame(edges).to_csv(filename, index=False)
        print(f"✅ Exported {len(edges)} edges of type '{edge_type}' to {filename}")

    # Print summary
    print("\n📊 Summary:")
    print(f"   Node labels exported: {len(nodes_by_label)}")
    print(f"   Edge types exported: {len(edges_by_type)}")

def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Export FalkorDB graph nodes and edges to CSV files by label/type."
    )
    parser.add_argument("graph_name", help="Name of the graph to export")
    parser.add_argument("--host", default="localhost", help="FalkorDB host (default: localhost)")
    parser.add_argument("--port", type=int, default=6379, help="FalkorDB port (default: 6379)")

    args = parser.parse_args()

    export_graph(args.graph_name, args.host, args.port)


if __name__ == "__main__":
    main()
