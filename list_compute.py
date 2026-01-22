#!/usr/bin/env python3
"""
List all-purpose compute clusters from sandbox-pastel.txt and fetch their policy IDs.
Exports results to CSV format.
"""

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails
from tqdm import tqdm


def get_workspace_client() -> WorkspaceClient:
    """Initialize and return Databricks WorkspaceClient."""
    host = os.getenv('DATABRICKS_HOST')
    token = os.getenv('DATABRICKS_TOKEN')
    
    if not host:
        print("Error: DATABRICKS_HOST environment variable is not set")
        sys.exit(1)
    
    if not token:
        print("Error: DATABRICKS_TOKEN environment variable is not set")
        sys.exit(1)
    
    return WorkspaceClient(host=host, token=token)


def format_datetime(dt: Optional[datetime]) -> Optional[str]:
    """Format datetime object to ISO format string."""
    if dt is None:
        return None
    return dt.isoformat()


def extract_cluster_fields(cluster: ClusterDetails) -> Dict[str, Optional[str]]:
    """Extract relevant fields from a ClusterDetails object."""
    return {
        'cluster_id': cluster.cluster_id,
        'cluster_name': cluster.cluster_name,
        'state': cluster.state.value if cluster.state else None,
        'policy_id': getattr(cluster, 'policy_id', None),
        'cluster_source': cluster.cluster_source.value if cluster.cluster_source else None,
        'creator_user_name': cluster.creator_user_name,
        'start_time': format_datetime(cluster.start_time),
        'terminated_time': format_datetime(cluster.terminated_time),
        'num_workers': cluster.num_workers if cluster.num_workers else 0,
        'node_type_id': cluster.node_type_id,
        'driver_node_type_id': cluster.driver_node_type_id,
        'spark_version': cluster.spark_version,
        'autotermination_minutes': cluster.autotermination_minutes if cluster.autotermination_minutes else None,
    }


def read_cluster_list(filepath: str) -> List[str]:
    """Read cluster identifiers from sandbox-pastel.txt file."""
    script_dir = Path(__file__).parent
    file_path = script_dir / filepath
    
    if not file_path.exists():
        print(f"Error: File '{file_path}' not found")
        sys.exit(1)
    
    cluster_ids = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # Skip empty lines and comments
                cluster_ids.append(line)
    
    print(f"Read {len(cluster_ids)} cluster identifiers from {filepath}")
    return cluster_ids


def fetch_cluster_details(w: WorkspaceClient, cluster_identifiers: List[str]) -> List[Dict[str, Optional[str]]]:
    """Fetch cluster details for given cluster identifiers (IDs or names)."""
    clusters = []
    not_found = []
    
    print("\nFetching cluster details from Databricks workspace...")
    
    # First, get all clusters to build lookup maps
    all_clusters = {}
    cluster_name_to_id = {}
    
    try:
        # Use tqdm to show progress while loading clusters
        cluster_list = list(w.clusters.list())
        for cluster in tqdm(cluster_list, desc="Loading clusters", unit="cluster"):
            if cluster.cluster_id:
                all_clusters[cluster.cluster_id] = cluster
            if cluster.cluster_name:
                cluster_name_to_id[cluster.cluster_name] = cluster.cluster_id
    except Exception as e:
        print(f"Error listing clusters: {e}")
        sys.exit(1)
    
    print(f"Loaded {len(all_clusters)} clusters from workspace\n")
    
    # Now fetch details for each identifier with progress bar
    for identifier in tqdm(cluster_identifiers, desc="Processing clusters", unit="cluster"):
        cluster = None
        
        # Try as cluster ID first
        if identifier in all_clusters:
            cluster = all_clusters[identifier]
        # Try as cluster name
        elif identifier in cluster_name_to_id:
            cluster_id = cluster_name_to_id[identifier]
            cluster = all_clusters.get(cluster_id)
        
        if cluster:
            cluster_data = extract_cluster_fields(cluster)
            clusters.append(cluster_data)
            # Use tqdm.write() to print without interfering with progress bar
            state = cluster_data['state'] or 'UNKNOWN'
            policy_id = cluster_data['policy_id'] or 'None'
            tqdm.write(f"✓ Found: {cluster_data['cluster_name']} ({state}) - Policy ID: {policy_id}")
        else:
            not_found.append(identifier)
            tqdm.write(f"✗ Not Found: {identifier}")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Total processed: {len(cluster_identifiers)}")
    print(f"  Found: {len(clusters)}")
    print(f"  Not found: {len(not_found)}")
    if not_found:
        print(f"\n  Not found clusters: {', '.join(not_found)}")
    print(f"{'='*60}\n")
    
    return clusters


def export_to_csv(clusters: List[Dict[str, Optional[str]]], filename: str = 'databricks_compute_list.csv'):
    """Export cluster data to CSV file."""
    if not clusters:
        print("No clusters to export.")
        return
    
    fieldnames = [
        'cluster_id',
        'cluster_name',
        'state',
        'policy_id',
        'cluster_source',
        'creator_user_name',
        'start_time',
        'terminated_time',
        'num_workers',
        'node_type_id',
        'driver_node_type_id',
        'spark_version',
        'autotermination_minutes',
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clusters)
    
    print(f"\nExported {len(clusters)} clusters to {filename}")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='List all-purpose compute clusters from Databricks workspace with policy IDs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python list_compute.py
  python list_compute.py -i my-clusters.txt -o results.csv
  python list_compute.py --input-file clusters.txt --output-file output.csv
        """
    )
    
    parser.add_argument(
        '-i', '--input-file',
        type=str,
        default='sandbox-pastel.txt',
        help='Input file containing cluster identifiers (one per line). Default: sandbox-pastel.txt'
    )
    
    parser.add_argument(
        '-o', '--output-file',
        type=str,
        default='databricks_compute_list.csv',
        help='Output CSV file path. Default: databricks_compute_list.csv'
    )
    
    return parser.parse_args()


def main():
    """Main execution function."""
    # Parse command-line arguments
    args = parse_arguments()
    
    print("=" * 60)
    print("Databricks All-Purpose Compute Lister")
    print("=" * 60)
    print(f"Input file: {args.input_file}")
    print(f"Output file: {args.output_file}")
    print("=" * 60)
    
    # Read cluster list from file
    cluster_identifiers = read_cluster_list(args.input_file)
    
    if not cluster_identifiers:
        print(f"No cluster identifiers found in {args.input_file}")
        sys.exit(1)
    
    # Initialize workspace client
    w = get_workspace_client()
    
    # Fetch cluster details
    clusters = fetch_cluster_details(w, cluster_identifiers)
    
    # Export to CSV
    export_to_csv(clusters, args.output_file)
    
    print("\nDone!")


if __name__ == '__main__':
    main()
