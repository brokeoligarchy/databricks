#!/usr/bin/env python3
"""
List all-purpose compute clusters from sandbox-pastel.txt and fetch their policy IDs.
Exports results to CSV format.
"""

import argparse
import csv
import os
import sys
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


def get_policy_name(w: WorkspaceClient, policy_id: Optional[str], policy_cache: Dict[str, str]) -> Optional[str]:
    """Get policy name by policy ID, using cache to avoid repeated API calls."""
    if not policy_id:
        return None
    
    # Ensure policy_id is a string
    policy_id_str = str(policy_id) if policy_id else None
    if not policy_id_str:
        return None
    
    # Check cache first
    if policy_id_str in policy_cache:
        cached_name = policy_cache[policy_id_str]
        return cached_name if cached_name else None
    
    # Fetch policy name from API
    try:
        policy = w.cluster_policies.get(policy_id=policy_id_str)
        policy_name = str(policy.name) if policy and policy.name else None
        policy_cache[policy_id_str] = policy_name or ''
        return policy_name
    except Exception:
        # Policy might not exist or user might not have permission
        policy_cache[policy_id_str] = ''
        return None


def extract_cluster_fields(cluster: ClusterDetails) -> Dict[str, Optional[str]]:
    """Extract cluster name and policy ID from a ClusterDetails object."""
    # Ensure all values are strings or None, convert policy_id to string if it's an integer
    policy_id = getattr(cluster, 'policy_id', None)
    if policy_id is not None:
        policy_id = str(policy_id)
    
    return {
        'cluster_name': str(cluster.cluster_name) if cluster.cluster_name else None,
        'policy_id': policy_id,
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
    policy_cache = {}  # Cache policy names to avoid repeated API calls
    
    print("\nFetching cluster details from Databricks workspace...")
    
    # First, get all clusters to build lookup maps
    all_clusters = {}
    cluster_name_to_id = {}
    
    try:
        # Iterate directly over the iterator with tqdm to show progress as clusters are fetched
        # Using total=None since we don't know the count upfront - this shows an indeterminate progress bar
        print("Connecting to Databricks workspace and fetching clusters...")
        print("(This may take a moment if you have many clusters)")
        
        cluster_iterator = w.clusters.list()
        cluster_count = 0
        
        for cluster in tqdm(cluster_iterator, desc="Loading clusters", unit="cluster", total=None, mininterval=1.0):
            cluster_count += 1
            if cluster.cluster_id:
                all_clusters[cluster.cluster_id] = cluster
            if cluster.cluster_name:
                cluster_name_to_id[cluster.cluster_name] = cluster.cluster_id
            
            # Print first cluster as a sign of progress
            if cluster_count == 1:
                tqdm.write(f"Successfully connected! First cluster loaded: {cluster.cluster_name or cluster.cluster_id}")
        
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        print(f"Loaded {len(all_clusters)} clusters before cancellation.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError listing clusters: {e}")
        print(f"Error type: {type(e).__name__}")
        print(f"\nTroubleshooting:")
        print(f"1. Verify DATABRICKS_HOST is correct: {os.getenv('DATABRICKS_HOST', 'NOT SET')}")
        print(f"2. Verify DATABRICKS_TOKEN is set: {'SET' if os.getenv('DATABRICKS_TOKEN') else 'NOT SET'}")
        print(f"3. Check network connectivity to Databricks workspace")
        import traceback
        traceback.print_exc()
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
            # Fetch policy name
            policy_id = cluster_data['policy_id']
            policy_name = get_policy_name(w, policy_id, policy_cache)
            
            # Ensure policy_name is a string or None
            policy_name_str = str(policy_name) if policy_name else None
            
            # Add policy name to cluster data (ensure it's a string or None)
            cluster_data['policy_name'] = policy_name_str
            
            clusters.append(cluster_data)
            # Use tqdm.write() to print without interfering with progress bar
            cluster_name_str = str(cluster_data['cluster_name']) if cluster_data['cluster_name'] else 'Unknown'
            policy_display = f"{policy_name_str} ({policy_id})" if policy_name_str and policy_id else (policy_id or 'No Policy')
            tqdm.write(f"✓ Found: {cluster_name_str} - Policy: {policy_display}")
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
        'cluster_name',
        'policy_id',
        'policy_name',
    ]
    
    # Ensure all values are strings or None before writing to CSV
    sanitized_clusters = []
    for cluster in clusters:
        sanitized_cluster = {}
        for key in fieldnames:
            value = cluster.get(key)
            # Convert to string if not None, otherwise keep as None
            sanitized_cluster[key] = str(value) if value is not None else None
        sanitized_clusters.append(sanitized_cluster)
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sanitized_clusters)
    
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
