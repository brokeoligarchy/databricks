# Databricks All-Purpose Compute Lister

This script lists all all-purpose compute clusters in a Databricks workspace (Azure), including terminated and turned-off clusters, and exports the results with policy IDs to CSV format.

## Features

- Lists all all-purpose compute clusters (excludes job clusters)
- Includes clusters in all states: RUNNING, TERMINATED, etc.
- Extracts policy ID for each cluster
- Exports to CSV with comprehensive cluster information

## Prerequisites

- Python 3.7 or higher
- Databricks workspace access token
- Workspace URL (for Azure Databricks)

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Set the following environment variables:

```bash
export DATABRICKS_HOST="https://adb-<workspace-id>.<region>.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
```

Alternatively, you can create a `.env` file (copy from `.env.example`) and use a tool like `python-dotenv` to load it.

## Usage

```bash
python list_compute.py
```

The script will:
1. Connect to your Databricks workspace
2. List all all-purpose compute clusters
3. Export results to `databricks_compute_list.csv`

## Output

The CSV file contains the following columns:

- `cluster_id` - Unique cluster identifier
- `cluster_name` - Name of the cluster
- `state` - Current state (RUNNING, TERMINATED, etc.)
- `policy_id` - Policy ID assigned to the cluster (may be empty)
- `cluster_source` - Source of cluster creation (UI or API)
- `creator_user_name` - User who created the cluster
- `start_time` - When the cluster was started
- `terminated_time` - When the cluster was terminated (if applicable)
- `num_workers` - Number of worker nodes
- `node_type_id` - Worker node type
- `driver_node_type_id` - Driver node type
- `spark_version` - Spark version
- `autotermination_minutes` - Auto-termination setting

## Important Notes

### API Limitations

The Databricks Clusters API returns:
- All pinned clusters (regardless of age)
- All active clusters
- All terminated **all-purpose** clusters from the last 30 days
- Up to 30 terminated **job** clusters from the last 30 days

**Clusters terminated more than 30 days ago that are not pinned will NOT appear** in the API response. To capture older clusters:
- Option 1: Pin clusters before termination (admin action)
- Option 2: Use Databricks Audit Logs API (requires admin permissions)
- Option 3: Run this script regularly to maintain historical records

### Policy ID

The `policy_id` field will be `None` (empty in CSV) if:
- No policy was assigned when the cluster was created
- The cluster was created before policies were implemented

## Troubleshooting

### Authentication Errors
- Verify your `DATABRICKS_TOKEN` is valid and not expired
- Ensure your `DATABRICKS_HOST` URL is correct (include `https://`)

### No Clusters Found
- Check that you have permissions to list clusters
- Verify you're connecting to the correct workspace
- Note that clusters terminated more than 30 days ago may not appear unless pinned

### Missing Policy IDs
- Policy IDs are only available if a policy was assigned at cluster creation
- Older clusters may not have policy IDs
