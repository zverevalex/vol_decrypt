# vol_decrypt — NetApp ONTAP Volume Decryption Tool

Automated Python script that disables NVE (NetApp Volume Encryption) on ONTAP volumes by performing non-disruptive volume move operations via the ONTAP REST API. Designed to run unattended as a cron job.

## How It Works

ONTAP does not allow toggling encryption in-place on an existing volume. The standard procedure to decrypt a volume is to **move it to an aggregate with encryption disabled**. This is the equivalent of the CLI command:

```
volume move start -vserver <svm> -volume <vol> -destination-aggregate <aggr> -encrypt-destination false
```

The script automates this end-to-end:

1. **Connects** to the ONTAP cluster management LIF using supplied credentials.
2. **Reports** any in-flight volume moves from previous runs (audit trail).
3. **Discovers** all NVE-encrypted, read-write volumes (filtering by SVM and exclusion list if provided).
4. **Discovers** all data aggregates and their current space utilization.
5. **Selects a target aggregate** for each volume using a two-tier strategy:
   - **Tier 1 (same-node):** Prefer an aggregate on the same node as the volume's current aggregate. Picks the one with the most available space — as long as projected utilization after the move stays under the capacity threshold (default 70 %).
   - **Tier 2 (cross-node fallback):** If no same-node aggregate qualifies, considers aggregates on other nodes.
   - If no aggregate anywhere meets the threshold, the volume is **skipped** with a warning.
6. **Initiates `volume move`** operations (up to the concurrent limit, default 6) with encryption disabled.
7. **Logs** every decision, action, and summary to a per-run log file.

Because volume moves are non-disruptive and handled asynchronously by ONTAP, the script **submits** moves and exits. The next cron invocation will detect in-flight moves (counting them against the concurrency limit) and continue processing any remaining encrypted volumes.

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.10+ |
| `netapp-ontap` library | ≥ 9.14 |
| ONTAP cluster | 9.6+ (REST API must be enabled) |
| Cluster credentials | Admin-level or delegated `volume move` privilege |
| Network | Script host must reach the cluster management LIF on port 443 |

## Installation

```bash
# Clone or copy the project
cd /path/to/vol_decrypt

# (Optional) Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

```bash
# Basic run — password from env var
export ONTAP_PASSWORD='s3cret'
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin

# Dry-run (no changes, just logs what would happen)
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --dry-run

# Password inline, scope to one SVM, lower concurrency
python3 vol_decrypt.py --cluster cluster1.example.com --username admin \
  --password 's3cret' --svm vs_prod --max-concurrent 4
```

## CLI Arguments Reference

| Argument | Required | Default | Description |
|---|---|---|---|
| `--cluster` | ✅ | — | Cluster management IP or hostname |
| `--username` | ✅ | — | Admin username |
| `--password` | — | `$ONTAP_PASSWORD` | Admin password. Falls back to `ONTAP_PASSWORD` env var |
| `--max-concurrent` | — | `6` | Max simultaneous volume move operations cluster-wide |
| `--capacity-threshold` | — | `70` | Max projected aggregate usage (%) after a move |
| `--dry-run` | — | `false` | Log planned moves without executing them |
| `--verify-ssl` | — | `false` | Verify SSL certificates (disabled by default) |
| `--svm` | — | all SVMs | Restrict to a single SVM |
| `--exclude-volume` | — | none | Volume name(s) to skip (repeatable) |
| `--log-dir` | — | `./logs/` | Directory for per-run log files |

## Project Structure

```
vol_decrypt/
├── ontap_migrate.py     # Entry point: replicate / collect / cutover
├── migrate/             # Migration package
│   ├── __init__.py      # Public re-exports
│   ├── snapmirror.py    # Module: SnapMirror replication + DP volume creation
│   └── cutover.py       # Module: CIFS/NFS share collection + cutover logic
├── tests/               # Test suite
│   ├── __init__.py
│   └── smoke_test.py    # 54 mock-based smoke tests (no live cluster needed)
├── vol_decrypt.py       # Original: volume decryption via volume move
├── cutover_state.json   # Runtime state file (auto-generated, git-ignored)
├── requirements.txt     # Python dependencies
├── README.md            # This file
└── USER_GUIDE.md        # Operational guide
```

---

## SnapMirror Migration (`ontap_migrate.py`)

Semi-automatic volume migration from a source ONTAP cluster/SVM to a
destination cluster/SVM using SnapMirror as the data transport.

### Workflow

```
1. replicate  →  Discover source volumes
                 Select unencrypted destination aggregate
                 Create DP volumes on destination
                 Establish SnapMirror relationships (bulk)
                 Start initial transfer

2. collect    →  Read CIFS shares / NFS export policies + rules from source
                 Write cutover_state.json (includes CIFS ACLs + nfs_policies)
                 Persist explicit volume_names list for cutover execution
                 (nfs_policies block contains full rule definitions)

3. cutover    →  Load cutover_state.json
                 Show summary + prompt for confirmation
                 For each volume in volume_names:
                   Skip if already listed in migrated_volumes (warning log)
                   Run final SnapMirror update (blocking)
                   Break SnapMirror (state: broken_off)
                   Unmount source volume (remove junction_path)
                   Mount destination volume (set junction_path)
                   Re-create CIFS shares (with ACLs) or NFS export policies
                   on destination
                   (skipped for same-SVM migrations — remount only)
                   Rename source volume to <name>_delete
                   Set renamed source volume state to offline
                   Rename destination volume from <name>_dst to <name>
                   Mark volume as migrated in cutover_state.json
```

### Quick Start

```bash
# Step 1 — Replicate
python3 ontap_migrate.py replicate \
  --source-cluster 10.0.0.1 --source-username admin \
  --destination-cluster 10.0.0.2 --destination-username admin \
  --source-svm vs_prod --protocol cifs

# Step 2 — Collect share/export state
python3 ontap_migrate.py collect \
  --source-cluster 10.0.0.1 --source-username admin \
  --destination-cluster 10.0.0.2 --destination-username admin \
  --source-svm vs_prod --protocol cifs

# Step 3 — Execute cutover (interactive confirmation required)
python3 ontap_migrate.py cutover \
  --source-cluster 10.0.0.1 --source-username admin \
  --destination-cluster 10.0.0.2 --destination-username admin \
  --source-svm vs_prod --protocol cifs
```

Passwords can be provided via `--source-password` / `--destination-password`,
via the `ONTAP_SRC_PASSWORD` / `ONTAP_DST_PASSWORD` environment variables,
or interactively at the prompt.

### CLI Arguments Reference

| Argument | Commands | Required | Default | Description |
|---|---|---|---|---|
| `--source-cluster` | all | ✅ | — | Source cluster management IP or hostname |
| `--source-username` | all | ✅ | — | Admin username for source cluster |
| `--source-password` | all | — | `$ONTAP_SRC_PASSWORD` | Source cluster password |
| `--destination-cluster` | all | ✅ | — | Destination cluster management IP or hostname |
| `--destination-username` | all | ✅ | — | Admin username for destination cluster |
| `--destination-password` | all | — | `$ONTAP_DST_PASSWORD` | Destination cluster password |
| `--source-svm` | all | ✅ | — | Name of the source SVM |
| `--destination-svm` | all | — | `<source-svm>_dst` | Name of the destination SVM |
| `--protocol` | all | — | `cifs` | Protocol to migrate: `cifs`, `nfs`, or `both` |
| `--exclude-volumes` | replicate, collect | — | none | Volume name(s) to skip |

### Destination Volume Naming

Source volumes are replicated with a `_dst` suffix on the destination:

| Source | Destination |
|---|---|
| `vol_sales` | `vol_sales_dst` |
| `vol_finance` | `vol_finance_dst` |

The destination volume inherits `size`, `language`, and
`security_style` from the source.

### Same-SVM Cutover

When `--source-svm` and `--destination-svm` refer to the same SVM:

- SVM peering is **skipped** entirely.
- CIFS share/ACL and NFS export policy recreation is **skipped**.
- SnapMirror break + volume remount is performed.
- Source volume is renamed to `<name>_delete` and then set to `offline`.
- Destination volume is renamed from `<name>_dst` to the original name.

### Same-Cluster Migration

When `--source-cluster` and `--destination-cluster` are the same host
(case-insensitive comparison):

- Source credentials are **reused** for the destination — no second
  password prompt.
- A single `HostConnection` is used for both source and destination
  operations.

### NFS Export Policy Migration

During `collect`, for each NFS volume the full export policy and all
rules are read from the source SVM and persisted in `cutover_state.json`
under `nfs_policies`. During `cutover`, the destination policy is
created via a single `ExportPolicy.post` call including all rules.

If a policy with the same name already exists on the destination SVM,
it is **skipped** with a warning — no overwrite is performed.

### Migration Progress Tracking

`cutover_state.json` contains a `migrated_volumes` list that is updated
after each successfully completed volume cutover. On subsequent `cutover`
runs (e.g. after a partial failure or intentional interruption), any
volume already present in `migrated_volumes` is **skipped** with a
warning log entry — no duplicate work is performed. Volumes not yet in
the list are processed normally.

The state file also contains `volume_names`, which is used as the
primary source for cutover iteration. This ensures cutover still runs
for replicated volumes even when no CIFS share or NFS export entries
exist for a volume.

### CIFS ACL Migration

During `collect`, CIFS share ACLs are captured via the share `acls`
field and persisted to `cutover_state.json`. During cross-SVM `cutover`,
ACLs are included when destination shares are recreated, preserving share
permissions.

### Architecture

```
┌─────────────────────────────────────────────┐
│              ontap_migrate.py               │
│  OntapMigrate.run_replicate()               │
│  OntapMigrate.run_collect()                 │
│  OntapMigrate.run_cutover()                 │
└──────────┬──────────────────┬───────────────┘
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌───────────────────────┐
│  snapmirror.py   │  │      cutover.py        │
│                  │  │                        │
│  Aggregate sel.  │  │  collect_cifs_shares() │
│  DP vol create   │  │  collect_nfs_exports() │
│  SnapMirror bulk │  │  write_cutover_state() │
│  post_collection │  │  CutoverExecutor       │
└──────────────────┘  └───────────────────────┘
           │                  │
           └────────┬─────────┘
                    ▼
        ONTAP Cluster (src + dst)
        HTTPS / REST API (port 443)
```



## Architecture

```
┌─────────────────────────────────┐
│          vol_decrypt.py         │
│                                 │
│  argparse → connect → discover  │
│       → select aggr → move      │
│               → log             │
└───────────┬─────────────────────┘
            │  HTTPS / REST API
            ▼
┌─────────────────────────────────┐
│     ONTAP Cluster Mgmt LIF     │
│  ┌─────────┐   ┌─────────────┐ │
│  │ Volumes  │   │ Aggregates  │ │
│  │  (NVE)   │   │ (per-node)  │ │
│  └─────────┘   └─────────────┘ │
└─────────────────────────────────┘
```

### API Endpoints Used

| Endpoint | Method | Purpose |
|---|---|---|
| `/api/storage/volumes` | GET | Discover encrypted volumes, in-flight moves |
| `/api/storage/volumes/{uuid}` | GET | Fetch individual volume details |
| `/api/storage/volumes/{uuid}` | PATCH | Start volume move with `encryption.enabled: false` |
| `/api/storage/aggregates` | GET | List aggregates with space info |

### Aggregate Selection Logic

```
For each encrypted volume:
  1. Get current node from volume's aggregate
  2. Evaluate same-node aggregates:
     - projected_usage = (aggr_used + vol_used) / aggr_total * 100
       (if same aggregate → no net space addition)
     - Filter: projected_usage ≤ capacity_threshold
     - Pick: most available space
  3. If none found → evaluate cross-node aggregates (same logic)
  4. If still none → skip volume, log warning
```

### Concurrency Control

The script counts **all** in-flight volume moves cluster-wide (not just those it started) against the `--max-concurrent` limit. This prevents overloading the cluster when other vol moves are already running.

### Capacity Safety

After each successful move submission, the script updates its in-memory aggregate space map so that the **next** volume in the same run gets correct projected-usage calculations. This prevents over-committing aggregate space within a single run.

## Security Notes

- **Password** is never written to log files. Supply it via `--password` argument or the `ONTAP_PASSWORD` environment variable.
- **SSL verification** is disabled by default for lab/self-signed cert environments. Use `--verify-ssl` in production with trusted certificates.
- The script requires admin-level credentials or an account with delegated `volume move` authority.

## License

Internal tool — see your organization's licensing policy.
