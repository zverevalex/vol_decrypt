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
├── vol_decrypt.py       # Volume decryption via unencrypted aggregate move
├── vol_schedule.py      # Interactive migration planner (bulk volume selection)
├── vol_move_exec.py     # Scheduled migration executor (YAML plan processor)
├── migrate/             # Migration package
│   ├── __init__.py      # Public re-exports
│   ├── snapmirror.py    # Module: SnapMirror replication + DP volume creation
│   └── cutover.py       # Module: CIFS/NFS share collection + cutover logic
├── tests/               # Test suite
│   ├── __init__.py
│   └── smoke_test.py    # 54 mock-based smoke tests (no live cluster needed)
├── plans/               # YAML plan files (auto-created; git-ignored)
├── logs/                # Per-run log files (auto-created; git-ignored)
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

# Show tool version
python3 ontap_migrate.py --version
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
| `--log-file` | all | — | none | Optional log file path (console logging remains enabled) |
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

If a source export policy has no rules, policy reassign during cutover
is skipped for that volume.

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

---

## Bulk Volume Move Scheduling (`vol_schedule.py` & `vol_move_exec.py`)

Two-phase workflow for **non-disruptive bulk volume move operations**: interactive planning followed by unattended execution.

### Workflow

```
1. vol_schedule.py  →  Connect to cluster
                      Discover all RW, online volumes in SVM
                      Display numbered volume table
                      ▪ Prompt user: select volumes (comma-sep, or 'all')
                      ▪ For each volume, display available target aggregates
                      ▪ Prompt user: select target aggregate
                      Write YAML plan → plans/<cluster_name>_<svm>.yaml

2. vol_move_exec.py →  Read YAML plan file(s) from directory or --plan arg
                      For each plan:
                        ▪ Query ONTAP movement.state to refresh in_progress
                        ▪ Start pending moves (up to --max-concurrent)
                        ▪ Update YAML status: pending → in_progress → done/failed
                        ▪ Write updated YAML back
                      Print per-plan + combined summary
                      (Run as cron job; idempotent with flock)
```

### `vol_schedule.py` — Interactive Planning

Discovers volumes in a single SVM and interactively guides you through volume
and aggregate selection. Outputs a YAML plan file for execution.

#### Quick Start

```bash
# Interactive planning session (prompts for volume & aggregate selection)
export ONTAP_PASSWORD='s3cret'
python3 vol_schedule.py --cluster 10.0.0.1 --username admin --svm vs_prod

# Dry-run: no YAML written
python3 vol_schedule.py --cluster 10.0.0.1 --username admin --svm vs_prod \
  --dry-run

# Custom output file
python3 vol_schedule.py --cluster 10.0.0.1 --username admin --svm vs_prod \
  --output /tmp/my_migration_plan.yaml
```

#### Behaviour

1. **Discovers** all read-write, online volumes in the specified SVM
   - Skips root volumes (e.g., `vol0`)
   - Skips volumes already in-flight (status: `volume move in progress`)

2. **Renders** a numbered table:
   ```
   │ # │ Volume Name │ SVM │ Size (GiB) │ Used (GiB) │ Current Aggregate │
   ├───┼─────────────┼─────┼────────────┼────────────┼──────────────────┤
   │ 1 │ vol_data_01 │ ... │ 100        │ 45         │ aggr1_node1      │
   │ 2 │ vol_data_02 │ ... │ 200        │ 120        │ aggr1_node1      │
   ```

3. **Prompts** user to select which volumes to move:
   ```
   Select volumes to schedule (comma-separated numbers, or 'all'): 1,2,3
   ```

4. **For each selected volume**, displays available target aggregates
   (current aggregate excluded):
   ```
   vol_data_01: Current aggregate is aggr1_node1
   Available targets:
   │ # │ Aggregate Name │ Node │ Total (GiB) │ Used (GiB) │ Available (GiB) │
   ├───┼────────────────┼──────┼─────────────┼────────────┼─────────────────┤
   │ 1 │ aggr2_node1    │ ...  │ 1000        │ 450        │ 550             │
   │ 2 │ aggr1_node2    │ ...  │ 1500        │ 600        │ 900             │
   
   Select target aggregate: 1
   ```

5. **Writes** a YAML plan file:
   ```yaml
   cluster: "10.0.0.1"
   svm: "vs_prod"
   volumes:
     - name: "vol_data_01"
       uuid: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
       target_aggregate: "aggr2_node1"
       status: "pending"
       error: null
     - name: "vol_data_02"
       uuid: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
       target_aggregate: "aggr1_node2"
       status: "pending"
       error: null
   ```

#### CLI Arguments

| Argument | Required | Default | Description |
|---|---|---|---|
| `--cluster` | ✅ | — | Cluster management IP or hostname |
| `--username` | ✅ | — | Admin username |
| `--password` | — | `$ONTAP_PASSWORD` | Admin password |
| `--svm` | ✅ | — | SVM name to discover volumes |
| `--output` | — | `plans/<cluster_name>_<svm>.yaml` | Output YAML plan file path |
| `--dry-run` | — | `false` | Prompt for selections but don't write YAML |
| `--verify-ssl` | — | `false` | Verify SSL certificates |
| `--log-dir` | — | `./logs/` | Directory for log files |

#### Logs

- Timestamped log file: `logs/vol_schedule_<YYYYMMDD_HHMMSS>.log`
- All prompts, volume discoveries, and YAML write operations are logged

---

### `vol_move_exec.py` — Scheduled Execution

Reads YAML plan file(s) and orchestrates volume move operations with
concurrency limits and idempotent status tracking. Designed to run unattended
as a cron job.

#### Quick Start

```bash
# Single plan file (cluster from --cluster arg)
export ONTAP_PASSWORD='s3cret'
python3 vol_move_exec.py --plan plans/10.0.0.1_vs_prod.yaml \
  --cluster 10.0.0.1 --username admin

# All plans in directory (cluster read from each YAML)
python3 vol_move_exec.py --plans-dir ./plans/ --username admin

# Dry-run: log what would happen (no ONTAP changes)
python3 vol_move_exec.py --plans-dir ./plans/ --username admin --dry-run

# Lower concurrency (default 6)
python3 vol_move_exec.py --plans-dir ./plans/ --username admin \
  --max-concurrent 3
```

#### Behaviour

On each run (e.g., every 30 minutes via cron):

1. **Reads** YAML plan file(s)
   - Single mode: `--plan <file> --cluster <host>`
   - Directory mode: `--plans-dir <dir>` (defaults to `./plans/`; reads cluster from YAML)

2. **For each plan:**
   - **Refreshes** in-flight volume move statuses by querying ONTAP
     `movement.state` REST endpoint
   - Any `in_progress` move that has completed is marked `done` or `failed`

3. **Starts** pending moves:
   - Counts currently `in_progress` moves cluster-wide (including external moves)
   - Starts up to `--max-concurrent` pending moves (default 6) per cluster
   - Runs ONTAP `PATCH /api/storage/volumes/{uuid}` with
     `movement.destination_aggregate` set

4. **Updates YAML** with new statuses and writes back to disk
   - Status never reverts (e.g., `done` stays `done`)
   - Sets `error` field if move fails

5. **Prints** per-plan summary:
   ```
   Plan: plans/10.0.0.1_vs_prod.yaml
   ├─ Cluster: 10.0.0.1 / SVM: vs_prod
   ├─ Pending: 2 | In Progress: 1 | Done: 3 | Failed: 0
   └─ Started this run: 2

   Combined (all plans):
   ├─ Pending: 5 | In Progress: 3 | Done: 8 | Failed: 1
   ```

#### Status Lifecycle

```
pending  ─(start move)→  in_progress  ─(query state)→  done
                                                 └→  failed
```

- `pending`: Awaiting execution (initial state from `vol_schedule.py`)
- `in_progress`: Move operation started; ONTAP is moving the volume
- `done`: Move completed successfully (queried from ONTAP)
- `failed`: Move failed or aborted (queried from ONTAP, `error` field populated)

#### Idempotency & Cron Safety

The script is **fully idempotent**:
- If a volume's move is already `in_progress`, it is not restarted
- If a volume is already `done`, it is skipped
- No operation is repeated on subsequent runs

**Prevent overlapping executions** with `flock`:

```bash
# crontab entry (every 30 minutes)
*/30 * * * * flock -n /tmp/vol_move_exec.lock \
  python3 /path/to/vol_move_exec.py --plans-dir ./plans/ \
  --username admin >> /var/log/vol_move_exec_cron.log 2>&1
```

If the script is already running, `flock -n` will exit immediately without queuing.

#### Logs

- Timestamped log file: `logs/vol_move_exec_<YYYYMMDD_HHMMSS>.log`
- Status refreshes, move initiations, and per-plan summaries are logged

#### CLI Arguments

| Argument | Mode | Default | Description |
|---|---|---|---|
| `--plan` | single | — | Single YAML plan file path |
| `--cluster` | single | — | Cluster IP/hostname (for `--plan` mode only) |
| `--username` | both | ✅ | Admin username |
| `--password` | both | `$ONTAP_PASSWORD` | Admin password |
| `--plans-dir` | directory | `./plans/` | Directory of plan files (reads cluster from each) |
| `--max-concurrent` | both | `6` | Max simultaneous volume move operations per cluster |
| `--dry-run` | both | `false` | Log planned operations without executing them |
| `--verify-ssl` | both | `false` | Verify SSL certificates |
| `--log-dir` | both | `./logs/` | Directory for log files |

---

### YAML Plan Schema

Generated by `vol_schedule.py` and consumed by `vol_move_exec.py`:

```yaml
cluster: "10.0.0.1"
svm: "vs_prod"
volumes:
  - name: "vol_data_01"
    uuid: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    target_aggregate: "aggr2_node1"
    status: "pending"           # pending | in_progress | done | failed
    error: null                 # null or error message from ONTAP
  - name: "vol_data_02"
    uuid: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
    target_aggregate: "aggr1_node2"
    status: "in_progress"
    error: null
  - name: "vol_archive"
    uuid: "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz"
    target_aggregate: "aggr3_node1"
    status: "done"
    error: null
```

#### Workflow Example: Three Clusters

1. **Plan** — on three separate clusters:
   ```bash
   # Cluster A
   vol_schedule.py --cluster clusterA.local --svm vs_prod
   # → plans/clusterA.local_vs_prod.yaml (pending volumes)

   # Cluster B
   vol_schedule.py --cluster clusterB.local --svm vs_data
   # → plans/clusterB.local_vs_data.yaml (pending volumes)

   # Cluster C
   vol_schedule.py --cluster clusterC.local --svm vs_app
   # → plans/clusterC.local_vs_app.yaml (pending volumes)
   ```

2. **Execute** — centralized cron job:
   ```bash
   # Every 30 min: reads all three plans, refreshes status, starts moves
   */30 * * * * flock -n /tmp/vol_move_exec.lock \
     python3 vol_move_exec.py --plans-dir ./plans/ --username admin
   ```

3. **Monitor** — check logs or re-run vol_move_exec.py anytime:
   ```bash
   # View current status across all plans
   python3 vol_move_exec.py --plans-dir ./plans/ --username admin
   # (Queries each cluster, prints summary, no changes if all are done)
   ```

---

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
