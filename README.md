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
├── vol_decrypt.py       # Main script
├── requirements.txt     # Python dependencies
├── README.md            # This file
├── USER_GUIDE.md        # Operational guide (scheduling, troubleshooting)
└── logs/                # Per-run log files (created automatically)
    └── vol_decrypt_20260322_140000.log
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
