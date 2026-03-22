# vol_decrypt — User Guide

Operational guide for scheduling, running, and troubleshooting the NetApp ONTAP volume decryption script.

---

## Table of Contents

1. [Before You Begin](#before-you-begin)
2. [Running Manually](#running-manually)
3. [Scheduling with Cron](#scheduling-with-cron)
4. [Understanding the Output](#understanding-the-output)
5. [Reading Log Files](#reading-log-files)
6. [Monitoring Progress](#monitoring-progress)
7. [Troubleshooting](#troubleshooting)
8. [Common Scenarios](#common-scenarios)
9. [FAQ](#faq)

---

## Before You Begin

### 1. Install Python and dependencies

```bash
# Verify Python 3.10+
python3 --version

# Create a virtual environment (recommended)
cd /path/to/vol_decrypt
python3 -m venv .venv
source .venv/bin/activate

# Install the netapp-ontap library
pip install -r requirements.txt
```

### 2. Verify cluster connectivity

```bash
# Can the script host reach the cluster management LIF?
curl -sk https://10.0.0.1/api/cluster | python3 -m json.tool
```

You should see a JSON response with cluster info. If not, check firewall rules and DNS.

### 3. Prepare credentials

**Option A — Environment variable (recommended for cron):**

```bash
export ONTAP_PASSWORD='your_password_here'
```

**Option B — Command-line argument:**

```bash
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --password 'your_password_here'
```

> **Security tip:** Avoid putting passwords in crontab lines. Use the environment variable approach with a protected file (see [Scheduling with Cron](#scheduling-with-cron)).

### 4. Do a dry run first

Always start with `--dry-run` to validate connectivity, volume discovery, and aggregate selection without making any changes:

```bash
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --dry-run
```

---

## Running Manually

### Basic run

```bash
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin
```

### Scope to a single SVM

```bash
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --svm vs_production
```

### Exclude specific volumes

```bash
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin \
  --exclude-volume audit_vol \
  --exclude-volume root_vs1
```

### Reduce concurrency

If you want to limit the cluster impact, lower the concurrent move limit:

```bash
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --max-concurrent 2
```

### Adjust capacity threshold

Lower (stricter) or raise (more aggressive) the aggregate usage ceiling:

```bash
# More conservative — don't fill aggregates beyond 60%
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --capacity-threshold 60

# More aggressive — allow up to 80%
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --capacity-threshold 80
```

### Enable SSL verification

For production clusters with valid certificates:

```bash
python3 vol_decrypt.py --cluster cluster1.example.com --username admin --verify-ssl
```

---

## Scheduling with Cron

### Step 1: Create a credentials file

```bash
# Create a file only readable by the cron user
cat > /path/to/vol_decrypt/.env << 'EOF'
export ONTAP_PASSWORD='your_password_here'
EOF
chmod 600 /path/to/vol_decrypt/.env
```

### Step 2: Create a wrapper script

```bash
cat > /path/to/vol_decrypt/run_vol_decrypt.sh << 'SCRIPT'
#!/bin/bash
# Wrapper for cron execution

# Load credentials
source /path/to/vol_decrypt/.env

# Activate virtual environment
source /path/to/vol_decrypt/.venv/bin/activate

# Run the decryption script
python3 /path/to/vol_decrypt/vol_decrypt.py \
  --cluster 10.0.0.1 \
  --username admin \
  --max-concurrent 6 \
  --capacity-threshold 70

# Deactivate
deactivate
SCRIPT

chmod +x /path/to/vol_decrypt/run_vol_decrypt.sh
```

### Step 3: Add to crontab

```bash
crontab -e
```

Add one of these entries:

```cron
# Every 2 hours
0 */2 * * * /path/to/vol_decrypt/run_vol_decrypt.sh >> /path/to/vol_decrypt/logs/cron.log 2>&1

# Every 4 hours during business hours (6am-10pm)
0 6,10,14,18,22 * * * /path/to/vol_decrypt/run_vol_decrypt.sh >> /path/to/vol_decrypt/logs/cron.log 2>&1

# Every 3 hours, weekdays only
0 */3 * * 1-5 /path/to/vol_decrypt/run_vol_decrypt.sh >> /path/to/vol_decrypt/logs/cron.log 2>&1
```

### Step 4: Verify cron is running

```bash
# Check cron log
tail -f /path/to/vol_decrypt/logs/cron.log

# List recent log files
ls -lt /path/to/vol_decrypt/logs/
```

---

## Understanding the Output

### Console output (INFO level)

```
2026-03-22 14:00:00  INFO      ========================================================================
2026-03-22 14:00:00  INFO      vol_decrypt run started
2026-03-22 14:00:00  INFO      ========================================================================
2026-03-22 14:00:00  INFO      Cluster:            10.0.0.1
2026-03-22 14:00:00  INFO      Max concurrent:     6
2026-03-22 14:00:00  INFO      Capacity threshold: 70%
2026-03-22 14:00:00  INFO      Dry-run:            False
2026-03-22 14:00:00  INFO      ...
2026-03-22 14:00:01  INFO      Connected to cluster: 10.0.0.1 (SSL verify: False)
2026-03-22 14:00:01  INFO      Checking in-flight volume moves from previous runs...
2026-03-22 14:00:02  INFO      In-flight volume moves: 2
2026-03-22 14:00:02  INFO        vol_data_03                     SVM: vs1             State: replicating     Progress: 45%  Dest: aggr2
2026-03-22 14:00:02  INFO        vol_data_07                     SVM: vs1             State: cutover_wait    Progress: 100% Dest: aggr1
2026-03-22 14:00:02  INFO      Available move slots: 4 (max 6, in-flight 2)
2026-03-22 14:00:02  INFO      ...
2026-03-22 14:00:03  INFO      Found 5 encrypted volume(s) eligible for decryption:
2026-03-22 14:00:03  INFO        vol_data_01                     SVM: vs1             Size:   100.00 GiB  Used:    45.23 GiB  Aggr: aggr1                      Node: node1
2026-03-22 14:00:03  INFO      ...
2026-03-22 14:00:03  INFO      Planning move for vol_data_01 (SVM: vs1): aggr1 -> aggr2 [SAME-NODE]  Projected usage: 62.3%
2026-03-22 14:00:04  INFO      Volume move started: vol_data_01 (SVM: vs1) -> aggregate aggr2 (encrypt-destination: false)
2026-03-22 14:00:05  INFO      ========================================================================
2026-03-22 14:00:05  INFO      Run summary
2026-03-22 14:00:05  INFO      ========================================================================
2026-03-22 14:00:05  INFO      Encrypted volumes discovered:  5
2026-03-22 14:00:05  INFO      In-flight moves (prior runs):  2
2026-03-22 14:00:05  INFO      Moves started this run:        4
2026-03-22 14:00:05  INFO      Skipped (capacity threshold):  1
2026-03-22 14:00:05  INFO      Skipped (API error):           0
```

### What each section means

| Section | Meaning |
|---|---|
| **In-flight moves** | Moves started by a previous run that are still running. These count against `--max-concurrent`. |
| **Available move slots** | How many new moves can be submitted this run. |
| **Aggregate list** | Every data aggregate with current space usage. Helps you verify which aggregates have room. |
| **Encrypted volumes** | All NVE-encrypted volumes matching your filters. |
| **Planning move** | Shows source/target aggregate, whether same-node or cross-node, and projected aggregate utilization. |
| **Run summary** | Final counts: started, skipped, deferred. |

---

## Reading Log Files

Log files are created in `./logs/` (or `--log-dir`) with the naming pattern:

```
vol_decrypt_YYYYMMDD_HHMMSS.log
```

The log file contains **DEBUG-level** detail — more than what's printed to the console:

```bash
# View the latest log
ls -t logs/ | head -1 | xargs -I{} cat logs/{}

# Search for errors in logs
grep -i "error\|warning\|skipped\|failed" logs/vol_decrypt_20260322_*.log

# Count moves started across all runs
grep "Volume move started" logs/*.log | wc -l

# Find volumes that were skipped due to capacity
grep "SKIPPED" logs/*.log
```

### Log retention

The script does **not** automatically delete old logs. Set up log rotation:

```bash
# Example: delete logs older than 30 days (add to cron)
find /path/to/vol_decrypt/logs -name "vol_decrypt_*.log" -mtime +30 -delete
```

---

## Monitoring Progress

### Check volume move status in ONTAP

After the script starts moves, you can monitor progress directly on the cluster:

```bash
# ONTAP CLI
ssh admin@10.0.0.1
cluster1::> volume move show -fields state,percent-complete,estimated-completion-time

# REST API
curl -sk -u admin:password \
  'https://10.0.0.1/api/storage/volumes?movement.state=!&fields=name,movement' | python3 -m json.tool
```

### Verify decryption completed

Once a volume move finishes, verify encryption is off:

```bash
# ONTAP CLI
cluster1::> volume show -encryption-type none -fields encryption-type

# REST API — find unencrypted volumes
curl -sk -u admin:password \
  'https://10.0.0.1/api/storage/volumes?encryption.enabled=false&fields=name,encryption' | python3 -m json.tool
```

### Check how many encrypted volumes remain

```bash
curl -sk -u admin:password \
  'https://10.0.0.1/api/storage/volumes?encryption.enabled=true&encryption.type=volume&fields=name' | python3 -m json.tool
```

---

## Troubleshooting

### Connection errors

**Symptom:** `Failed to establish a new connection` or `Connection refused`

**Causes & fixes:**
- Cluster management LIF is unreachable — check network routing, firewall rules.
- Wrong IP/hostname — verify `--cluster` value.
- HTTPS not enabled on management LIF — check `system services web show` on the cluster.

```bash
# Test connectivity
curl -vsk https://10.0.0.1/api/cluster 2>&1 | head -20
```

### Authentication errors

**Symptom:** `401 Unauthorized` or `Authentication failed`

**Causes & fixes:**
- Wrong username/password — double-check credentials.
- Account locked — verify on cluster: `security login show -user-or-group-name <user>`.
- Password from env var not loaded — check `echo $ONTAP_PASSWORD` (should not be empty).

### SSL certificate errors

**Symptom:** `SSLError` or `certificate verify failed`

**Causes & fixes:**
- Self-signed certificate on cluster — use `--no-verify-ssl` (default) or install the cluster CA cert.
- If using `--verify-ssl`, set the `REQUESTS_CA_BUNDLE` environment variable to point to your CA bundle.

### "No eligible encrypted volumes found"

**Causes:**
- All volumes are already decrypted ✅ (expected after all moves complete).
- `--svm` filter is wrong — try without `--svm` to see all volumes.
- Volumes have NAE (aggregate-level) encryption, not NVE — this script handles NVE only.
- All encrypted volumes are already in a move.

```bash
# Check what encryption types exist
curl -sk -u admin:password \
  'https://10.0.0.1/api/storage/volumes?encryption.enabled=true&fields=name,encryption.type' | python3 -m json.tool
```

### "No aggregate found with projected usage ≤ 70%"

**Causes & fixes:**
- All aggregates are too full. Free space by deleting snapshots, decommissioning old volumes, or adding disks.
- Raise `--capacity-threshold` (e.g., `--capacity-threshold 80`) if you accept higher utilization.
- The volume itself is very large — check aggregate sizes vs. volume size in the log.

### "Max concurrent limit reached"

**Not an error.** The script respects the concurrency limit. Previously started moves are still running. Solutions:
- Wait for in-flight moves to complete (ONTAP handles them asynchronously).
- Increase `--max-concurrent` if the cluster can handle more simultaneous moves.
- Run the script more frequently (e.g., every 1 hour instead of every 2 hours).

### Volume move fails after submission

The script submits moves asynchronously — ONTAP may reject them later. Check:

```bash
# ONTAP CLI
cluster1::> volume move show -fields state,details

# Look for failed moves
cluster1::> volume move show -state failed
```

Common reasons: destination aggregate offline, insufficient space (race condition with other operations), or the volume has a SnapMirror relationship that prevents the move.

---

## Common Scenarios

### Scenario 1: First-time run on a large cluster

```bash
# Step 1: Dry run to see what will happen
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --dry-run

# Step 2: Review the log
cat logs/vol_decrypt_*.log

# Step 3: Start with low concurrency
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --max-concurrent 2

# Step 4: Once confident, set up cron with default concurrency
```

### Scenario 2: Decrypt volumes in a specific SVM only

```bash
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --svm vs_finance
```

### Scenario 3: Skip protected volumes

Some volumes should remain encrypted (e.g., audit logs, compliance data):

```bash
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin \
  --exclude-volume audit_vol \
  --exclude-volume compliance_data \
  --exclude-volume snaplock_vol
```

### Scenario 4: Aggregates are nearly full

```bash
# Raise threshold to 85% (accept more risk)
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --capacity-threshold 85

# Or reduce concurrency to move fewer volumes at once
python3 vol_decrypt.py --cluster 10.0.0.1 --username admin --max-concurrent 2
```

### Scenario 5: Multiple clusters

Run the script separately for each cluster:

```bash
python3 vol_decrypt.py --cluster cluster1.example.com --username admin --log-dir ./logs/cluster1
python3 vol_decrypt.py --cluster cluster2.example.com --username admin --log-dir ./logs/cluster2
```

---

## FAQ

**Q: Does the volume move cause downtime?**
A: No. ONTAP volume moves are non-disruptive. Clients continue accessing data during the move. There is a brief cutover phase (typically seconds) at the end.

**Q: Can I run the script while other volume moves are happening?**
A: Yes. The script detects in-flight moves and counts them against the concurrency limit. It will not exceed `--max-concurrent` total moves.

**Q: What if the script is interrupted mid-run?**
A: Moves already submitted to ONTAP continue running. The next invocation will detect them as in-flight and proceed with remaining volumes.

**Q: Does the script move the volume to a different aggregate?**
A: It prefers an aggregate on the **same node** to minimize data transfer. If no same-node aggregate has enough space, it selects one on another node (cross-node move). If the volume's current aggregate is the best candidate, ONTAP performs an in-place move (data stays on the same disks).

**Q: How long does a volume move take?**
A: Depends on volume size and cluster load. A 100 GiB volume typically takes 10–60 minutes. The script does not wait for completion — it submits the move and exits.

**Q: How do I know when all volumes are decrypted?**
A: Run the script with `--dry-run`. If it reports "No eligible encrypted volumes found", all NVE volumes have been decrypted.

**Q: Does this handle NAE (aggregate-level) encryption?**
A: No. This script targets NVE (volume-level) encryption only. NAE decryption requires different procedures (aggregate-level changes).
