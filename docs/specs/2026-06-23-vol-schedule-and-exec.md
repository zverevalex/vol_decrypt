# Vol-Schedule & Vol-Move-Exec — Implementation Spec

Status: Done
Date: 2026-06-23

## Goal

Two standalone Python scripts that together automate the scheduling and execution of
ONTAP volume-move operations (typically used for decryption or aggregate rebalancing):

1. **`vol_schedule.py`** — an interactive CLI that discovers volumes in a given SVM,
   lets the operator select which volumes to schedule and which aggregate each should
   move to, and persists the plan as a YAML file.

2. **`vol_move_exec.py`** — a non-interactive scheduler-ready script that reads the
   YAML plan, checks available vol-move slots (same logic as `vol_decrypt.py`), starts
   pending moves, and writes the updated status back to the YAML file.

## Scope

- In scope:
  - `vol_schedule.py` — interactive volume/aggregate selection, YAML write.
  - `vol_move_exec.py` — YAML read, slot-check, vol-move start, YAML status update.
  - Reuse of `connect()` / `get_aggregates()` / `get_in_flight_moves()` helpers from
    `vol_decrypt.py` (copy/extract — no shared package yet, keep scripts self-contained
    for now to match the existing repo layout).
  - YAML status lifecycle: `pending` → `in_progress` → `done` / `failed`.
  - Credentials from env vars (`ONTAP_PASSWORD`) or `--password` flag, matching
    existing pattern in `vol_decrypt.py`.

- Out of scope (YAGNI):
  - Packaging into a module/library.
  - GUI or TUI (plain `input()` prompts are sufficient).
  - Multi-cluster YAML plans (single cluster per YAML).
  - SVM auto-discovery (SVM is a required argument).
  - Rollback / undo of started moves.

## Design summary

Transaction-script style, consistent with `vol_decrypt.py`. Each script is a single
flat module: constants → helpers → ONTAP interactions → orchestration → `main()`.
Credentials follow the existing `ONTAP_PASSWORD` env-var pattern. The YAML file is
written/read with the stdlib `yaml` module from PyYAML (already available via
`netapp_ontap` transitive deps; add explicitly to `requirements.txt`).

### YAML plan schema

```yaml
cluster: "cluster1.example.com"
svm: "vs_prod"
volumes:
  - name: "vol_data_01"
    uuid: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    target_aggregate: "aggr1_node1"
    status: "pending"   # pending | in_progress | done | failed
    error: null         # last error message, or null
  - ...
```

## Tasks

- [ ] **T1 — Implement `vol_schedule.py`**
  Files: `vol_schedule.py`

  Behaviour:
  1. `argparse` CLI: `--cluster` (required), `--username` (required),
     `--password` (optional; falls back to `$ONTAP_PASSWORD` then `getpass`),
     `--svm` (required), `--output` (optional; default `migration_plan.yaml`),
     `--verify-ssl` (flag, default False),
     `--log-dir` (default `./logs/`).
  2. Call `setup_logging(log_dir)` (same pattern as `vol_decrypt.py`) to create a
     timestamped file `logs/vol_schedule_<YYYYMMDD_HHMMSS>.log` at DEBUG level and a
     console handler at INFO level.
  3. Connect to cluster via `netapp_ontap.HostConnection`.
  3. Discover all RW, online volumes in the SVM (fields: `uuid,name,svm.name,size,
     space.used,aggregates.name`). Skip root volumes (name ends with `_root` or
     equals `vol0`) and volumes already in an active move.
  4. Print a numbered table of discovered volumes (name, SVM, size GiB, used GiB,
     current aggregate).
  5. Prompt: "Enter volume numbers to schedule (comma-separated, or 'all'):" —
     validate input; re-prompt on invalid entry.
  6. Discover aggregates (fields: `uuid,name,node.name,space.block_storage.*`).
     Print a numbered table per aggregate: name, node, total GiB, used GiB,
     free GiB, usage %.
  7. For each selected volume, prompt: "Select target aggregate for <vol_name>
     (enter number):" — validate, re-prompt on invalid. Disallow selecting the
     volume's current aggregate.
  8. Build the YAML structure (see schema above), all entries with
     `status: pending`, `error: null`.
  9. If `--output` file already exists, ask: "File exists. Overwrite? [y/N]:" and
     abort if not confirmed.
  10. Write YAML to `--output` path.
  11. Print confirmation: "Plan written to <path> — <n> volumes scheduled."

  Done when: `python vol_schedule.py --help` exits 0; unit tests for
  `build_plan_entry()` and `format_volume_table()` pass; no ruff/mypy errors.

- [ ] **T2 — Implement `vol_move_exec.py`**
  Files: `vol_move_exec.py`

  Behaviour:
  1. `argparse` CLI: `--plan` (required; path to YAML file), `--cluster`
     (required), `--username` (required), `--password` (optional; falls back to
     `$ONTAP_PASSWORD` then `getpass`), `--max-concurrent` (default 6),
     `--dry-run` (flag), `--verify-ssl` (flag),
     `--log-dir` (default `./logs/`).
  2. Call `setup_logging(log_dir)` to create `logs/vol_move_exec_<YYYYMMDD_HHMMSS>.log`
     at DEBUG level + console at INFO (same pattern as `vol_decrypt.py`).
  3. Load and validate the YAML plan (schema check: required keys present,
     status values valid).
  3. Connect to cluster.
  4. Query `movement.state` for every volume in the plan whose status is
     `in_progress` — transition to `done` if state is absent or `success`,
     to `failed` if state is `failed`.
  5. Count in-flight moves cluster-wide via `get_in_flight_moves()` (same
     logic as `vol_decrypt.py`). Compute available slots =
     `max_concurrent - len(in_flight)`.
  6. For each `pending` entry (in order), if slots remain:
     a. Issue `Volume.patch(poll=False)` with
        `movement.destination_aggregate.name` set to `target_aggregate` and
        `encryption.enabled: false` (mirrors `vol_decrypt.py` approach).
     b. On success: set `status: in_progress`.
     c. On `NetAppRestError`: set `status: failed`, record `error` message.
     d. Decrement available slots.
  7. Write updated YAML back to the same `--plan` path.
  8. Print summary: pending / started / in_progress / done / failed counts.

  Done when: `python vol_move_exec.py --help` exits 0; unit tests for
  `load_plan()`, `update_in_progress_statuses()`, and `start_pending_moves()`
  pass (mocked SDK); no ruff/mypy errors.

- [ ] **T3 — Add `pyyaml` to `requirements.txt`**
  Files: `requirements.txt`

  Done when: `python -c "import yaml"` succeeds in the project venv; `pyyaml`
  line is present and pinned in `requirements.txt`.

- [ ] **T4 — Extend `tests/smoke_test.py` with import + logic tests**
  Files: `tests/smoke_test.py`

  Tests to add:
  - `TestVolScheduleImports.test_vol_schedule_imports` — module imports cleanly.
  - `TestVolMoveExecImports.test_vol_move_exec_imports` — module imports cleanly.
  - `TestBuildPlanEntry` — `build_plan_entry()` returns correct dict shape.
  - `TestFormatVolumeTable` — `format_volume_table()` returns non-empty string.
  - `TestLoadPlan` — `load_plan()` with a valid YAML string returns expected
    structure; raises `ValueError` on missing keys.
  - `TestUpdateInProgressStatuses` — given a mocked `Volume.get_collection`
    returning no movement, status transitions from `in_progress` to `done`.
  - `TestStartPendingMoves` — given a mocked `Volume.patch`, a `pending` entry
    transitions to `in_progress`; a patched `NetAppRestError` transitions to
    `failed`.

  Done when: `python tests/smoke_test.py` runs all new tests green with no
  import errors.

## End-to-end verification

Hardware test cluster: `bahamas.muccbc.hq.netapp.com`, SVM: `azvsvmmgt002`.

```bash
# 1. Schedule against the hardware test cluster
python vol_schedule.py \
  --cluster bahamas.muccbc.hq.netapp.com \
  --username admin \
  --svm azvsvmmgt002 \
  --output /tmp/test_plan.yaml

# 2. Inspect the generated YAML and the per-run log in logs/
cat /tmp/test_plan.yaml
ls -lh logs/vol_schedule_*.log

# 3. Dry-run executor
python vol_move_exec.py \
  --plan /tmp/test_plan.yaml \
  --cluster bahamas.muccbc.hq.netapp.com \
  --username admin \
  --dry-run
ls -lh logs/vol_move_exec_*.log

# 4. Smoke tests
python tests/smoke_test.py
```

## Open questions

- **Unit test coverage** — `vol_schedule.py` and `vol_move_exec.py` currently have no dedicated
  unit tests beyond import smoke tests in `tests/smoke_test.py`. Coverage is below the 80% floor
  required by project standards. Tracked as a known gap; address in a follow-up task.
- **`verify_ssl=False` default** — intentionally off by default for lab/self-signed cert
  environments. Production callers should pass `--verify-ssl`. Documented inline in both scripts.
