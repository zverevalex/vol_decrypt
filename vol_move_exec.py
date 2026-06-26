#!/usr/bin/env python3
"""vol_move_exec.py — ONTAP volume-move plan executor.

Reads a YAML plan file (or all YAML files in a directory) produced by
vol_schedule.py, checks available vol-move concurrency slots, starts pending
moves up to the slot limit, updates statuses for in-progress entries, and
writes the updated plan(s) back to disk.

Designed to be safe to run repeatedly as a cron job: already-completed or
failed entries are left as-is; only ``pending`` entries consume slots.

Single-plan usage::

    python vol_move_exec.py --plan plans/cluster1.yaml \\
        --cluster cluster1.example.com --username admin

Multi-cluster usage (reads all *.yaml files in ./plans/)::

    python vol_move_exec.py --plans-dir ./plans --username admin
"""

import argparse
import getpass
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any  # Any: ONTAP SDK responses and YAML payloads are schema-less dicts

import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIPT_NAME = "vol_move_exec"
ENV_PASSWORD_VAR = "ONTAP_PASSWORD"
DEFAULT_MAX_PER_NODE = 8

# Volume movement states that count as "still moving"
ACTIVE_MOVE_STATES: frozenset[str] = frozenset(
    {"replicating", "cutover_wait", "cutover_pending", "queued"}
)

# Required keys in the plan file and in each volume entry
REQUIRED_PLAN_KEYS: frozenset[str] = frozenset({"cluster", "svm", "volumes"})
REQUIRED_ENTRY_KEYS: frozenset[str] = frozenset(
    {"name", "uuid", "target_aggregate", "status"}
)

LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def bytes_to_gib(b: int) -> float:
    """Convert bytes to GiB rounded to 2 decimal places.

    Args:
        b: Size in bytes.

    Returns:
        Size in GiB, rounded to 2 decimal places.
    """
    return round(b / (1024**3), 2)


def pct(used: int, total: int) -> float:
    """Return usage as a percentage rounded to 1 decimal place.

    Args:
        used: Used amount (any unit, must match *total*).
        total: Total amount.

    Returns:
        Percentage rounded to 1 decimal place; 0.0 when *total* is zero.
    """
    if total == 0:
        return 0.0
    return round(used / total * 100, 1)


def setup_logging(log_dir: str | Path) -> str:
    """Create a per-run log file and configure the root logger.

    Attaches a DEBUG-level ``FileHandler`` (timestamped filename under
    *log_dir*) and an INFO-level ``StreamHandler`` to ``stdout``.

    Args:
        log_dir: Directory in which the log file is created.  Created
            automatically if it does not exist.

    Returns:
        Absolute path of the log file that was created.
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_path / f"{SCRIPT_NAME}_{timestamp}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    fh = logging.FileHandler(str(log_file))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    root.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    root.addHandler(ch)

    return str(log_file)


def _resolve_password(
    explicit: str | None,
    env_var: str,
    prompt_label: str,
) -> str:
    """Resolve a password from an explicit value, env var, or interactive prompt.

    Resolution order:

    1. *explicit* (``--password`` CLI flag), if provided and non-empty.
    2. The environment variable named *env_var*.
    3. An interactive :func:`getpass.getpass` prompt.

    Args:
        explicit: Explicit password value, or ``None``.
        env_var: Name of the environment variable to check.
        prompt_label: Human-readable label for the interactive prompt.

    Returns:
        The resolved password string.
    """
    if explicit:
        return explicit
    from_env = os.environ.get(env_var)
    if from_env:
        return from_env
    return getpass.getpass(prompt=f"{prompt_label}: ")


def connect(cluster: str, username: str, password: str, verify_ssl: bool) -> None:
    """Establish a global ONTAP HostConnection.

    Sets ``netapp_ontap.config.CONNECTION`` so all subsequent SDK calls use
    this cluster without needing an explicit context manager.

    Args:
        cluster: Cluster management IP or hostname.
        username: Admin username.
        password: Admin password.
        verify_ssl: Whether to verify TLS certificates.
    """
    import urllib3  # type: ignore[import-untyped]
    from netapp_ontap import HostConnection  # type: ignore[import-untyped]
    from netapp_ontap import config as ontap_config  # type: ignore[import-untyped]

    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    conn = HostConnection(cluster, username=username, password=password, verify=verify_ssl)
    ontap_config.CONNECTION = conn
    logger.info("Connected to cluster: %s (SSL verify: %s)", cluster, verify_ssl)


def get_aggregates() -> dict[str, Any]:
    """Return a dict of aggregate info keyed by aggregate name.

    Each value is a dict with keys ``name``, ``uuid``, ``node_name``,
    ``total``, ``used``, ``available``, ``usage_pct``.

    Returns:
        Aggregate map keyed by aggregate name.
    """
    from netapp_ontap.resources import Aggregate  # type: ignore[import-untyped]

    aggr_map: dict[str, Any] = {}
    fields = (
        "uuid,name,node.name,"
        "space.block_storage.size,"
        "space.block_storage.used,"
        "space.block_storage.available"
    )
    for aggr in Aggregate.get_collection(fields=fields):
        aggr.get(fields=fields)
        block = getattr(getattr(aggr, "space", None), "block_storage", None)
        total: int = getattr(block, "size", 0) or 0
        used: int = getattr(block, "used", 0) or 0
        available: int = getattr(block, "available", 0) or 0
        node_name: str = aggr.node.name if getattr(aggr, "node", None) else "unknown"
        aggr_map[aggr.name] = {
            "name": aggr.name,
            "uuid": aggr.uuid,
            "node_name": node_name,
            "total": total,
            "used": used,
            "available": available,
        }
    logger.info("Discovered %d aggregate(s) across cluster.", len(aggr_map))
    return aggr_map


def get_in_flight_moves() -> list[dict[str, Any]]:
    """Return volumes currently undergoing a move on the connected cluster.

    Fetches ``aggregates.name`` (the source aggregate) so callers can map
    each in-flight move to its source node via the aggregate map.

    Returns:
        List of in-flight move dicts with keys: ``name``, ``uuid``,
        ``svm``, ``state``, ``source_aggregate``.
    """
    from netapp_ontap.error import NetAppRestError  # type: ignore[import-untyped]
    from netapp_ontap.resources import Volume  # type: ignore[import-untyped]

    in_flight: list[dict[str, Any]] = []
    fields = "uuid,name,svm.name,movement.state,aggregates.name"
    try:
        for vol in Volume.get_collection(fields=fields):
            vol.get(fields=fields)
            movement = getattr(vol, "movement", None)
            if movement is None:
                continue
            move_state = getattr(movement, "state", None)
            if move_state and move_state in ACTIVE_MOVE_STATES:
                aggrs = getattr(vol, "aggregates", []) or []
                source_aggr = aggrs[0].name if aggrs else ""
                in_flight.append({
                    "name": vol.name,
                    "uuid": vol.uuid,
                    "svm": vol.svm.name if getattr(vol, "svm", None) else "",
                    "state": move_state,
                    "source_aggregate": source_aggr,
                })
    except NetAppRestError as exc:
        logger.warning("Could not query in-flight moves: %s", exc)
    return in_flight


def build_node_move_counts(
    in_flight: list[dict[str, Any]],
    aggr_map: dict[str, Any],
) -> dict[str, int]:
    """Count in-flight vol-move operations per source node.

    Args:
        in_flight: List of in-flight move dicts as returned by
            :func:`get_in_flight_moves`.
        aggr_map: Aggregate map as returned by :func:`get_aggregates`.

    Returns:
        Dict mapping node name → number of active moves originating from
        that node.
    """
    counts: dict[str, int] = {}
    for move in in_flight:
        src_aggr: str = str(move.get("source_aggregate", ""))
        node: str = aggr_map.get(src_aggr, {}).get("node_name", "unknown")
        counts[node] = counts.get(node, 0) + 1
    return counts


def get_volume_source_node(vol_uuid: str, aggr_map: dict[str, Any]) -> str | None:
    """Return the source node for a volume by querying its current aggregate.

    Args:
        vol_uuid: UUID of the volume to look up.
        aggr_map: Aggregate map as returned by :func:`get_aggregates`.

    Returns:
        Node name string, or ``None`` if the aggregate cannot be resolved.
    """
    from netapp_ontap.error import NetAppRestError  # type: ignore[import-untyped]
    from netapp_ontap.resources import Volume  # type: ignore[import-untyped]

    try:
        vol = Volume(uuid=vol_uuid)
        vol.get(fields="aggregates.name")
        aggrs = getattr(vol, "aggregates", []) or []
        if not aggrs:
            logger.warning("Volume %s has no aggregate info.", vol_uuid)
            return None
        src_aggr: str = aggrs[0].name
        node: str | None = aggr_map.get(src_aggr, {}).get("node_name")
        return node
    except NetAppRestError as exc:
        logger.warning("Could not query source aggregate for volume %s: %s", vol_uuid, exc)
        return None


# ---------------------------------------------------------------------------
# Plan I/O
# ---------------------------------------------------------------------------


def load_plan(plan_path: Path) -> dict[str, Any]:
    """Load and validate a YAML plan file.

    Validates that required top-level keys (``cluster``, ``svm``,
    ``volumes``) are present, and that every volume entry contains
    ``name``, ``uuid``, ``target_aggregate``, and ``status``.

    Args:
        plan_path: Path to the YAML plan file.

    Returns:
        The plan as a dict.

    Raises:
        FileNotFoundError: If *plan_path* does not exist.
        ValueError: If required top-level or per-entry keys are missing.
    """
    text = plan_path.read_text(encoding="utf-8")
    data: Any = yaml.safe_load(text)  # safe_load may return None or non-dict on bad input
    if not isinstance(data, dict):
        raise ValueError(
            f"Plan file must contain a YAML mapping, got {type(data).__name__!r}. "
            "Is the file empty or malformed?"
        )

    missing_top = REQUIRED_PLAN_KEYS - set(data.keys())
    if missing_top:
        raise ValueError(
            f"Plan file is missing required top-level key(s): {sorted(missing_top)}"
        )

    for i, entry in enumerate(data["volumes"]):
        missing_entry = REQUIRED_ENTRY_KEYS - set(entry.keys())
        if missing_entry:
            raise ValueError(
                f"Volume entry #{i} is missing required key(s): {sorted(missing_entry)}"
            )

    # Provide a default plan-level status for YAML files written before this
    # field was introduced (backward compatibility — not a required key).
    if "status" not in data:
        data["status"] = "pending"

    return data


def write_plan(plan: dict[str, Any], plan_path: Path) -> None:
    """Serialise *plan* to *plan_path* as YAML.

    Args:
        plan: The plan dict to write.
        plan_path: Destination file path (overwritten if it already exists).
    """
    plan_path.write_text(
        yaml.dump(plan, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )
    logger.debug("Plan written to %s.", plan_path)


# ---------------------------------------------------------------------------
# Status management
# ---------------------------------------------------------------------------


def update_in_progress_statuses(entries: list[dict[str, Any]]) -> None:
    """Refresh statuses for all ``in_progress`` entries.

    For each entry with ``status == "in_progress"``, fetches the current
    ``movement.state`` from ONTAP and applies the following transitions:

    * ``movement`` absent **or** state not in :data:`ACTIVE_MOVE_STATES`
      → ``"done"``
    * ``movement.state == "failed"`` → ``"failed"``
    * Otherwise (still actively moving) — no change.

    Entries are mutated in-place.

    Args:
        entries: List of volume plan entry dicts.
    """
    from netapp_ontap.error import NetAppRestError  # type: ignore[import-untyped]
    from netapp_ontap.resources import Volume  # type: ignore[import-untyped]

    for entry in entries:
        if entry["status"] != "in_progress":
            continue

        vol_name: str = str(entry["name"])
        vol_uuid: str = str(entry["uuid"])

        vol = Volume(uuid=vol_uuid)
        try:
            vol.get(fields="movement.state")
        except NetAppRestError as exc:
            logger.warning(
                "Could not check movement state for %s: %s", vol_name, exc
            )
            continue

        movement = getattr(vol, "movement", None)
        if movement is None:
            logger.info(
                "Volume %s: no movement attribute — transitioning to done.", vol_name
            )
            entry["status"] = "done"
            continue

        move_state = getattr(movement, "state", None)
        if move_state == "failed":
            logger.warning("Volume %s: move failed — transitioning to failed.", vol_name)
            entry["status"] = "failed"
        elif move_state not in ACTIVE_MOVE_STATES:
            logger.info(
                "Volume %s: move state is '%s' — transitioning to done.",
                vol_name,
                move_state,
            )
            entry["status"] = "done"
        else:
            logger.debug(
                "Volume %s: move still active (state: %s).", vol_name, move_state
            )


def start_pending_moves(
    entries: list[dict[str, Any]],
    svm: str,
    aggr_map: dict[str, Any],
    node_move_counts: dict[str, int],
    max_per_node: int,
    *,
    dry_run: bool = False,
) -> int:
    """Start volume moves for ``pending`` entries respecting per-node slot limits.

    For each ``pending`` entry (in list order):

    1. Resolve the volume's current source node via :func:`get_volume_source_node`.
    2. Check whether that node has a free slot (``node_move_counts[node] < max_per_node``).
    3. If yes: issue ``Volume.patch(poll=False)``, increment the node counter,
       set ``status = "in_progress"``.
    4. If no free slot on the source node: skip and log; continue to next entry.

    On :class:`NetAppRestError`: set ``status = "failed"``, record ``error``.
    In dry-run mode: log the intended action, skip the source-node slot check,
    and do not mutate statuses or call ``Volume.patch``.

    Args:
        entries: List of volume plan entry dicts (mutated in-place).
        svm: SVM name used in log messages.
        aggr_map: Aggregate map (node lookup) from :func:`get_aggregates`.
        node_move_counts: Mutable dict of current in-flight moves per node
            (updated in-place as new moves are started).
        max_per_node: Maximum concurrent vol-move operations per source node.
        dry_run: When ``True``, log intended actions without executing.

    Returns:
        Number of moves started (or would-be started in dry-run).
    """
    from netapp_ontap.error import NetAppRestError  # type: ignore[import-untyped]
    from netapp_ontap.resources import Volume  # type: ignore[import-untyped]

    started = 0

    for entry in entries:
        if entry["status"] != "pending":
            continue

        vol_name: str = str(entry["name"])
        vol_uuid: str = str(entry["uuid"])
        target_aggr: str = str(entry["target_aggregate"])

        if dry_run:
            logger.info(
                "[DRY-RUN] Would move %s (SVM: %s) -> %s",
                vol_name, svm, target_aggr,
            )
            started += 1
            continue

        # Resolve source node to check per-node slot availability
        source_node = get_volume_source_node(vol_uuid, aggr_map)
        if source_node is None:
            logger.warning(
                "Skipping %s — could not resolve source node.", vol_name
            )
            continue

        current_count = node_move_counts.get(source_node, 0)
        if current_count >= max_per_node:
            logger.info(
                "Skipping %s — source node %s has no free slots (%d/%d in use).",
                vol_name, source_node, current_count, max_per_node,
            )
            continue

        try:
            vol = Volume(uuid=vol_uuid)
            vol.movement = {"destination_aggregate": {"name": target_aggr}}
            vol.patch(poll=False)
            entry["status"] = "in_progress"
            node_move_counts[source_node] = current_count + 1
            started += 1
            logger.info(
                "Volume move started: %s (SVM: %s) -> %s  "
                "[node: %s  slots used: %d/%d]",
                vol_name, svm, target_aggr,
                source_node, node_move_counts[source_node], max_per_node,
            )
        except NetAppRestError as exc:
            entry["status"] = "failed"
            entry["error"] = str(exc)
            logger.error("Failed to start volume move for %s: %s", vol_name, exc)

    return started


# ---------------------------------------------------------------------------
# Plan-level status
# ---------------------------------------------------------------------------


def _compute_plan_status(entries: list[dict[str, Any]]) -> str:
    """Return the overall plan status based on volume entry statuses.

    Rules (evaluated in order):

    * ``"done"``        — all entries are ``"done"``.
    * ``"pending"``     — all entries are ``"pending"``.
    * ``"failed"``      — at least one entry is ``"failed"`` and none are
                          ``"pending"`` or ``"in_progress"``.
    * ``"in_progress"`` — any other combination.

    Args:
        entries: List of volume plan entry dicts, each containing a
            ``"status"`` key.

    Returns:
        One of ``"done"``, ``"pending"``, ``"failed"``, or ``"in_progress"``.
    """
    statuses: set[str] = {str(e.get("status", "pending")) for e in entries}
    if statuses == {"done"}:
        return "done"
    if statuses == {"pending"}:
        return "pending"
    if "pending" not in statuses and "in_progress" not in statuses:
        return "failed"
    return "in_progress"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for vol_move_exec.

    ``--plan`` (single file, requires ``--cluster``) and ``--plans-dir``
    (multi-cluster, cluster read from each YAML) are mutually exclusive.
    When neither is supplied, ``--plans-dir`` defaults to ``./plans``.

    Args:
        argv: Optional argument list; uses ``sys.argv`` when ``None``.

    Returns:
        Parsed :class:`argparse.Namespace`.

    Raises:
        SystemExit: On argument errors.
    """
    p = argparse.ArgumentParser(
        description="Execute an ONTAP volume-move plan produced by vol_schedule.py.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  # Single plan (cluster supplied on CLI)
  %(prog)s --plan plans/cluster1.yaml --cluster 10.0.0.1 --username admin
  %(prog)s --plan plans/cluster1.yaml --cluster 10.0.0.1 --username admin --dry-run

  # Multi-cluster (reads all *.yaml/*.yml from ./plans/ by default)
  %(prog)s --username admin
  %(prog)s --plans-dir ./plans --username admin --dry-run
""",
    )

    source_group = p.add_mutually_exclusive_group(required=False)
    source_group.add_argument(
        "--plan",
        default=None,
        help="Path to a single YAML plan file. Requires --cluster.",
    )
    source_group.add_argument(
        "--plans-dir",
        default=None,
        metavar="DIR",
        help=(
            "Directory containing *.yaml / *.yml plan files. "
            "The cluster is read from each file's 'cluster' field; "
            "--cluster is not used. Defaults to ./plans/ when neither "
            "--plan nor --plans-dir is specified."
        ),
    )

    p.add_argument(
        "--cluster",
        default=None,
        help="Cluster management IP or hostname. Required with --plan; ignored with --plans-dir.",
    )
    p.add_argument("--username", required=True, help="Admin username.")
    p.add_argument(
        "--password",
        default=None,
        help=(
            f"Admin password. Falls back to ${ENV_PASSWORD_VAR} env var, "
            "then interactive prompt."
        ),
    )
    p.add_argument(
        "--max-per-node",
        type=int,
        default=DEFAULT_MAX_PER_NODE,
        help=(
            f"Maximum concurrent vol-move operations per source node "
            f"(default: {DEFAULT_MAX_PER_NODE})."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Log planned moves without executing them.",
    )
    p.add_argument(
        "--verify-ssl",
        action="store_true",
        default=False,  # Disabled by default: lab/on-prem clusters typically use self-signed certs.
        help="Verify TLS certificates (default: disabled for self-signed lab clusters).",
    )
    p.add_argument(
        "--log-dir",
        default=str(Path(__file__).resolve().parent / "logs"),
        help="Directory for log files (default: ./logs/ next to this script).",
    )

    args = p.parse_args(argv)

    # Post-parse validation
    if args.plan is None and args.plans_dir is None:
        args.plans_dir = "plans"

    if args.plan is not None and not args.cluster:
        p.error("--cluster is required when using --plan.")

    return args


# ---------------------------------------------------------------------------
# Single-plan processor
# ---------------------------------------------------------------------------


def run_single_plan(
    plan_path: Path,
    cluster: str,
    username: str,
    password: str,
    *,
    max_per_node: int,
    dry_run: bool,
    verify_ssl: bool,
) -> dict[str, int]:
    """Load, process, and update a single YAML plan against one cluster.

    Per-node slot logic:

    1. Builds an aggregate → node map.
    2. Counts in-flight vol-moves per source node.
    3. For each ``pending`` volume, resolves its source node and only starts
       the move if that node has a free slot (``< max_per_node``).

    Args:
        plan_path: Path to the YAML plan file.
        cluster: Cluster management IP or hostname.
        username: Admin username.
        password: Admin password.
        max_per_node: Maximum concurrent vol-move operations per source node.
        dry_run: When ``True``, log actions without executing.
        verify_ssl: Whether to verify TLS certificates.

    Returns:
        Dict with keys ``pending``, ``started``, ``in_progress``, ``done``,
        ``failed`` reflecting the post-run state.
    """
    logger.info("-" * 72)
    logger.info("Processing plan: %s  (cluster: %s)", plan_path.name, cluster)
    logger.info("-" * 72)

    try:
        plan = load_plan(plan_path)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Cannot load plan %s: %s", plan_path, exc)
        return {"pending": 0, "started": 0, "in_progress": 0, "done": 0, "failed": 0}

    entries: list[dict[str, Any]] = plan["volumes"]
    svm: str = str(plan["svm"])
    logger.info("Plan loaded: %d volume(s), SVM: %s", len(entries), svm)

    connect(cluster, username, password, verify_ssl)

    # Build aggregate→node map (needed for per-node slot accounting)
    aggr_map = get_aggregates()

    logger.info("Refreshing in-progress entry statuses...")
    update_in_progress_statuses(entries)

    # Count in-flight moves per source node
    in_flight = get_in_flight_moves()
    node_move_counts = build_node_move_counts(in_flight, aggr_map)

    logger.info("In-flight moves cluster-wide: %d", len(in_flight))
    for node, count in sorted(node_move_counts.items()):
        logger.info(
            "  Node %-30s  in-flight: %d / %d slots used",
            node, count, max_per_node,
        )

    pending_count = sum(1 for e in entries if e.get("status") == "pending")
    if pending_count == 0:
        logger.info("No pending volumes in plan.")
    else:
        logger.info(
            "Attempting to start %d pending volume(s) (max %d per node)...",
            pending_count, max_per_node,
        )

    started = start_pending_moves(
        entries,
        svm=svm,
        aggr_map=aggr_map,
        node_move_counts=node_move_counts,
        max_per_node=max_per_node,
        dry_run=dry_run,
    )

    # Compute and persist the plan-level status before writing.
    plan["status"] = _compute_plan_status(entries)
    logger.info("Plan %s status: %s", plan_path.name, plan["status"])

    if not dry_run:
        write_plan(plan, plan_path)

    counts: dict[str, int] = {"pending": 0, "in_progress": 0, "done": 0, "failed": 0}
    for entry in entries:
        status: str = str(entry.get("status", "pending"))
        if status in counts:
            counts[status] += 1

    logger.info(
        "Plan %s [status: %s] — pending: %d, started: %d, in_progress: %d, done: %d, failed: %d",
        plan_path.name,
        plan["status"],
        counts["pending"],
        started,
        counts["in_progress"],
        counts["done"],
        counts["failed"],
    )
    return {**counts, "started": started}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Entry point for vol_move_exec.

    Args:
        argv: Optional argument list; uses ``sys.argv`` when ``None``.

    Returns:
        0 on success, 1 on unrecoverable error.
    """
    args = parse_args(argv)
    log_file = setup_logging(args.log_dir)

    logger.info("=" * 72)
    logger.info("vol_move_exec started")
    logger.info("=" * 72)
    logger.info("Max per node:   %d", args.max_per_node)
    logger.info("Dry-run:        %s", args.dry_run)
    logger.info("SSL verify:     %s", args.verify_ssl)
    logger.info("Log file:       %s", log_file)

    password = _resolve_password(
        explicit=args.password,
        env_var=ENV_PASSWORD_VAR,
        prompt_label=f"Password for {args.username}",
    )

    # --- Collect plan files ---
    if args.plan:
        plan_files = [Path(args.plan)]
    else:
        plans_dir = Path(args.plans_dir)
        if not plans_dir.is_dir():
            logger.error("--plans-dir '%s' is not a directory.", plans_dir)
            return 1
        plan_files = sorted(
            [*plans_dir.glob("*.yaml"), *plans_dir.glob("*.yml")]
        )
        if not plan_files:
            logger.warning("No *.yaml / *.yml files found in '%s'.", plans_dir)
            return 0
        logger.info("Found %d plan file(s) in '%s'.", len(plan_files), plans_dir)

    # --- Process each plan ---
    totals: dict[str, int] = {
        "pending": 0, "started": 0, "in_progress": 0, "done": 0, "failed": 0,
    }
    errors = 0

    for plan_path in plan_files:
        # Determine cluster: from CLI (--plan mode) or from the YAML (--plans-dir mode)
        if args.plan:
            cluster = args.cluster
        else:
            try:
                raw = load_plan(plan_path)
                cluster = str(raw["cluster"])
            except (FileNotFoundError, ValueError) as exc:
                logger.error("Skipping %s — cannot load: %s", plan_path.name, exc)
                errors += 1
                continue

        result = run_single_plan(
            plan_path,
            cluster=cluster,
            username=args.username,
            password=password,
            max_per_node=args.max_per_node,
            dry_run=args.dry_run,
            verify_ssl=args.verify_ssl,
        )
        for key in totals:
            totals[key] += result.get(key, 0)

    # --- Combined summary (only meaningful for multi-plan runs) ---
    if len(plan_files) > 1:
        logger.info("=" * 72)
        logger.info(
            "COMBINED SUMMARY (%d plan(s)) — "
            "pending: %d, started: %d, in_progress: %d, done: %d, failed: %d",
            len(plan_files),
            totals["pending"],
            totals["started"],
            totals["in_progress"],
            totals["done"],
            totals["failed"],
        )

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
