#!/usr/bin/env python3
"""vol_schedule.py — Interactive ONTAP volume-move scheduler.

Discovers RW online volumes in a given SVM, lets the operator select which
volumes to schedule and which target aggregate each should move to, and
persists the plan as a YAML file for consumption by vol_move_exec.py.

Usage::

    python vol_schedule.py --cluster cluster1.example.com \\
        --username admin --svm vs_prod
    python vol_schedule.py --cluster 10.0.0.1 --username admin \\
        --svm vs_prod --output my_plan.yaml
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

SCRIPT_NAME = "vol_schedule"
ENV_PASSWORD_VAR = "ONTAP_PASSWORD"

# Volume movement states that count as "active" (skip during discovery)
ACTIVE_MOVE_STATES: frozenset[str] = frozenset(
    {"replicating", "cutover_wait", "cutover_pending", "queued"}
)

# Root-volume heuristics
ROOT_VOLUME_NAMES: frozenset[str] = frozenset({"vol0"})
ROOT_VOLUME_SUFFIX = "_root"

LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
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


def get_aggregates() -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    """Discover all data aggregates on the connected cluster.

    Returns:
        A tuple ``(aggr_list, node_map)`` where *aggr_list* is a list of
        aggregate info dicts (keys: ``name``, ``uuid``, ``node_name``,
        ``total``, ``used``, ``available``, ``usage_pct``, ``create_time``)
        and *node_map* maps each node name to its list of aggregate names.
        ``create_time`` is an ISO 8601 string from ONTAP or ``None`` when
        the field is not returned.
    """
    from netapp_ontap.resources import Aggregate  # type: ignore[import-untyped]

    aggr_list: list[dict[str, Any]] = []
    node_map: dict[str, list[str]] = {}
    fields = (
        "uuid,name,node.name,create_time,"
        "space.block_storage.size,space.block_storage.used,space.block_storage.available"
    )
    for aggr in Aggregate.get_collection(fields=fields):
        aggr.get(fields=fields)
        total: int = getattr(aggr.space.block_storage, "size", 0) or 0
        used: int = getattr(aggr.space.block_storage, "used", 0) or 0
        available: int = getattr(aggr.space.block_storage, "available", 0) or 0
        node_name: str = aggr.node.name
        info: dict[str, Any] = {
            "name": aggr.name,
            "uuid": aggr.uuid,
            "node_name": node_name,
            "total": total,
            "used": used,
            "available": available,
            "usage_pct": pct(used, total),
            "create_time": getattr(aggr, "create_time", None),
        }
        aggr_list.append(info)
        node_map.setdefault(node_name, []).append(aggr.name)
    return aggr_list, node_map


def get_in_flight_moves() -> list[dict[str, Any]]:
    """Return volumes currently undergoing a move on the connected cluster.

    Returns:
        List of in-flight move dicts (keys: ``name``, ``uuid``, ``state``).
    """
    from netapp_ontap.error import NetAppRestError  # type: ignore[import-untyped]
    from netapp_ontap.resources import Volume  # type: ignore[import-untyped]

    in_flight: list[dict[str, Any]] = []
    fields = "uuid,name,svm.name,movement.state"
    try:
        for vol in Volume.get_collection(fields=fields):
            vol.get(fields=fields)
            movement = getattr(vol, "movement", None)
            if movement is None:
                continue
            move_state = getattr(movement, "state", None)
            if move_state and move_state in ACTIVE_MOVE_STATES:
                in_flight.append({"name": vol.name, "uuid": vol.uuid, "state": move_state})
    except NetAppRestError as exc:
        logger.warning("Could not query in-flight moves: %s", exc)
    return in_flight


# ---------------------------------------------------------------------------
# Volume discovery
# ---------------------------------------------------------------------------


def get_volumes(svm: str) -> list[dict[str, Any]]:
    """Discover all eligible RW online volumes in *svm*.

    Skips:

    * Root volumes (name ends with ``_root`` or equals ``vol0``).
    * Volumes that are already in an active move state.
    * Volumes with no aggregate information.

    Args:
        svm: SVM name to scope the query.

    Returns:
        List of volume info dicts with keys: ``name``, ``uuid``, ``svm``,
        ``size``, ``space_used``, ``current_aggr``.
    """
    from netapp_ontap.resources import Volume  # type: ignore[import-untyped]

    volumes: list[dict[str, Any]] = []
    fields = "uuid,name,svm.name,size,space.used,aggregates.name,movement.state"
    query: dict[str, str] = {"type": "rw", "state": "online", "svm.name": svm}

    for vol in Volume.get_collection(fields=fields, **query):
        vol.get(fields=fields)

        # Skip root volumes
        if vol.name in ROOT_VOLUME_NAMES or vol.name.endswith(ROOT_VOLUME_SUFFIX):
            logger.debug("Skipping root volume: %s", vol.name)
            continue

        # Skip volumes already in an active move
        movement = getattr(vol, "movement", None)
        if movement:
            move_state = getattr(movement, "state", None)
            if move_state and move_state in ACTIVE_MOVE_STATES:
                logger.debug(
                    "Skipping volume %s — move already in progress (%s)",
                    vol.name,
                    move_state,
                )
                continue

        aggrs = vol.aggregates if vol.aggregates else []
        if not aggrs:
            logger.warning("Volume %s has no aggregate info, skipping.", vol.name)
            continue

        space_used: int = 0
        sp = getattr(vol, "space", None)
        if sp:
            space_used = getattr(sp, "used", 0) or 0

        volumes.append(
            {
                "name": vol.name,
                "uuid": vol.uuid,
                "svm": vol.svm.name if vol.svm else svm,
                "size": vol.size or 0,
                "space_used": space_used,
                "current_aggr": aggrs[0].name,
            }
        )

    return volumes


# ---------------------------------------------------------------------------
# Aggregate-scoped volume discovery
# ---------------------------------------------------------------------------


def get_volumes_on_aggregate(svm: str, aggr_name: str) -> list[dict[str, Any]]:
    """Discover eligible RW online volumes that reside on *aggr_name* in *svm*.

    Applies the same skip rules as :func:`get_volumes` (root volumes,
    already in-flight volumes, volumes with no aggregate info) and additionally
    scopes the ONTAP query to *aggr_name* via the ``aggregates.name`` filter.

    Args:
        svm: SVM name to scope the query.
        aggr_name: Name of the source aggregate to filter by.

    Returns:
        List of volume info dicts with keys: ``name``, ``uuid``, ``svm``,
        ``size``, ``space_used``, ``current_aggr``.
    """
    from netapp_ontap.resources import Volume  # type: ignore[import-untyped]

    volumes: list[dict[str, Any]] = []
    fields = "uuid,name,svm.name,size,space.used,aggregates.name,movement.state"
    query: dict[str, str] = {
        "type": "rw",
        "state": "online",
        "svm.name": svm,
        "aggregates.name": aggr_name,
    }

    for vol in Volume.get_collection(fields=fields, **query):
        vol.get(fields=fields)

        # Skip root volumes
        if vol.name in ROOT_VOLUME_NAMES or vol.name.endswith(ROOT_VOLUME_SUFFIX):
            logger.debug("Skipping root volume: %s", vol.name)
            continue

        # Skip volumes already in an active move
        movement = getattr(vol, "movement", None)
        if movement:
            move_state = getattr(movement, "state", None)
            if move_state and move_state in ACTIVE_MOVE_STATES:
                logger.debug(
                    "Skipping volume %s — move already in progress (%s)",
                    vol.name,
                    move_state,
                )
                continue

        aggrs = vol.aggregates if vol.aggregates else []
        if not aggrs:
            logger.warning("Volume %s has no aggregate info, skipping.", vol.name)
            continue

        space_used: int = 0
        sp = getattr(vol, "space", None)
        if sp:
            space_used = getattr(sp, "used", 0) or 0

        volumes.append(
            {
                "name": vol.name,
                "uuid": vol.uuid,
                "svm": vol.svm.name if vol.svm else svm,
                "size": vol.size or 0,
                "space_used": space_used,
                "current_aggr": aggrs[0].name,
            }
        )

    return volumes


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def format_volume_table(volumes: list[dict[str, Any]]) -> str:
    """Format a 1-based numbered table of volumes for interactive display.

    Args:
        volumes: List of volume info dicts as returned by :func:`get_volumes`.

    Returns:
        A multi-line string containing the formatted table.
    """
    header = (
        f"{'#':<4}{'Name':<25}{'SVM':<20}"
        f"{'Size(GiB)':>10}{'Used(GiB)':>11}  {'Current Aggr'}"
    )
    separator = "-" * 84
    lines = [header, separator]
    for i, vol in enumerate(volumes, start=1):
        lines.append(
            f"{i:<4}{str(vol['name']):<25}{str(vol['svm']):<20}"
            f"{bytes_to_gib(int(vol['size'])):>10.2f}"
            f"{bytes_to_gib(int(vol['space_used'])):>11.2f}"
            f"  {vol['current_aggr']}"
        )
    return "\n".join(lines)


def format_aggregate_table(
    aggr_list: list[dict[str, Any]],
    planned_bytes: dict[str, int] | None = None,
) -> str:
    """Format a 1-based numbered table of aggregates for interactive display.

    Sorts aggregates by ``create_time`` descending (newest first) before
    rendering so operators can easily identify recently-created aggregates.
    When *planned_bytes* is supplied, capacity figures are adjusted to reflect
    moves already committed in the current planning session.

    Args:
        aggr_list: List of aggregate info dicts as returned by
            :func:`get_aggregates`.  Each dict may contain an optional
            ``create_time`` key (ISO 8601 string or ``None``).
        planned_bytes: Optional mapping of aggregate name → bytes of volumes
            already planned to move to that aggregate this session.  When
            provided, ``Used``, ``Free``, and ``Usage%`` columns reflect the
            post-move state.  Aggregates with a non-zero planned amount are
            marked with ``*`` after the usage percentage.

    Returns:
        A multi-line string containing the formatted table.  A footnote line
        is appended when any aggregate has planned bytes.
    """
    pb: dict[str, int] = planned_bytes or {}
    # Sort newest-first; aggregates without create_time sort last.
    sorted_aggrs = sorted(
        aggr_list,
        key=lambda a: a.get("create_time") or "",
        reverse=True,
    )

    header = (
        f"{'#':<4}{'Aggregate':<25}{'Node':<20}"
        f"{'Total(GiB)':>11}{'Used(GiB)':>10}{'Free(GiB)':>10}{'Usage%':>9}  {'Created'}"
    )
    separator = "-" * 100
    lines = [header, separator]
    has_planned = False
    for i, aggr in enumerate(sorted_aggrs, start=1):
        name: str = str(aggr["name"])
        extra_bytes: int = pb.get(name, 0)
        eff_used: int = int(aggr["used"]) + extra_bytes
        eff_available: int = int(aggr["available"]) - extra_bytes
        eff_pct: float = pct(eff_used, int(aggr["total"]))
        if extra_bytes > 0:
            has_planned = True
        pct_str = f"{eff_pct:.1f}%{'*' if extra_bytes > 0 else ''}"
        raw_ct: str | None = aggr.get("create_time")
        created: str = raw_ct[:10] if raw_ct else "unknown"
        lines.append(
            f"{i:<4}{name:<25}{str(aggr['node_name']):<20}"
            f"{bytes_to_gib(int(aggr['total'])):>11.2f}"
            f"{bytes_to_gib(eff_used):>10.2f}"
            f"{bytes_to_gib(eff_available):>10.2f}"
            f"{pct_str:>9}  {created}"
        )
    if has_planned:
        lines.append("(* capacity adjusted for already-planned moves in this session)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Plan building
# ---------------------------------------------------------------------------


def build_plan_entry(vol: dict[str, Any], target_aggr: str) -> dict[str, Any]:
    """Build a single YAML plan entry for a volume move.

    Args:
        vol: Volume info dict as returned by :func:`get_volumes`.
        target_aggr: Name of the target aggregate.

    Returns:
        A plan entry dict with keys ``name``, ``uuid``, ``target_aggregate``,
        ``status`` (``"pending"``), and ``error`` (``None``).
    """
    return {
        "name": vol["name"],
        "uuid": vol["uuid"],
        "target_aggregate": target_aggr,
        "status": "pending",
        "error": None,
    }


# ---------------------------------------------------------------------------
# Interactive prompts  (print() is intentional UX — not library code)
# ---------------------------------------------------------------------------


def prompt_volume_selection(volumes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prompt the operator to select volumes from the numbered list.

    Accepts comma-separated 1-based numbers or the keyword ``all``.
    Re-prompts on invalid input.

    Args:
        volumes: The full list of discovered volumes.

    Returns:
        The subset of *volumes* chosen by the operator.
    """
    while True:
        raw = input(
            "\nEnter volume numbers to schedule (comma-separated, or 'all'): "
        ).strip()
        if raw.lower() == "all":
            return list(volumes)
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        try:
            indices = [int(p) for p in parts]
        except ValueError:
            print("Invalid input — enter numbers separated by commas, or 'all'.")
            continue
        if not indices:
            print("No volumes selected — try again.")
            continue
        if any(i < 1 or i > len(volumes) for i in indices):
            print(f"All numbers must be between 1 and {len(volumes)} — try again.")
            continue
        return [volumes[i - 1] for i in indices]


def prompt_aggregate_selection(
    vol: dict[str, Any],
    aggr_list: list[dict[str, Any]],
) -> str:
    """Prompt the operator to choose a target aggregate for one volume.

    The caller is responsible for passing a pre-filtered *aggr_list* that
    already excludes the volume's current aggregate.  Re-prompts on invalid
    input.

    Args:
        vol: Volume info dict (must contain ``name``).
        aggr_list: Candidate aggregates (current aggregate already excluded).

    Returns:
        The name of the chosen target aggregate.
    """
    vol_name: str = str(vol["name"])
    while True:
        raw = input(f"\nSelect target aggregate for {vol_name} (enter number): ").strip()
        try:
            idx = int(raw)
        except ValueError:
            print("Enter a number.")
            continue
        if idx < 1 or idx > len(aggr_list):
            print(f"Number must be between 1 and {len(aggr_list)} — try again.")
            continue
        selected = aggr_list[idx - 1]
        return str(selected["name"])


def prompt_pick_aggregate(
    aggr_list: list[dict[str, Any]],
    prompt_text: str,
) -> dict[str, Any]:
    """Prompt the operator to select one aggregate by 1-based number.

    The caller is responsible for printing the aggregate table before invoking
    this function.  Re-prompts on invalid input.

    Args:
        aggr_list: List of aggregate info dicts in the same display order as
            the table the caller already printed (typically pre-sorted by
            ``create_time`` descending).
        prompt_text: Prompt string shown to the operator (no leading newline
            required — one is added automatically).

    Returns:
        The selected aggregate info dict from *aggr_list*.
    """
    while True:
        raw = input(f"\n{prompt_text}").strip()
        try:
            idx = int(raw)
        except ValueError:
            print("Enter a number.")
            continue
        if idx < 1 or idx > len(aggr_list):
            print(f"Number must be between 1 and {len(aggr_list)} — try again.")
            continue
        return aggr_list[idx - 1]


def run_planning_loop(
    aggr_list: list[dict[str, Any]],
    svm: str,
) -> list[dict[str, Any]]:
    """Run the interactive aggregate-first batch planning loop.

    Each iteration of the outer loop constitutes one *batch*:

    1. Show all aggregates; operator picks a **source** aggregate to drain.
    2. Discover eligible volumes on that source aggregate.
    3. Operator selects volumes to move (comma-separated numbers or ``all``).
    4. Show target aggregates (source excluded) with capacity adjusted for
       in-session planned moves; operator picks a **target** aggregate.
    5. Record the batch (source volumes → target aggregate).
    6. Operator decides whether to plan another batch.

    Capacity guard: if the selected target would have negative effective free
    space after accounting for in-session planned bytes and the current
    selection, the operator is warned and asked to confirm or re-pick.

    Duplicate guard: volumes already planned in a previous batch (matched by
    UUID) are excluded from subsequent source-aggregate lists.

    Args:
        aggr_list: List of aggregate info dicts as returned by
            :func:`get_aggregates`.
        svm: SVM name used to scope volume discovery on each source aggregate.

    Returns:
        List of plan entry dicts (as produced by :func:`build_plan_entry`)
        for all selected volumes across all batches.
    """
    plan_entries: list[dict[str, Any]] = []
    planned_bytes: dict[str, int] = {}
    planned_uuids: set[str] = set()

    while True:
        # --- Step 2: Show all aggregates, sorted newest → oldest ---
        sorted_aggrs: list[dict[str, Any]] = sorted(
            aggr_list,
            key=lambda a: a.get("create_time") or "",
            reverse=True,
        )
        print("\nAll aggregates:")
        print(format_aggregate_table(sorted_aggrs))

        # --- Step 3: Pick source aggregate ---
        src_aggr = prompt_pick_aggregate(
            sorted_aggrs,
            "Select SOURCE aggregate to drain (enter number): ",
        )
        src_aggr_name: str = str(src_aggr["name"])

        # --- Step 4: Discover volumes on source aggregate ---
        logger.info("Discovering volumes on aggregate '%s'...", src_aggr_name)
        volumes = get_volumes_on_aggregate(svm, src_aggr_name)

        # Filter out volumes already committed in earlier batches
        volumes = [v for v in volumes if str(v["uuid"]) not in planned_uuids]

        if not volumes:
            print(f"\nNo eligible (unplanned) volumes found on aggregate '{src_aggr_name}'.")
            answer = input("Plan another batch? [y/N]: ").strip().lower()
            if answer != "y":
                break
            continue

        logger.info(
            "Found %d eligible volume(s) on aggregate '%s'.", len(volumes), src_aggr_name
        )

        # --- Step 5: Print volume table ---
        print(f"\nVolumes on {src_aggr_name}:")
        print(format_volume_table(volumes))

        # --- Step 6: Select volumes ---
        selected_volumes = prompt_volume_selection(volumes)
        logger.info(
            "Selected %d volume(s) from aggregate '%s'.", len(selected_volumes), src_aggr_name
        )

        # Total bytes being moved in this batch
        vol_bytes: int = sum(int(v["space_used"]) for v in selected_volumes)

        # --- Step 7: Print target aggregate table (exclude source) ---
        target_aggrs = [a for a in aggr_list if str(a["name"]) != src_aggr_name]
        sorted_targets: list[dict[str, Any]] = sorted(
            target_aggrs,
            key=lambda a: a.get("create_time") or "",
            reverse=True,
        )
        print("\nAvailable target aggregates (excluding source):")
        print(format_aggregate_table(sorted_targets, planned_bytes))

        # --- Step 8: Pick target aggregate (with capacity guard) ---
        target_name: str = ""
        while True:
            target_aggr = prompt_pick_aggregate(
                sorted_targets,
                "Select TARGET aggregate (enter number): ",
            )
            target_name = str(target_aggr["name"])

            extra: int = planned_bytes.get(target_name, 0)
            eff_available: int = int(target_aggr["available"]) - extra - vol_bytes
            if eff_available < 0:
                gib_needed: float = bytes_to_gib(vol_bytes)
                gib_avail: float = bytes_to_gib(int(target_aggr["available"]) - extra)
                print(
                    f"\nWARNING: '{target_name}' would have {gib_avail:.2f} GiB free "
                    f"but {gib_needed:.2f} GiB are needed."
                )
                confirm = input("Proceed anyway? [y/N]: ").strip().lower()
                if confirm == "y":
                    break
                print("\nRe-selecting target aggregate — updated capacity view:")
                print(format_aggregate_table(sorted_targets, planned_bytes))
            else:
                break

        # --- Step 9: Record the batch ---
        for vol in selected_volumes:
            vol_uuid: str = str(vol["uuid"])
            if vol_uuid in planned_uuids:
                logger.warning("Volume %s already planned — skipping.", vol["name"])
                continue
            entry = build_plan_entry(vol, target_name)
            plan_entries.append(entry)
            planned_uuids.add(vol_uuid)
            logger.info("Scheduled: %s -> %s", vol["name"], target_name)

        planned_bytes[target_name] = planned_bytes.get(target_name, 0) + vol_bytes

        # --- Step 10: Another batch? ---
        answer = input("\nPlan another batch? [y/N]: ").strip().lower()
        if answer != "y":
            break

    return plan_entries


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for vol_schedule.

    Args:
        argv: Optional argument list; uses ``sys.argv`` when ``None``.

    Returns:
        Parsed :class:`argparse.Namespace`.
    """
    p = argparse.ArgumentParser(
        description="Interactively schedule ONTAP volume moves and write a YAML plan.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s --cluster cluster1.example.com --username admin --svm vs_prod
  %(prog)s --cluster 10.0.0.1 --username admin --svm vs_prod --output my_plan.yaml
""",
    )
    p.add_argument("--cluster", required=True, help="Cluster management IP or hostname.")
    p.add_argument("--username", required=True, help="Admin username.")
    p.add_argument(
        "--password",
        default=None,
        help=(
            f"Admin password. Falls back to ${ENV_PASSWORD_VAR} env var, "
            "then interactive prompt."
        ),
    )
    p.add_argument("--svm", required=True, help="SVM name to scope volume discovery.")
    p.add_argument(
        "--output",
        default=None,
        help=(
            "Output YAML plan file path. "
            "Defaults to plans/<cluster_name>_<svm>.yaml (directory created if needed)."
        ),
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
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Cluster name
# ---------------------------------------------------------------------------


def get_cluster_short_name() -> str:
    """Return the ONTAP short cluster name from the active connection.

    Returns:
        str: The cluster name as reported by ONTAP (e.g. ``cluster1``).

    Raises:
        RuntimeError: If the cluster name is absent in the response.
    """
    from netapp_ontap.resources import Cluster  # type: ignore[import-untyped]

    cluster_info = Cluster()
    cluster_info.get()
    name: str | None = getattr(cluster_info, "name", None)
    if not name:
        raise RuntimeError("Cluster name missing in ONTAP response.")
    return name


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Entry point for vol_schedule.

    Args:
        argv: Optional argument list; uses ``sys.argv`` when ``None``.

    Returns:
        0 on success, 1 on unrecoverable error.
    """
    args = parse_args(argv)
    log_file = setup_logging(args.log_dir)

    logger.info("=" * 72)
    logger.info("vol_schedule started")
    logger.info("=" * 72)
    logger.info("Cluster:  %s", args.cluster)
    logger.info("SVM:      %s", args.svm)
    logger.info("Log file: %s", log_file)
    logger.info("-" * 72)

    password = _resolve_password(
        explicit=args.password,
        env_var=ENV_PASSWORD_VAR,
        prompt_label=f"Password for {args.username}@{args.cluster}",
    )
    connect(args.cluster, args.username, password, args.verify_ssl)

    # --- Resolve output path (auto-generate if not supplied) ---
    if args.output:
        output_path = Path(args.output)
    else:
        try:
            cluster_short = get_cluster_short_name()
        except RuntimeError as exc:
            logger.warning("Could not resolve cluster short name: %s — using 'cluster'.", exc)
            cluster_short = "cluster"
        svm_safe = args.svm.replace("/", "_")
        output_path = Path("plans") / f"{cluster_short}_{svm_safe}.yaml"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Output:   %s", output_path)

    # --- Discover aggregates ---
    logger.info("Discovering aggregates...")
    aggr_list, _ = get_aggregates()
    if not aggr_list:
        logger.info("No aggregates found on cluster. Nothing to schedule.")
        return 0
    logger.info("Found %d aggregate(s).", len(aggr_list))

    # --- Run interactive aggregate-first batch planning loop ---
    plan_entries = run_planning_loop(aggr_list, args.svm)
    if not plan_entries:
        logger.info("No volumes selected. Nothing to write.")
        return 0

    # --- Build plan dict ---
    plan: dict[str, Any] = {
        "cluster": args.cluster,
        "svm": args.svm,
        "status": "pending",
        "volumes": plan_entries,
    }

    # --- Confirm overwrite if output already exists ---
    if output_path.exists():
        answer = (
            input(f"\nFile {output_path} already exists. Overwrite? [y/N]: ")
            .strip()
            .lower()
        )
        if answer != "y":
            logger.info("Aborted — output file not overwritten.")
            return 0

    output_path.write_text(
        yaml.dump(plan, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )
    logger.info(
        "Plan written to %s — %d volumes scheduled.", output_path, len(plan_entries)
    )
    print(f"\nPlan written to {output_path} — {len(plan_entries)} volumes scheduled.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
