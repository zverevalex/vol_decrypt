#!/usr/bin/env python3
"""
vol_decrypt.py — NetApp ONTAP Volume Decryption via Volume Move

Discovers NVE-encrypted volumes on a NetApp ONTAP cluster and initiates
non-disruptive volume move operations with encryption disabled, effectively
decrypting the volumes. Designed to run as a recurring cron job.
"""

import argparse
import logging
import os
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_NAME = "vol_decrypt"
DEFAULT_MAX_CONCURRENT = 6
DEFAULT_CAPACITY_THRESHOLD = 70  # percent
ENV_PASSWORD_VAR = "ONTAP_PASSWORD"

# Volume movement states that count as "in-flight"
ACTIVE_MOVE_STATES = {"replicating", "cutover_wait", "cutover_pending", "queued"}

LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def bytes_to_gib(b):
    """Convert bytes to GiB with 2 decimal places."""
    return round(b / (1024**3), 2)


def pct(used, total):
    """Return percentage as float rounded to 1 decimal."""
    if total == 0:
        return 0.0
    return round(used / total * 100, 1)


def setup_logging(log_dir):
    """Create a per-run log file and configure root logger."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{SCRIPT_NAME}_{timestamp}.log")

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # File handler — DEBUG level
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    root.addHandler(fh)

    # Console handler — INFO level
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    root.addHandler(ch)

    return log_file


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Decrypt NetApp ONTAP NVE-encrypted volumes via volume move.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s --cluster 10.0.0.1 --username admin --password secret
  %(prog)s --cluster cluster1.example.com --username admin --dry-run
  %(prog)s --cluster 10.0.0.1 --username admin --svm vs1 --max-concurrent 4
  %(prog)s --cluster 10.0.0.1 --username admin --exclude-volume root_vol
""",
    )
    p.add_argument(
        "--cluster", required=True, help="Cluster management IP or hostname."
    )
    p.add_argument("--username", required=True, help="Admin username.")
    p.add_argument(
        "--password",
        default=None,
        help=f"Admin password. If omitted, read from ${ENV_PASSWORD_VAR} env var.",
    )
    p.add_argument(
        "--max-concurrent",
        type=int,
        default=DEFAULT_MAX_CONCURRENT,
        help=f"Max concurrent volume move operations (default: {DEFAULT_MAX_CONCURRENT}).",
    )
    p.add_argument(
        "--capacity-threshold",
        type=int,
        default=DEFAULT_CAPACITY_THRESHOLD,
        help=f"Max aggregate usage %% after move (default: {DEFAULT_CAPACITY_THRESHOLD}).",
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Log planned moves without executing."
    )
    p.add_argument(
        "--verify-ssl",
        action="store_true",
        default=False,
        help="Verify SSL certificates (default: disabled).",
    )
    p.add_argument(
        "--svm",
        default=None,
        help="Scope to a single SVM (default: all SVMs).",
    )
    p.add_argument(
        "--exclude-volume",
        action="append",
        default=[],
        metavar="VOL",
        help="Volume name(s) to skip. Can be repeated.",
    )
    p.add_argument(
        "--log-dir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs"),
        help="Directory for log files (default: ./logs/).",
    )
    args = p.parse_args(argv)

    # Resolve password
    if args.password is None:
        args.password = os.environ.get(ENV_PASSWORD_VAR)
        if not args.password:
            p.error(
                f"--password not provided and ${ENV_PASSWORD_VAR} environment variable is not set."
            )
    return args


# ---------------------------------------------------------------------------
# ONTAP interaction
# ---------------------------------------------------------------------------


def connect(cluster, username, password, verify_ssl):
    """Establish a global HostConnection."""
    import urllib3
    from netapp_ontap import HostConnection
    from netapp_ontap import config as ontap_config

    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    conn = HostConnection(
        cluster, username=username, password=password, verify=verify_ssl
    )
    ontap_config.CONNECTION = conn
    logging.info("Connected to cluster: %s (SSL verify: %s)", cluster, verify_ssl)


def get_aggregates():
    """Return a dict of aggregate info keyed by aggregate name.

    Each entry: {name, uuid, node_name, total, used, available, usage_pct}
    Also returns a node_map: {node_name: [aggr_name, ...]}
    """
    aggr_map = {}
    node_map = {}

    from netapp_ontap.resources import Aggregate

    fields = "uuid,name,node.name,space.block_storage.size,space.block_storage.used,space.block_storage.available"
    for aggr in Aggregate.get_collection(fields=fields):
        aggr.get(fields=fields)
        total = aggr.space.block_storage.size or 0
        used = aggr.space.block_storage.used or 0
        available = aggr.space.block_storage.available or 0
        node_name = aggr.node.name

        info = {
            "name": aggr.name,
            "uuid": aggr.uuid,
            "node_name": node_name,
            "total": total,
            "used": used,
            "available": available,
            "usage_pct": pct(used, total),
        }
        aggr_map[aggr.name] = info
        node_map.setdefault(node_name, []).append(aggr.name)

    return aggr_map, node_map


def get_in_flight_moves():
    """Return list of volumes currently undergoing a move."""
    from netapp_ontap.error import NetAppRestError
    from netapp_ontap.resources import Volume

    in_flight = []
    fields = "uuid,name,svm.name,movement.state,movement.percent_complete,movement.destination_aggregate"
    try:
        for vol in Volume.get_collection(fields=fields):
            vol.get(fields=fields)
            state = getattr(vol, "movement", None)
            if state is None:
                continue
            move_state = getattr(state, "state", None)
            if move_state and move_state in ACTIVE_MOVE_STATES:
                dest_aggr_name = ""
                dest = getattr(state, "destination_aggregate", None)
                if dest:
                    dest_aggr_name = getattr(dest, "name", "")
                in_flight.append(
                    {
                        "name": vol.name,
                        "uuid": vol.uuid,
                        "svm": vol.svm.name if vol.svm else "",
                        "state": move_state,
                        "percent_complete": getattr(state, "percent_complete", None),
                        "destination_aggregate": dest_aggr_name,
                    }
                )
    except NetAppRestError as exc:
        logging.warning("Could not query in-flight moves: %s", exc)
    return in_flight


def get_encrypted_volumes(svm_filter=None, exclude_volumes=None):
    """Return list of NVE-encrypted volumes eligible for decryption."""
    from netapp_ontap.resources import Volume

    exclude_set = set(exclude_volumes) if exclude_volumes else set()
    volumes = []

    fields = (
        "uuid,name,svm.name,size,style,"
        "encryption.enabled,encryption.type,encryption.state,"
        "aggregates.name,aggregates.uuid,"
        "movement.state,space.used"
    )
    query = {
        "encryption.enabled": "true",
        "encryption.type": "volume",
        "type": "rw",
    }
    if svm_filter:
        query["svm.name"] = svm_filter

    for vol in Volume.get_collection(fields=fields, **query):
        vol.get(fields=fields)

        # Skip excluded volumes
        if vol.name in exclude_set:
            logging.debug("Skipping excluded volume: %s", vol.name)
            continue

        # Skip volumes already in a move
        move = getattr(vol, "movement", None)
        if move:
            move_state = getattr(move, "state", None)
            if move_state and move_state in ACTIVE_MOVE_STATES:
                logging.debug(
                    "Skipping volume %s — move already in progress (%s)",
                    vol.name,
                    move_state,
                )
                continue

        # Determine current aggregate and node
        aggrs = vol.aggregates if vol.aggregates else []
        if not aggrs:
            logging.warning("Volume %s has no aggregate info, skipping.", vol.name)
            continue

        current_aggr = aggrs[0]
        current_aggr_name = current_aggr.name
        # Node name is not available on the volumes endpoint;
        # it will be resolved from the aggr_map at aggregate-selection time.
        current_node_name = None

        space_used = 0
        sp = getattr(vol, "space", None)
        if sp:
            space_used = getattr(sp, "used", 0) or 0

        volumes.append(
            {
                "name": vol.name,
                "uuid": vol.uuid,
                "svm": vol.svm.name if vol.svm else "",
                "size": vol.size or 0,
                "space_used": space_used,
                "current_aggr": current_aggr_name,
                "current_node": current_node_name,
                "style": vol.style,
            }
        )

    return volumes


# ---------------------------------------------------------------------------
# Aggregate selection
# ---------------------------------------------------------------------------


def select_target_aggregate(vol_info, aggr_map, node_map, capacity_threshold):
    """Pick the best aggregate for decrypting a volume.

    Tier 1: same-node aggregate with the most free space that stays under threshold.
    Tier 2: cross-node aggregate (fallback).

    Returns (aggr_name, projected_pct, is_cross_node) or (None, None, None).
    """
    vol_space = vol_info["space_used"]
    current_aggr = vol_info["current_aggr"]
    current_node = vol_info["current_node"]

    def evaluate_candidates(candidate_aggr_names):
        """Evaluate a list of aggregate names. Return best (name, projected_pct) or None."""
        best = None
        for aname in candidate_aggr_names:
            # ONTAP rejects vol move to the same aggregate
            if aname == current_aggr:
                continue
            ainfo = aggr_map.get(aname)
            if ainfo is None:
                continue
            total = ainfo["total"]
            if total == 0:
                continue

            projected_used = ainfo["used"] + vol_space

            projected_pct = pct(projected_used, total)
            if projected_pct > capacity_threshold:
                continue

            # Pick the aggregate with the most available space
            if best is None or ainfo["available"] > aggr_map[best[0]]["available"]:
                best = (aname, projected_pct)
        return best

    # Tier 1: same-node
    if current_node and current_node in node_map:
        result = evaluate_candidates(node_map[current_node])
        if result:
            return result[0], result[1], False

    # Tier 2: cross-node
    other_aggrs = []
    for node, aggr_names in node_map.items():
        if node != current_node:
            other_aggrs.extend(aggr_names)
    if other_aggrs:
        result = evaluate_candidates(other_aggrs)
        if result:
            return result[0], result[1], True

    return None, None, None


# ---------------------------------------------------------------------------
# Volume move
# ---------------------------------------------------------------------------


def start_volume_move(vol_info, target_aggr, dry_run):
    """Issue PATCH to start a volume move with encryption disabled.

    Returns True on success, False on failure.
    """
    vol_name = vol_info["name"]
    vol_uuid = vol_info["uuid"]
    svm_name = vol_info["svm"]

    if dry_run:
        logging.info(
            "[DRY-RUN] Would move volume %s (SVM: %s) to aggregate %s with encryption disabled.",
            vol_name,
            svm_name,
            target_aggr,
        )
        return True

    from netapp_ontap.error import NetAppRestError
    from netapp_ontap.resources import Volume

    try:
        vol = Volume(uuid=vol_uuid)
        vol.movement = {"destination_aggregate": {"name": target_aggr}}
        vol.encryption = {"enabled": False}
        vol.patch(poll=False)
        logging.info(
            "Volume move started: %s (SVM: %s) -> aggregate %s (encrypt-destination: false)",
            vol_name,
            svm_name,
            target_aggr,
        )
        return True
    except NetAppRestError as exc:
        logging.error("Failed to start volume move for %s: %s", vol_name, exc)
        return False


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def run(args):
    log_file = setup_logging(args.log_dir)
    log = logging.getLogger()

    log.info("=" * 72)
    log.info("vol_decrypt run started")
    log.info("=" * 72)
    log.info("Cluster:            %s", args.cluster)
    log.info("Username:           %s", args.username)
    log.info("Max concurrent:     %d", args.max_concurrent)
    log.info("Capacity threshold: %d%%", args.capacity_threshold)
    log.info("Dry-run:            %s", args.dry_run)
    log.info("SSL verify:         %s", args.verify_ssl)
    log.info("SVM filter:         %s", args.svm or "(all)")
    log.info("Excluded volumes:   %s", args.exclude_volume or "(none)")
    log.info("Log file:           %s", log_file)
    log.info("-" * 72)

    # --- Connect ---
    connect(args.cluster, args.username, args.password, args.verify_ssl)

    # --- Report in-flight moves (audit trail) ---
    log.info("Checking in-flight volume moves from previous runs...")
    in_flight = get_in_flight_moves()
    if in_flight:
        log.info("In-flight volume moves: %d", len(in_flight))
        for m in in_flight:
            pct_str = (
                f"{m['percent_complete']}%"
                if m["percent_complete"] is not None
                else "N/A"
            )
            log.info(
                "  %-30s  SVM: %-15s  State: %-15s  Progress: %s  Dest: %s",
                m["name"],
                m["svm"],
                m["state"],
                pct_str,
                m["destination_aggregate"],
            )
    else:
        log.info("No in-flight volume moves detected.")

    available_slots = args.max_concurrent - len(in_flight)
    if available_slots <= 0:
        log.warning(
            "Max concurrent limit reached (%d/%d in-flight). No new moves will be started.",
            len(in_flight),
            args.max_concurrent,
        )
        log.info("Run complete. No new moves started.")
        return

    log.info(
        "Available move slots: %d (max %d, in-flight %d)",
        available_slots,
        args.max_concurrent,
        len(in_flight),
    )
    log.info("-" * 72)

    # --- Discover aggregates ---
    log.info("Discovering aggregates...")
    aggr_map, node_map = get_aggregates()
    log.info("Found %d data aggregates across %d nodes.", len(aggr_map), len(node_map))
    for aname, ainfo in sorted(aggr_map.items()):
        log.info(
            "  %-30s  Node: %-20s  Total: %8.2f GiB  Used: %8.2f GiB  Avail: %8.2f GiB  Usage: %5.1f%%",
            aname,
            ainfo["node_name"],
            bytes_to_gib(ainfo["total"]),
            bytes_to_gib(ainfo["used"]),
            bytes_to_gib(ainfo["available"]),
            ainfo["usage_pct"],
        )
    log.info("-" * 72)

    # --- Discover encrypted volumes ---
    log.info("Discovering NVE-encrypted volumes...")
    encrypted_vols = get_encrypted_volumes(
        svm_filter=args.svm, exclude_volumes=args.exclude_volume
    )

    # Resolve node names from aggr_map (not available on volumes endpoint)
    for v in encrypted_vols:
        ainfo = aggr_map.get(v["current_aggr"])
        if ainfo:
            v["current_node"] = ainfo["node_name"]
    if not encrypted_vols:
        log.info("No eligible encrypted volumes found. Nothing to do.")
        log.info("Run complete.")
        return

    log.info(
        "Found %d encrypted volume(s) eligible for decryption:", len(encrypted_vols)
    )
    for v in encrypted_vols:
        log.info(
            "  %-30s  SVM: %-15s  Size: %8.2f GiB  Used: %8.2f GiB  Aggr: %-25s  Node: %s",
            v["name"],
            v["svm"],
            bytes_to_gib(v["size"]),
            bytes_to_gib(v["space_used"]),
            v["current_aggr"],
            v["current_node"],
        )
    log.info("-" * 72)

    # --- Select targets and execute moves ---
    moves_started = 0
    moves_skipped_capacity = 0
    moves_skipped_error = 0

    for v in encrypted_vols:
        if moves_started >= available_slots:
            log.info(
                "Concurrent move limit reached (%d). Remaining volumes deferred to next run.",
                available_slots,
            )
            break

        target_aggr, projected_pct, is_cross_node = select_target_aggregate(
            v, aggr_map, node_map, args.capacity_threshold
        )

        if target_aggr is None:
            log.warning(
                "SKIPPED %s (SVM: %s): no aggregate found with projected usage <= %d%%. "
                "Current aggr: %s, volume used: %.2f GiB.",
                v["name"],
                v["svm"],
                args.capacity_threshold,
                v["current_aggr"],
                bytes_to_gib(v["space_used"]),
            )
            moves_skipped_capacity += 1
            continue

        move_type = "CROSS-NODE" if is_cross_node else "SAME-NODE"
        log.info(
            "Planning move for %s (SVM: %s): %s -> %s [%s]  Projected usage: %.1f%%",
            v["name"],
            v["svm"],
            v["current_aggr"],
            target_aggr,
            move_type,
            projected_pct,
        )

        success = start_volume_move(v, target_aggr, args.dry_run)
        if success:
            moves_started += 1
            # Update aggregate used space in our local map to account for the planned move
            if target_aggr != v["current_aggr"]:
                aggr_map[target_aggr]["used"] += v["space_used"]
                aggr_map[target_aggr]["available"] -= v["space_used"]
                aggr_map[target_aggr]["usage_pct"] = pct(
                    aggr_map[target_aggr]["used"], aggr_map[target_aggr]["total"]
                )
        else:
            moves_skipped_error += 1

    # --- Summary ---
    log.info("=" * 72)
    log.info("Run summary")
    log.info("=" * 72)
    log.info("Encrypted volumes discovered:  %d", len(encrypted_vols))
    log.info("In-flight moves (prior runs):  %d", len(in_flight))
    log.info(
        "Moves started this run:        %d%s",
        moves_started,
        " (dry-run)" if args.dry_run else "",
    )
    log.info("Skipped (capacity threshold):  %d", moves_skipped_capacity)
    log.info("Skipped (API error):           %d", moves_skipped_error)
    remaining = (
        len(encrypted_vols)
        - moves_started
        - moves_skipped_capacity
        - moves_skipped_error
    )
    if remaining > 0:
        log.info("Deferred to next run (limit):  %d", remaining)
    log.info("Log file: %s", log_file)
    log.info("Run complete.")


def main():
    args = parse_args()
    try:
        from netapp_ontap.error import NetAppRestError
    except ImportError:
        logging.error(
            "The netapp-ontap library is not installed. Run: pip install netapp-ontap"
        )
        sys.exit(1)
    try:
        run(args)
    except NetAppRestError as exc:
        logging.error("ONTAP API error: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
        sys.exit(130)


if __name__ == "__main__":
    main()
