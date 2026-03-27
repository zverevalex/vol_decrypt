#!/usr/bin/env python3
"""
snapmirror.py — NetApp ONTAP SnapMirror Replication

Replicates volumes from a source cluster/SVM to a destination cluster/SVM
using SnapMirror relationships. Destination volumes are created automatically
via the create_destination REST API parameter.

Usage example:
    python snapmirror.py \
        --source-cluster 10.0.0.1 \
        --source-username admin \
        --destination-cluster 10.0.0.2 \
        --destination-username admin \
        --source-svm vs_prod \
        --exclude-volumes vol_temp vol_scratch
"""

import argparse
import getpass
import logging
import os
import sys
from typing import NamedTuple, Optional

import urllib3
from netapp_ontap import HostConnection
from netapp_ontap.resources import (
    Cluster,
    SnapmirrorRelationship,
    Svm,
    SvmPeer,
    Volume,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ENV_SRC_PASSWORD_VAR = "ONTAP_SRC_PASSWORD"
ENV_DST_PASSWORD_VAR = "ONTAP_DST_PASSWORD"
DEFAULT_POLICY = "MirrorAllSnapshots"
DST_SVM_SUFFIX = "_dst"

LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# ---------------------------------------------------------------------------
# Type definitions
# ---------------------------------------------------------------------------


class VolumeInfo(NamedTuple):
    """Volume information from the source cluster.

    Attributes:
        name: Volume name.
        uuid: Volume UUID.
        svm_name: Name of the source SVM containing the volume.
    """

    name: str
    uuid: str
    svm_name: str


class ReplicationContext(NamedTuple):
    """Context for SnapMirror replication operations.

    Attributes:
        src_cluster_name: Name of the source cluster.
        src_svm_name: Name of the source SVM.
        dst_svm_name: Name of the destination SVM.
        dst_connection: HostConnection to the destination cluster.
    """

    src_cluster_name: str
    src_svm_name: str
    dst_svm_name: str
    dst_connection: HostConnection


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logging() -> None:
    """Configure root logger with console output.

    Returns:
        None
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    root.addHandler(handler)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse and return command-line arguments.

    Args:
        argv: Optional list of arguments to parse. If None, sys.argv is used.

    Returns:
        argparse.Namespace: Parsed arguments with resolved passwords and
            default destination SVM name.
    """
    parser = argparse.ArgumentParser(
        description="Replicate volumes via SnapMirror from source to destination cluster.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s --source-cluster 10.0.0.1 --source-username admin \\
           --destination-cluster 10.0.0.2 --destination-username admin \\
           --source-svm vs_prod

  %(prog)s --source-cluster src.example.com --source-username admin \\
           --destination-cluster dst.example.com --destination-username admin \\
           --source-svm vs_prod --destination-svm vs_prod_dr \\
           --exclude-volumes temp_vol scratch_vol
""",
    )

    # Source parameters
    parser.add_argument(
        "--source-cluster",
        required=True,
        help="Source cluster management IP or hostname.",
    )
    parser.add_argument(
        "--source-username",
        required=True,
        help="Admin username for the source cluster.",
    )
    parser.add_argument(
        "--source-password",
        default=None,
        help=(
            f"Admin password for the source cluster. "
            f"If omitted, read from ${ENV_SRC_PASSWORD_VAR} or prompted interactively."
        ),
    )

    # Destination parameters
    parser.add_argument(
        "--destination-cluster",
        required=True,
        help="Destination cluster management IP or hostname.",
    )
    parser.add_argument(
        "--destination-username",
        required=True,
        help="Admin username for the destination cluster.",
    )
    parser.add_argument(
        "--destination-password",
        default=None,
        help=(
            f"Admin password for the destination cluster. "
            f"If omitted, read from ${ENV_DST_PASSWORD_VAR} or prompted interactively."
        ),
    )

    # SVM parameters
    parser.add_argument(
        "--source-svm",
        required=True,
        help="Name of the source SVM containing the volumes to replicate.",
    )
    parser.add_argument(
        "--destination-svm",
        default=None,
        help=(
            "Name of the destination SVM. "
            "If omitted, defaults to <source-svm>" + DST_SVM_SUFFIX + "."
        ),
    )

    # Volume filtering
    parser.add_argument(
        "--exclude-volumes",
        nargs="*",
        default=[],
        metavar="VOL",
        help="Volume name(s) to exclude from replication.",
    )

    args = parser.parse_args(argv)

    # Resolve passwords via env vars or interactive prompt
    args.source_password = _resolve_password(
        explicit=args.source_password,
        env_var=ENV_SRC_PASSWORD_VAR,
        prompt_label=f"Password for {args.source_username}@{args.source_cluster}",
    )
    args.destination_password = _resolve_password(
        explicit=args.destination_password,
        env_var=ENV_DST_PASSWORD_VAR,
        prompt_label=f"Password for {args.destination_username}@{args.destination_cluster}",
    )

    # Default destination SVM name
    if args.destination_svm is None:
        args.destination_svm = f"{args.source_svm}{DST_SVM_SUFFIX}"

    return args


def _resolve_password(
    explicit: Optional[str],
    env_var: str,
    prompt_label: str,
) -> str:
    """Return a password from explicit value, environment variable, or interactive prompt.

    Args:
        explicit: Explicit password value, or None.
        env_var: Environment variable name to check for password.
        prompt_label: Label to display in interactive password prompt.

    Returns:
        str: The resolved password.
    """
    if explicit:
        return explicit
    from_env = os.environ.get(env_var)
    if from_env:
        return from_env
    return getpass.getpass(prompt=f"{prompt_label}: ")


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def create_connection(
    cluster: str,
    username: str,
    password: str,
    *,
    verify_ssl: bool = False,
) -> HostConnection:
    """Create and return a HostConnection for the given cluster.

    Args:
        cluster: Cluster management IP or hostname.
        username: Admin username.
        password: Admin password.
        verify_ssl: Whether to verify SSL certificates. Defaults to False.

    Returns:
        HostConnection: Active connection to the cluster.
    """

    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    conn = HostConnection(
        cluster, username=username, password=password, verify=verify_ssl
    )
    logging.info("Connected to cluster: %s", cluster)
    return conn


# noinspection SpellCheckingInspection
def get_cluster_name(connection: HostConnection) -> str:
    """Return the ONTAP cluster name for the given connection.

    Args:
        connection: HostConnection object to query.

    Returns:
        str: The cluster name.

    Raises:
        RuntimeError: If cluster name is missing in the response.
    """
    with connection:
        cluster_information = Cluster()
        cluster_information.get()

        if not getattr(cluster_information, "name", None):
            raise RuntimeError(
                "Cluster name is missing in destination cluster response."
            )

    return cluster_information.name


# ---------------------------------------------------------------------------
# SVM operations
# ---------------------------------------------------------------------------


def validate_source_svm_exists(
    src_svm_name: str,
    src_connection: HostConnection,
) -> None:
    """Validate that the source SVM exists exactly once on the source cluster.

    Args:
        src_svm_name: Name of the source SVM.
        src_connection: HostConnection to the source cluster.

    Returns:
        None

    Raises:
        RuntimeError: If SVM count is 0 or if count is invalid (not exactly 1).
    """
    svm_count = Svm.count_collection(connection=src_connection, name=src_svm_name)

    match svm_count:
        case 1:
            logging.info("Validated source SVM '%s'.", src_svm_name)
        case 0:
            raise RuntimeError(
                f"Source SVM '{src_svm_name}' was not found on the source cluster."
            )
        case _:
            raise RuntimeError(
                "Source SVM validation returned an invalid count "
                f"({svm_count}) for '{src_svm_name}'."
            )


def ensure_destination_svm(
    dst_svm_name: str,
    dst_connection: HostConnection,
) -> bool:
    """Ensure the destination SVM exists. Create it if it does not.

    Args:
        dst_svm_name: Name of the destination SVM.
        dst_connection: HostConnection to the destination cluster.

    Returns:
        bool: True if SVM already existed, False if it was created.
    """
    # Check whether the destination SVM already exists
    existing = list(Svm.get_collection(connection=dst_connection, name=dst_svm_name))
    if existing:
        logging.info("Destination SVM '%s' already exists.", dst_svm_name)
        return True

    logging.info("Destination SVM '%s' not found, creating it.", dst_svm_name)
    svm = Svm(name=dst_svm_name)
    svm.set_connection(dst_connection)
    svm.post()
    logging.info("Destination SVM '%s' created successfully.", dst_svm_name)
    return False


def ensure_svm_peer(
    src_svm_name: str,
    dst_svm_name: str,
    src_connection: HostConnection,
    dst_connection: HostConnection,
) -> None:
    """Ensure an SVM peer relationship exists between source and destination SVMs.

    Args:
        src_svm_name: Name of the source SVM.
        dst_svm_name: Name of the destination SVM.
        src_connection: HostConnection to the source cluster.
        dst_connection: HostConnection to the destination cluster.

    Returns:
        None
    """
    dst_cluster_name = get_cluster_name(dst_connection)

    # Check for existing peering from the source side
    peers = list(
        SvmPeer.get_collection(
            connection=src_connection,
            **{
                "svm.name": src_svm_name,
                "peer.svm.name": dst_svm_name,
                "peer.cluster.name": dst_cluster_name,
            },
        )
    )
    if peers:
        logging.info(
            "SVM peering between '%s' and '%s' already exists.",
            src_svm_name,
            dst_svm_name,
        )
        return

    logging.info(
        "Creating SVM peering: '%s' <-> '%s'.",
        src_svm_name,
        dst_svm_name,
    )
    peer_body = {
        "svm": {"name": src_svm_name},
        "peer": {
            "cluster": {"name": dst_cluster_name},
            "svm": {"name": dst_svm_name},
        },
        "applications": ["snapmirror"],
    }
    peer = SvmPeer(**peer_body)
    peer.set_connection(src_connection)
    peer.post()
    logging.info("SVM peering created successfully.")


# ---------------------------------------------------------------------------
# Volume discovery
# ---------------------------------------------------------------------------


def get_source_volumes(
    svm_name: str,
    exclude: list[str],
    connection: HostConnection,
) -> list[VolumeInfo]:
    """Return a list of data volumes from the source SVM, excluding root and user-specified volumes.

    Args:
        svm_name: Name of the source SVM.
        exclude: List of volume names to exclude from results.
        connection: HostConnection to the source cluster.

    Returns:
        list[VolumeInfo]: List of VolumeInfo instances.
    """
    exclude_set = [v for v in exclude]
    if len(exclude_set) == 0:
        exclude_vols = {}
    else:
        exclude_vols = {"name": f"!{','.join(exclude_set)}"}
    volumes: list[VolumeInfo] = []

    fields = "uuid,name,svm.name,type"
    for vol in Volume.get_collection(
        connection=connection,
        fields=fields,
        is_svm_root=False,
        **{"svm.name": svm_name, "type": "rw", **exclude_vols},
    ):
        volumes.append(
            VolumeInfo(
                name=vol.name,
                uuid=vol.uuid,
                svm_name=svm_name,
            )
        )

    logging.info(
        "Discovered %d volume(s) in SVM '%s' for replication.",
        len(volumes),
        svm_name,
    )
    return volumes


# ---------------------------------------------------------------------------
# SnapMirror replication
# ---------------------------------------------------------------------------


def filter_existing_relationships(
    dst_svm_name: str,
    volumes: list[VolumeInfo],
    dst_connection: HostConnection,
) -> list[VolumeInfo]:
    """Filter out volumes that already have SnapMirror relationships.

    Args:
        dst_svm_name: Name of the destination SVM.
        volumes: List of VolumeInfo instances to filter.
        dst_connection: HostConnection to the destination cluster.

    Returns:
        list[VolumeInfo]: Filtered list of volumes without existing relationships.
    """
    new_volumes = []

    for vol in volumes:
        dst_path = f"{dst_svm_name}:{vol.name}"
        existing = list(
            SnapmirrorRelationship.get_collection(
                connection=dst_connection,
                **{"destination.path": dst_path},
            )
        )

        if existing:
            logging.warning(
                "SnapMirror relationship already exists for destination path '%s', skipping.",
                dst_path,
            )
        else:
            new_volumes.append(vol)

    return new_volumes


def build_relationship_body(
    ctx: ReplicationContext,
    volume_name: str,
) -> dict[str, object]:
    """Build the request body for a single SnapMirror relationship.

    Args:
        ctx: ReplicationContext containing cluster and SVM information.
        volume_name: Name of the volume to replicate.

    Returns:
        dict[str, object]: SnapMirror relationship request body with create_destination
            enabled, policy set to MirrorAllSnapshots, and state set to snapmirrored.
    """
    return {
        "source": {
            "cluster": {"name": ctx.src_cluster_name},
            "path": f"{ctx.src_svm_name}:{volume_name}",
        },
        "destination": {
            "path": f"{ctx.dst_svm_name}:{volume_name}",
        },
        "policy": {
            "name": DEFAULT_POLICY,
        },
        "create_destination": {
            "enabled": True,
        },
        "state": "snapmirrored",
    }


# noinspection SpellCheckingInspection
def create_snapmirror_relationships(
    ctx: ReplicationContext,
    volumes: list[VolumeInfo],
) -> None:
    """Create SnapMirror relationships in bulk for the given volumes.

    Filters out existing relationships and creates new ones in a single batch
    request via SnapmirrorRelationship.post_collection().

    Args:
        ctx: ReplicationContext containing cluster and SVM information.
        volumes: List of VolumeInfo instances to replicate.

    Returns:
        None
    """
    if not volumes:
        logging.info("No volumes found for SnapMirror replication, nothing to do.")
        return

    # Filter out volumes that already have relationships
    volumes = filter_existing_relationships(
        ctx.dst_svm_name,
        volumes,
        ctx.dst_connection,
    )

    if not volumes:
        logging.info(
            "No new volumes to replicate after filtering existing relationships."
        )
        return

    record_bodies = [build_relationship_body(ctx, vol.name) for vol in volumes]

    logging.info(
        "Creating %d SnapMirror relationship(s): %s -> %s",
        len(record_bodies),
        ctx.src_svm_name,
        ctx.dst_svm_name,
    )
    for rec in record_bodies:
        logging.debug("  %s", rec["source"]["path"])

    records = [SnapmirrorRelationship.from_dict(body) for body in record_bodies]
    SnapmirrorRelationship.post_collection(records, connection=ctx.dst_connection)

    logging.info("SnapMirror relationships created successfully and transfer started.")


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def run(args: argparse.Namespace) -> None:
    """Execute the full SnapMirror replication workflow.

    Args:
        args: Parsed command-line arguments containing cluster/SVM information.

    Returns:
        None
    """
    # 1. Establish connections
    src_conn = create_connection(
        args.source_cluster,
        args.source_username,
        args.source_password,
    )
    dst_conn = create_connection(
        args.destination_cluster,
        args.destination_username,
        args.destination_password,
    )

    # 2. Validate source SVM existence
    validate_source_svm_exists(args.source_svm, src_conn)

    # 3. Ensure destination SVM exists
    ensure_destination_svm(args.destination_svm, dst_conn)

    # 4. Ensure SVM peering is in place
    ensure_svm_peer(
        args.source_svm,
        args.destination_svm,
        src_conn,
        dst_conn,
    )

    # 5. Discover source volumes
    volumes = get_source_volumes(
        args.source_svm,
        args.exclude_volumes,
        src_conn,
    )

    # 6. Create SnapMirror relationships in bulk
    src_cluster_name = get_cluster_name(src_conn)
    ctx = ReplicationContext(
        src_cluster_name=src_cluster_name,
        src_svm_name=args.source_svm,
        dst_svm_name=args.destination_svm,
        dst_connection=dst_conn,
    )
    create_snapmirror_relationships(ctx, volumes)


def main() -> None:
    """Entry point: parse arguments and run the replication workflow.

    Returns:
        None

    Raises:
        sys.exit(1): On any exception during replication workflow.
    """
    setup_logging()
    args = parse_args()

    try:
        run(args)
    except Exception:
        logging.exception("Error during SnapMirror replication.")
        sys.exit(1)

    logging.info("SnapMirror replication completed successfully.")


if __name__ == "__main__":
    main()
