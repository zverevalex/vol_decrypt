#!/usr/bin/env python3
"""ontap_migrate.py — NetApp ONTAP Migration Entry Point.

Provides two subcommands that together implement a semi-automatic
volume migration with SnapMirror as the data transport:

  replicate   Discover source volumes, create unencrypted DP volumes on
              the destination cluster, and establish SnapMirror
              relationships in bulk.

  collect     Read CIFS share / NFS export properties from the source
              volumes and write a cutover state JSON file.

  cutover     Load the cutover state, confirm with the operator, then
              break SnapMirror, remount volumes, and re-create protocol
              shares/exports on the destination.

Usage examples:
    python ontap_migrate.py replicate \\
        --source-cluster 10.0.0.1 --source-username admin \\
        --destination-cluster 10.0.0.2 --destination-username admin \\
        --source-svm vs_prod --protocol cifs

    python ontap_migrate.py collect \\
        --source-cluster 10.0.0.1 --source-username admin \\
        --destination-cluster 10.0.0.2 --destination-username admin \\
        --source-svm vs_prod --protocol cifs

    python ontap_migrate.py cutover \\
        --source-cluster 10.0.0.1 --source-username admin \\
        --destination-cluster 10.0.0.2 --destination-username admin \\
        --source-svm vs_prod --protocol cifs
"""

import argparse
import logging
import sys
from pathlib import Path

from netapp_ontap.resources import Volume as OntapVolume

from migrate.cutover import (
    CUTOVER_STATE_FILENAME,
    CutoverExecutor,
    CutoverStateMap,
    ExportInfo,
    NfsPolicyInfo,
    NfsRuleInfo,
    ShareInfo,
    collect_cifs_shares,
    collect_nfs_exports,
    collect_nfs_policies,
    load_cutover_state,
    write_cutover_state,
)
from migrate.snapmirror import (
    DST_SVM_SUFFIX,
    DST_VOLUME_SUFFIX,
    ENV_DST_PASSWORD_VAR,
    ENV_SRC_PASSWORD_VAR,
    ReplicationContext,
    _resolve_password,
    create_connection,
    create_snapmirror_relationships,
    ensure_destination_svm,
    ensure_svm_peer,
    get_cluster_name,
    get_source_volumes,
    setup_logging,
    validate_source_svm_exists,
)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_PROTOCOL_CHOICES = ("cifs", "nfs", "both")


def _build_common_parser(subparser: argparse.ArgumentParser) -> None:
    """Add shared cluster/SVM arguments to a subcommand parser.

    Args:
        subparser: The ArgumentParser instance to extend.

    Returns:
        None
    """
    subparser.add_argument(
        "--source-cluster",
        required=True,
        help="Source cluster management IP or hostname.",
    )
    subparser.add_argument(
        "--source-username",
        required=True,
        help="Admin username for the source cluster.",
    )
    subparser.add_argument(
        "--source-password",
        default=None,
        help=(
            f"Source cluster password. If omitted, read from "
            f"${ENV_SRC_PASSWORD_VAR} or prompted interactively."
        ),
    )
    subparser.add_argument(
        "--destination-cluster",
        required=True,
        help="Destination cluster management IP or hostname.",
    )
    subparser.add_argument(
        "--destination-username",
        required=True,
        help="Admin username for the destination cluster.",
    )
    subparser.add_argument(
        "--destination-password",
        default=None,
        help=(
            f"Destination cluster password. If omitted, read from "
            f"${ENV_DST_PASSWORD_VAR} or prompted interactively."
        ),
    )
    subparser.add_argument(
        "--source-svm",
        required=True,
        help="Name of the source SVM.",
    )
    subparser.add_argument(
        "--destination-svm",
        default=None,
        help=(
            f"Name of the destination SVM. Defaults to <source-svm>{DST_SVM_SUFFIX}."
        ),
    )
    subparser.add_argument(
        "--protocol",
        choices=_PROTOCOL_CHOICES,
        default="cifs",
        help="Protocol to migrate (cifs, nfs, or both). Defaults to cifs.",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse top-level arguments and subcommand arguments.

    Args:
        argv: Optional argument list. Defaults to sys.argv when None.

    Returns:
        argparse.Namespace: Parsed arguments with resolved passwords and
            default destination SVM name.
    """
    parser = argparse.ArgumentParser(
        prog="ontap_migrate",
        description="Semi-automatic ONTAP volume migration via SnapMirror.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(
        dest="command",
        metavar="COMMAND",
        required=True,
    )

    # -- replicate ----------------------------------------------------------
    replicate_parser = subparsers.add_parser(
        "replicate",
        help="Create DP volumes and SnapMirror relationships.",
    )
    _build_common_parser(replicate_parser)
    replicate_parser.add_argument(
        "--exclude-volumes",
        nargs="*",
        default=[],
        metavar="VOL",
        help="Volume name(s) to exclude from replication.",
    )

    # -- collect ------------------------------------------------------------
    collect_parser = subparsers.add_parser(
        "collect",
        help="Read share/export properties and write cutover state JSON.",
    )
    _build_common_parser(collect_parser)
    collect_parser.add_argument(
        "--exclude-volumes",
        nargs="*",
        default=[],
        metavar="VOL",
        help="Volume name(s) to exclude from collection.",
    )

    # -- cutover ------------------------------------------------------------
    cutover_parser = subparsers.add_parser(
        "cutover",
        help="Execute the confirmed cutover from source to destination.",
    )
    _build_common_parser(cutover_parser)

    args = parser.parse_args(argv)

    # Resolve passwords
    args.source_password = _resolve_password(
        explicit=args.source_password,
        env_var=ENV_SRC_PASSWORD_VAR,
        prompt_label=f"Password for {args.source_username}@{args.source_cluster}",
    )

    # When source and destination cluster are the same host, reuse the
    # source credentials so the operator is not prompted twice for the
    # same password.
    same_cluster = (
        args.source_cluster.strip().lower() == args.destination_cluster.strip().lower()
    )
    if same_cluster:
        logging.warning(
            "Source and destination cluster are identical ('%s'). "
            "Reusing source credentials for destination.",
            args.source_cluster,
        )
        args.destination_username = args.source_username
        args.destination_password = args.source_password
    else:
        args.destination_password = _resolve_password(
            explicit=args.destination_password,
            env_var=ENV_DST_PASSWORD_VAR,
            prompt_label=(
                f"Password for {args.destination_username}@{args.destination_cluster}"
            ),
        )

    # Default destination SVM
    if args.destination_svm is None:
        args.destination_svm = f"{args.source_svm}{DST_SVM_SUFFIX}"

    return args


# ---------------------------------------------------------------------------
# Migration orchestrator
# ---------------------------------------------------------------------------


class OntapMigrate:
    """Orchestrates the full SnapMirror-based volume migration workflow.

    Coordinates SnapMirror setup (replicate), protocol state collection
    (collect), and the actual cutover (cutover) across source and
    destination ONTAP clusters.

    Args:
        args: Parsed CLI arguments produced by parse_args().
    """

    def __init__(self, args: argparse.Namespace) -> None:
        """Store parsed arguments and initialise cluster connections.

        When source and destination cluster are the same host, a single
        ``HostConnection`` is reused for both roles to avoid opening a
        redundant second connection to the same cluster.

        Args:
            args: Parsed CLI arguments with cluster credentials and SVM names.
        """
        self._args = args
        self._state_path = Path(CUTOVER_STATE_FILENAME)

        self._src_conn = create_connection(
            args.source_cluster,
            args.source_username,
            args.source_password,
        )

        same_cluster = (
            args.source_cluster.strip().lower()
            == args.destination_cluster.strip().lower()
        )
        if same_cluster:
            logging.info(
                "Same cluster detected ('%s') — reusing source connection "
                "for destination.",
                args.source_cluster,
            )
            self._dst_conn = self._src_conn
        else:
            self._dst_conn = create_connection(
                args.destination_cluster,
                args.destination_username,
                args.destination_password,
            )

    # ------------------------------------------------------------------
    # replicate
    # ------------------------------------------------------------------

    def run_replicate(self) -> None:
        """Run the SnapMirror replication setup workflow.

        Validates the source SVM, ensures the destination SVM and SVM
        peering exist, then creates DP volumes and SnapMirror
        relationships in bulk.

        Returns:
            None
        """
        args = self._args

        validate_source_svm_exists(args.source_svm, self._src_conn)
        ensure_destination_svm(args.destination_svm, self._dst_conn)
        ensure_svm_peer(
            args.source_svm,
            args.destination_svm,
            self._src_conn,
            self._dst_conn,
        )

        volumes = get_source_volumes(
            args.source_svm,
            getattr(args, "exclude_volumes", []),
            self._src_conn,
        )

        src_cluster_name = get_cluster_name(self._src_conn)
        ctx = ReplicationContext(
            src_cluster_name=src_cluster_name,
            src_svm_name=args.source_svm,
            dst_svm_name=args.destination_svm,
            dst_connection=self._dst_conn,
        )
        create_snapmirror_relationships(ctx, volumes)

    # ------------------------------------------------------------------
    # collect
    # ------------------------------------------------------------------

    def run_collect(self) -> None:
        """Collect CIFS/NFS protocol state and write the cutover state file.

        Reads CIFS shares and/or NFS export policies from the source SVM
        for all replicating volumes and persists them to the cutover
        state JSON file in the application root.

        Returns:
            None
        """
        args = self._args

        if self._state_path.exists():
            logging.warning(
                "Cutover state file '%s' already exists and will be overwritten.",
                self._state_path,
            )

        volumes = get_source_volumes(
            args.source_svm,
            getattr(args, "exclude_volumes", []),
            self._src_conn,
        )
        volume_names = [v.name for v in volumes]

        shares: list[ShareInfo] = []
        exports: list[ExportInfo] = []
        nfs_policies: list[NfsPolicyInfo] = []

        if args.protocol in ("cifs", "both"):
            shares = collect_cifs_shares(args.source_svm, volume_names, self._src_conn)
        if args.protocol in ("nfs", "both"):
            exports = collect_nfs_exports(args.source_svm, volume_names, self._src_conn)
            nfs_policies = collect_nfs_policies(
                args.source_svm,
                exports,
                self._src_conn,
            )

        write_cutover_state(
            src_svm=args.source_svm,
            dst_svm=args.destination_svm,
            shares=shares,
            exports=exports,
            nfs_policies=nfs_policies,
            state_path=self._state_path,
            volume_names=volume_names,
        )
        logging.info(
            "Collect complete. Review '%s' before running cutover.",
            self._state_path,
        )

    # ------------------------------------------------------------------
    # cutover
    # ------------------------------------------------------------------

    def run_cutover(self) -> None:
        """Execute the operator-confirmed cutover sequence.

        Loads the cutover state file, displays a summary of changes,
        waits for explicit confirmation, then runs the CutoverExecutor
        for each volume in the state.

        Returns:
            None

        Raises:
            FileNotFoundError: If the cutover state file does not exist.
            RuntimeError: If any cutover step fails.
        """
        state: CutoverStateMap = load_cutover_state(self._state_path)

        src_svm: str = str(state["src_svm"])
        dst_svm: str = str(state["dst_svm"])
        raw_shares: list[dict] = state.get("cifs_shares", [])  # type: ignore[assignment]
        raw_exports: list[dict] = state.get("nfs_exports", [])  # type: ignore[assignment]
        raw_nfs_policies: list[dict] = state.get("nfs_policies", [])  # type: ignore[assignment]
        raw_volume_names: list[str] = state.get("volume_names", [])  # type: ignore[assignment]

        shares = [
            ShareInfo(
                share_name=str(share.get("share_name", "")),
                volume_name=str(share.get("volume_name", "")),
                path=str(share.get("path", "/")),
                comment=str(share.get("comment", "")),
                acls=list(share.get("acls", [])),
            )
            for share in raw_shares
        ]
        exports = [ExportInfo(**e) for e in raw_exports]
        nfs_policies = [
            NfsPolicyInfo(
                source_policy_name=str(policy.get("source_policy_name", "")),
                destination_policy_name=str(policy.get("destination_policy_name", "")),
                rules=[NfsRuleInfo(**rule) for rule in list(policy.get("rules", []))],
            )
            for policy in raw_nfs_policies
        ]

        self._print_cutover_summary(src_svm, dst_svm, shares, exports)

        answer = (
            input("\nProceed with cutover? This action is irreversible. [yes/no]: ")
            .strip()
            .lower()
        )
        if answer != "yes":
            logging.info("Cutover aborted by operator.")
            return

        executor = CutoverExecutor(
            src_svm=src_svm,
            dst_svm=dst_svm,
            src_connection=self._src_conn,
            dst_connection=self._dst_conn,
            state_path=self._state_path,
        )

        all_volume_names = {
            str(volume_name) for volume_name in raw_volume_names if str(volume_name)
        }
        if not all_volume_names:
            # Legacy fallback for state files without explicit volume_names.
            vol_names_from_shares = {s.volume_name for s in shares if s.volume_name}
            vol_names_from_exports = {e.volume_name for e in exports if e.volume_name}
            all_volume_names = vol_names_from_shares | vol_names_from_exports

        for vol_name in sorted(all_volume_names):
            junction_path = self._resolve_junction_path(vol_name)
            executor.execute(
                volume_name=vol_name,
                junction_path=junction_path,
                shares=shares,
                exports=exports,
                protocol=self._args.protocol,
                nfs_policies=nfs_policies,
            )

        logging.info("All cutover operations completed successfully.")

    def _resolve_junction_path(self, volume_name: str) -> str:
        """Derive the destination junction path from the source volume.

        Reads the current junction path of the source volume and
        appends ``DST_VOLUME_SUFFIX`` to produce the destination path.

        Args:
            volume_name: Name of the source volume.

        Returns:
            str: Destination junction path (e.g. ``/vol_sales_dst``).
        """
        vol_results = list(
            OntapVolume.get_collection(
                connection=self._src_conn,
                fields="nas.path",
                **{
                    "svm.name": self._args.source_svm,
                    "name": volume_name,
                },
            )
        )
        if not vol_results:
            logging.warning(
                "Could not read junction path for '%s', defaulting to '/%s%s'.",
                volume_name,
                volume_name,
                DST_VOLUME_SUFFIX,
            )
            return f"/{volume_name}{DST_VOLUME_SUFFIX}"

        src_path = getattr(getattr(vol_results[0], "nas", None), "path", None)
        if not src_path:
            return f"/{volume_name}{DST_VOLUME_SUFFIX}"

        return f"{src_path.rstrip('/')}{DST_VOLUME_SUFFIX}"

    @staticmethod
    def _print_cutover_summary(
        src_svm: str,
        dst_svm: str,
        shares: list[ShareInfo],
        exports: list[ExportInfo],
    ) -> None:
        """Print a human-readable summary of the planned cutover actions.

        Args:
            src_svm: Name of the source SVM.
            dst_svm: Name of the destination SVM.
            shares: List of CIFS shares to be recreated.
            exports: List of NFS exports to be reassigned.

        Returns:
            None
        """
        same = src_svm == dst_svm
        print("\n" + "=" * 60)
        print("  CUTOVER PLAN SUMMARY")
        print("=" * 60)
        print(f"  Source SVM      : {src_svm}")
        print(f"  Destination SVM : {dst_svm}")
        if same:
            print("  Mode            : Same-SVM (remount only, no share recreation)")
        else:
            print("  Mode            : Cross-SVM (full share recreation)")

        if shares:
            print(f"\n  CIFS shares to migrate ({len(shares)}):")
            for share in shares:
                print(
                    f"    - {share.share_name} "
                    f"(volume: {share.volume_name}, path: {share.path})"
                )
        if exports:
            print(f"\n  NFS export policies to reassign ({len(exports)}):")
            for exp in exports:
                print(f"    - policy '{exp.policy_name}' (volume: {exp.volume_name})")
        print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Parse arguments and dispatch to the correct migration subcommand.

    Returns:
        None

    Raises:
        SystemExit: With exit code 1 on any unhandled exception.
    """
    setup_logging()
    args = parse_args()

    migrator = OntapMigrate(args)

    try:
        match args.command:
            case "replicate":
                migrator.run_replicate()
            case "collect":
                migrator.run_collect()
            case "cutover":
                migrator.run_cutover()
    except (RuntimeError, FileNotFoundError, ValueError):
        logging.exception("Migration failed.")
        sys.exit(1)

    logging.info("Command '%s' completed successfully.", args.command)


if __name__ == "__main__":
    main()
