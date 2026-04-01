#!/usr/bin/env python3
"""cutover.py — Semi-Automatic CIFS/NFS Cutover for SnapMirror Migrations.

Collects CIFS share and NFS export policy properties from source volumes,
persists the state to a JSON file, and executes a user-confirmed cutover
sequence:

  1. Break the SnapMirror relationship (state: broken_off).
  2. Unmount the source volume (remove junction_path).
  3. Mount the destination volume (set new junction_path).
  4. Re-create CIFS shares / NFS exports on the destination volume.

When source and destination SVM are identical, step 4 (share/export
recreation) is skipped because the share already points to the SVM;
only the volume remount is required.

This module is imported by ontap_migrate.py and is not intended to be
run standalone.
"""

import json
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import NamedTuple, TypeAlias

from netapp_ontap import HostConnection
from netapp_ontap.resources import (
    CifsShare,
    ExportPolicy,
    ExportRule,
    SnapmirrorRelationship,
    Volume,
)

from migrate.snapmirror import DST_VOLUME_SUFFIX

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CUTOVER_STATE_FILENAME = "cutover_state.json"

# ---------------------------------------------------------------------------
# Type definitions
# ---------------------------------------------------------------------------

SharePropertyList: TypeAlias = list[dict[str, object]]
CutoverStateMap: TypeAlias = dict[str, object]


class ShareInfo(NamedTuple):
    """CIFS share properties captured before a cutover.

    Attributes:
        share_name: Name of the CIFS share.
        volume_name: Name of the source volume hosting the share.
        path: Share path (relative to volume root, e.g. ``/``).
        comment: Optional share comment.
        acls: Optional share ACL entries for cross-SVM recreation.
    """

    share_name: str
    volume_name: str
    path: str
    comment: str
    acls: list[dict[str, object]]


class ExportInfo(NamedTuple):
    """NFS export policy properties captured before a cutover.

    Attributes:
        policy_name: Name of the export policy.
        volume_name: Name of the source volume using the policy.
    """

    policy_name: str
    volume_name: str


class NfsRuleInfo(NamedTuple):
    """NFS export rule definition captured for migration.

    Attributes:
        clients: Client match entries (for example CIDR hosts).
        protocols: Allowed NFS protocols (for example ``nfs3``, ``nfs4``).
        ro_rule: Read-only authentication flavors.
        rw_rule: Read-write authentication flavors.
        superuser: Superuser authentication flavors.
        anonymous_user: Anonymous UID mapping.
        allow_suid: Whether SetUID operations are honored.
        allow_device_creation: Whether device creation is allowed.
        chown_mode: Ownership mode restrictions.
        ntfs_unix_security: NTFS export UNIX security behavior.
        index: Rule index inside the export policy.
    """

    clients: list[dict[str, str]]
    protocols: list[str]
    ro_rule: list[str]
    rw_rule: list[str]
    superuser: list[str]
    anonymous_user: str | None
    allow_suid: bool | None
    allow_device_creation: bool | None
    chown_mode: str | None
    ntfs_unix_security: str | None
    index: int | None


class NfsPolicyInfo(NamedTuple):
    """NFS export policy and its rules captured for migration.

    Attributes:
        source_policy_name: Policy name on the source SVM.
        destination_policy_name: Policy name to apply on destination SVM.
        rules: Rule definitions that belong to the policy.
    """

    source_policy_name: str
    destination_policy_name: str
    rules: list[NfsRuleInfo]


# ---------------------------------------------------------------------------
# Share / export discovery
# ---------------------------------------------------------------------------


def collect_cifs_shares(
    svm_name: str,
    volume_names: list[str],
    connection: HostConnection,
) -> list[ShareInfo]:
    """Return all CIFS shares hosted on the specified volumes.

    Queries CifsShare for the given SVM and filters results to only
    include shares whose ``volume.name`` matches one of the provided
    volume names.

    Args:
        svm_name: Name of the source SVM.
        volume_names: List of volume names to collect shares for.
        connection: HostConnection to the source cluster.

    Returns:
        list[ShareInfo]: CIFS share details for all matching shares.
    """
    volume_set = set(volume_names)
    fields = "name,volume.name,path,comment,acls"
    shares: list[ShareInfo] = []

    for share in CifsShare.get_collection(
        connection=connection,
        fields=fields,
        **{"svm.name": svm_name},
    ):
        raw_vol_name = getattr(getattr(share, "volume", None), "name", None)
        if not isinstance(raw_vol_name, str):
            continue
        if raw_vol_name not in volume_set:
            continue

        share_name = getattr(share, "name", None)
        if not isinstance(share_name, str) or not share_name:
            continue

        raw_path = getattr(share, "path", "/")
        raw_comment = getattr(share, "comment", "")
        share_path = raw_path if isinstance(raw_path, str) else "/"
        share_comment = raw_comment if isinstance(raw_comment, str) else ""

        shares.append(
            ShareInfo(
                share_name=share_name,
                volume_name=raw_vol_name,
                path=share_path,
                comment=share_comment,
                acls=_serialize_share_acls(getattr(share, "acls", None)),
            )
        )

    logging.info(
        "Collected %d CIFS share(s) from SVM '%s'.",
        len(shares),
        svm_name,
    )
    return shares


def _serialize_share_acls(raw_acls: object) -> list[dict[str, object]]:
    """Convert SDK ACL objects into JSON-serializable dictionaries.

    Args:
        raw_acls: ACL collection from the SDK object.

    Returns:
        list[dict[str, object]]: Normalized ACL entries.
    """
    if not raw_acls:
        return []
    if isinstance(raw_acls, (str, bytes)):
        return []
    if not isinstance(raw_acls, Iterable):
        return []

    serialized: list[dict[str, object]] = []
    for acl in list(raw_acls):
        if isinstance(acl, dict):
            serialized.append(dict(acl))
            continue

        acl_dict = {
            key: value
            for key in (
                "user_or_group",
                "permission",
                "type",
                "sid",
                "name",
            )
            if (value := getattr(acl, key, None)) is not None
        }
        if acl_dict:
            serialized.append(acl_dict)

    return serialized


def collect_nfs_exports(
    svm_name: str,
    volume_names: list[str],
    connection: HostConnection,
) -> list[ExportInfo]:
    """Return NFS export policies associated with the specified volumes.

    Queries Volume for each volume and reads the ``nas.export_policy.name``
    attribute to map volumes to their export policies.

    Args:
        svm_name: Name of the source SVM.
        volume_names: List of volume names to inspect.
        connection: HostConnection to the source cluster.

    Returns:
        list[ExportInfo]: Export policy name and associated volume name
            for all volumes that have an NFS export policy set.
    """
    exports: list[ExportInfo] = []
    fields = "name,nas.export_policy.name"

    for vol_name in volume_names:
        results = list(
            Volume.get_collection(
                connection=connection,
                fields=fields,
                **{"svm.name": svm_name, "name": vol_name},
            )
        )
        if not results:
            continue
        vol = results[0]
        raw_policy_name = getattr(
            getattr(getattr(vol, "nas", None), "export_policy", None),
            "name",
            None,
        )
        if isinstance(raw_policy_name, str) and raw_policy_name:
            exports.append(
                ExportInfo(
                    policy_name=raw_policy_name,
                    volume_name=vol_name,
                )
            )

    logging.info(
        "Collected %d NFS export policy mapping(s) from SVM '%s'.",
        len(exports),
        svm_name,
    )
    return exports


def collect_nfs_policies(
    svm_name: str,
    exports: list[ExportInfo],
    connection: HostConnection,
) -> list[NfsPolicyInfo]:
    """Return NFS export policies with full rule definitions.

    Resolves each unique policy referenced by ``exports`` on the source
    SVM, reads all rules for that policy, and returns a structured list
    that can be persisted in ``cutover_state.json``.

    Args:
        svm_name: Name of the source SVM.
        exports: Volume-to-policy mappings collected from source volumes.
        connection: HostConnection to the source cluster.

    Returns:
        list[NfsPolicyInfo]: Collected policy definitions with nested rules.
    """
    unique_policy_names = sorted({exp.policy_name for exp in exports})
    if not unique_policy_names:
        logging.info(
            "No NFS policies referenced by selected volumes in SVM '%s'.",
            svm_name,
        )
        return []

    policies: list[NfsPolicyInfo] = []

    for policy_name in unique_policy_names:
        source_policies = list(
            ExportPolicy.get_collection(
                connection=connection,
                fields="id,name",
                **{"svm.name": svm_name, "name": policy_name},
            )
        )
        if not source_policies:
            logging.warning(
                "NFS export policy '%s' was referenced but not found in "
                "SVM '%s'; skipping policy collection.",
                policy_name,
                svm_name,
            )
            continue

        source_policy = source_policies[0]
        policy_id = getattr(source_policy, "id", None)
        if policy_id is None:
            logging.warning(
                "NFS export policy '%s' in SVM '%s' has no id; skipping.",
                policy_name,
                svm_name,
            )
            continue

        rules = list(
            ExportRule.get_collection(
                policy_id,
                connection=connection,
                fields=(
                    "index,clients,protocols,ro_rule,rw_rule,superuser,"
                    "anonymous_user,allow_suid,allow_device_creation,"
                    "chown_mode,ntfs_unix_security"
                ),
            )
        )

        rule_infos: list[NfsRuleInfo] = []
        for rule in rules:
            raw_clients = getattr(rule, "clients", None) or []
            clients: list[dict[str, str]] = [
                {"match": str(getattr(client, "match", ""))}
                for client in raw_clients
                if getattr(client, "match", None)
            ]
            rule_infos.append(
                NfsRuleInfo(
                    clients=clients,
                    protocols=list(getattr(rule, "protocols", None) or []),
                    ro_rule=list(getattr(rule, "ro_rule", None) or []),
                    rw_rule=list(getattr(rule, "rw_rule", None) or []),
                    superuser=list(getattr(rule, "superuser", None) or []),
                    anonymous_user=getattr(rule, "anonymous_user", None),
                    allow_suid=getattr(rule, "allow_suid", None),
                    allow_device_creation=getattr(
                        rule,
                        "allow_device_creation",
                        None,
                    ),
                    chown_mode=getattr(rule, "chown_mode", None),
                    ntfs_unix_security=getattr(
                        rule,
                        "ntfs_unix_security",
                        None,
                    ),
                    index=getattr(rule, "index", None),
                )
            )

        policies.append(
            NfsPolicyInfo(
                source_policy_name=policy_name,
                destination_policy_name=policy_name,
                rules=sorted(
                    rule_infos,
                    key=lambda item: (
                        item.index is None,
                        item.index if item.index is not None else 0,
                    ),
                ),
            )
        )

    logging.info(
        "Collected %d NFS export policy definition(s) from SVM '%s'.",
        len(policies),
        svm_name,
    )
    return policies


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def write_cutover_state(
    src_svm: str,
    dst_svm: str,
    shares: list[ShareInfo],
    exports: list[ExportInfo],
    nfs_policies: list[NfsPolicyInfo] | None,
    state_path: Path,
    migrated_volumes: list[str] | None = None,
    volume_names: list[str] | None = None,
) -> None:
    """Serialise cutover state to a JSON file in the application root.

    The state file captures source/destination SVM names, CIFS share
    properties, NFS export policy mappings, and the list of volumes
    already successfully cut over.

    Args:
        src_svm: Name of the source SVM.
        dst_svm: Name of the destination SVM.
        shares: List of ShareInfo instances to persist.
        exports: List of ExportInfo instances to persist.
        nfs_policies: Optional list of NFS policy/rule definitions.
        state_path: Absolute path to the output JSON file.
        migrated_volumes: Optional list of volume names that have
            already been successfully cut over. Defaults to an empty
            list when omitted.
        volume_names: Optional explicit list of source volume names to
            process during cutover. If omitted, it is derived from
            share/export data.

    Returns:
        None
    """
    derived_volume_names = sorted(
        {
            *[share.volume_name for share in shares],
            *[export.volume_name for export in exports],
        }
    )

    payload: CutoverStateMap = {
        "src_svm": src_svm,
        "dst_svm": dst_svm,
        "volume_names": sorted(set(volume_names or derived_volume_names)),
        "cifs_shares": [share._asdict() for share in shares],
        "nfs_exports": [exp._asdict() for exp in exports],
        "nfs_policies": [
            _serialize_nfs_policy(policy) for policy in (nfs_policies or [])
        ],
        "migrated_volumes": migrated_volumes or [],
    }
    state_path.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
    logging.info("Cutover state written to '%s'.", state_path)


def mark_volume_migrated(volume_name: str, state_path: Path) -> None:
    """Append a volume name to the migrated_volumes list in the state file.

    Reads the existing state, adds the volume to ``migrated_volumes``
    (if not already present), and writes the updated state back to disk.

    Args:
        volume_name: Name of the source volume that was successfully
            cut over.
        state_path: Path to the cutover state JSON file.

    Returns:
        None

    Raises:
        FileNotFoundError: If the state file does not exist.
        ValueError: If the state file is missing required keys.
    """
    state = load_cutover_state(state_path)
    migrated: list[str] = list(state.get("migrated_volumes", []))  # type: ignore[arg-type]
    if volume_name not in migrated:
        migrated.append(volume_name)
        state["migrated_volumes"] = migrated
        state_path.write_text(
            json.dumps(state, indent=2),
            encoding="utf-8",
        )
        logging.info(
            "Volume '%s' marked as migrated in '%s'.",
            volume_name,
            state_path,
        )


def _serialize_nfs_policy(policy: NfsPolicyInfo) -> dict[str, object]:
    """Convert an NfsPolicyInfo object into a JSON-serializable dict.

    Args:
        policy: NFS policy definition with nested rules.

    Returns:
        dict[str, object]: Serialized policy dictionary.
    """
    return {
        "source_policy_name": policy.source_policy_name,
        "destination_policy_name": policy.destination_policy_name,
        "rules": [rule._asdict() for rule in policy.rules],
    }


def load_cutover_state(state_path: Path) -> CutoverStateMap:
    """Load and return a previously written cutover state from JSON.

    Args:
        state_path: Path to the cutover state JSON file.

    Returns:
        CutoverStateMap: Deserialised state dictionary.

    Raises:
        FileNotFoundError: If the state file does not exist.
        ValueError: If the JSON content is invalid or missing required keys.
    """
    if not state_path.exists():
        raise FileNotFoundError(
            f"Cutover state file not found: {state_path}. Run the 'collect' step first."
        )

    raw = json.loads(state_path.read_text(encoding="utf-8"))

    for required_key in ("src_svm", "dst_svm", "cifs_shares", "nfs_exports"):
        if required_key not in raw:
            raise ValueError(
                f"Cutover state file is missing required key '{required_key}'."
            )

    # Backward compatibility with older state files.
    if "volume_names" not in raw:
        legacy_share_volumes = {
            str(share.get("volume_name", ""))
            for share in list(raw.get("cifs_shares", []))
            if str(share.get("volume_name", ""))
        }
        legacy_export_volumes = {
            str(export.get("volume_name", ""))
            for export in list(raw.get("nfs_exports", []))
            if str(export.get("volume_name", ""))
        }
        raw["volume_names"] = sorted(legacy_share_volumes | legacy_export_volumes)
    if "nfs_policies" not in raw:
        raw["nfs_policies"] = []
    if "migrated_volumes" not in raw:
        raw["migrated_volumes"] = []

    logging.info("Loaded cutover state from '%s'.", state_path)
    return raw


# ---------------------------------------------------------------------------
# Cutover executor
# ---------------------------------------------------------------------------


class CutoverExecutor:
    """Orchestrates the cutover sequence for a single volume.

    Handles SnapMirror break, source unmount, destination mount, and
    protocol share/export recreation. Detects the same-SVM case and
    skips share recreation when source and destination SVM are identical.

    Tracks which volumes have already been successfully migrated via
    the ``migrated_volumes`` list in the cutover state JSON file.
    Already-migrated volumes are skipped with a warning log entry.

    Args:
        src_svm: Name of the source SVM.
        dst_svm: Name of the destination SVM.
        src_connection: HostConnection to the source cluster.
        dst_connection: HostConnection to the destination cluster.
        state_path: Path to the cutover state JSON file used to persist
            migration progress.
    """

    def __init__(
        self,
        src_svm: str,
        dst_svm: str,
        src_connection: HostConnection,
        dst_connection: HostConnection,
        state_path: Path | None = None,
    ) -> None:
        """Initialise the executor with SVM names, cluster connections,
        and optional state-file path.

        Args:
            src_svm: Name of the source SVM.
            dst_svm: Name of the destination SVM.
            src_connection: HostConnection to the source cluster.
            dst_connection: HostConnection to the destination cluster.
            state_path: Optional path to the cutover state JSON file.
                When provided, successfully migrated volumes are recorded
                so subsequent runs can skip already-completed work.
        """
        self._src_svm = src_svm
        self._dst_svm = dst_svm
        self._src_conn = src_connection
        self._dst_conn = dst_connection
        self._same_svm = src_svm == dst_svm
        self._nfs_policy_sync_done = False
        self._nfs_policy_map: dict[str, str] = {}
        self._state_path = state_path

    # ------------------------------------------------------------------
    # SnapMirror
    # ------------------------------------------------------------------

    def update_snapmirror(self, volume_name: str) -> None:
        """Run a final blocking SnapMirror update before break.

        The update is executed as a blocking PATCH operation with
        ``poll=True`` and ``poll_interval=10`` so cutover only continues
        after ONTAP reports completion.

        Args:
            volume_name: Name of the source volume whose relationship
                should be updated.

        Returns:
            None

        Raises:
            RuntimeError: If no matching SnapMirror relationship is found.
        """
        dst_vol_name = f"{volume_name}{DST_VOLUME_SUFFIX}"
        dst_path = f"{self._dst_svm}:{dst_vol_name}"

        relationships = list(
            SnapmirrorRelationship.get_collection(
                connection=self._dst_conn,
                **{"destination.path": dst_path},
                fields="uuid,state",
            )
        )
        if not relationships:
            raise RuntimeError(
                f"No SnapMirror relationship found for destination path '{dst_path}'."
            )

        rel = relationships[0]
        rel.set_connection(self._dst_conn)
        rel.state = "snapmirrored"
        rel.patch(poll=True, poll_interval=10)
        logging.info(
            "Final SnapMirror update completed for '%s'.",
            dst_path,
        )

    def break_snapmirror(self, volume_name: str) -> None:
        """Break the SnapMirror relationship for the given volume.

        Sets the relationship state to ``broken_off`` via a PATCH on the
        SnapmirrorRelationship resource. This must be done before any
        volume remount operation.

        Args:
            volume_name: Name of the source volume whose relationship
                should be broken.

        Returns:
            None

        Raises:
            RuntimeError: If no matching SnapMirror relationship is found.
        """
        dst_vol_name = f"{volume_name}{DST_VOLUME_SUFFIX}"
        dst_path = f"{self._dst_svm}:{dst_vol_name}"

        relationships = list(
            SnapmirrorRelationship.get_collection(
                connection=self._dst_conn,
                **{"destination.path": dst_path},
                fields="uuid,state",
            )
        )

        if not relationships:
            raise RuntimeError(
                f"No SnapMirror relationship found for destination path '{dst_path}'."
            )

        rel = relationships[0]
        rel.set_connection(self._dst_conn)
        rel.state = "broken_off"
        rel.patch()

        logging.info(
            "SnapMirror relationship for '%s' set to broken_off.",
            dst_path,
        )

    # ------------------------------------------------------------------
    # Volume mount / unmount
    # ------------------------------------------------------------------

    # noinspection PyMethodMayBeStatic
    def _get_junction_path(
        self,
        svm_name: str,
        volume_name: str,
        connection: HostConnection,
    ) -> str | None:
        """Return the current junction path of a volume, or None if unmounted.

        Args:
            svm_name: SVM name where the volume resides.
            volume_name: Name of the volume.
            connection: HostConnection to the cluster.

        Returns:
            str | None: Junction path string, or None if not mounted.
        """
        results = list(
            Volume.get_collection(
                connection=connection,
                fields="nas.path",
                **{"svm.name": svm_name, "name": volume_name},
            )
        )
        if not results:
            return None
        return getattr(getattr(results[0], "nas", None), "path", None)

    def unmount_source_volume(self, volume_name: str) -> None:
        """Remove the junction path from the source volume (unmount).

        Patches the source volume with an empty ``nas.path`` to detach
        it from the namespace. Skips the operation if the volume is not
        currently mounted.

        Args:
            volume_name: Name of the source volume to unmount.

        Returns:
            None
        """
        junction = self._get_junction_path(self._src_svm, volume_name, self._src_conn)
        if not junction:
            logging.info(
                "Source volume '%s' is not mounted, skipping unmount.",
                volume_name,
            )
            return

        results = list(
            Volume.get_collection(
                connection=self._src_conn,
                fields="uuid",
                **{"svm.name": self._src_svm, "name": volume_name},
            )
        )
        if not results:
            raise RuntimeError(
                f"Source volume '{volume_name}' not found in SVM '{self._src_svm}'."
            )

        src_vol = results[0]
        src_vol.set_connection(self._src_conn)
        src_vol.nas = {"path": ""}
        src_vol.patch()

        logging.info(
            "Source volume '%s' unmounted (junction path removed).",
            volume_name,
        )

    def mount_destination_volume(
        self,
        volume_name: str,
        junction_path: str,
    ) -> None:
        """Set the junction path on the destination (DP) volume to mount it.

        Args:
            volume_name: Name of the source volume (destination will have
                ``DST_VOLUME_SUFFIX`` appended).
            junction_path: Namespace path to assign
                (e.g. ``/vol_sales_dst``).

        Returns:
            None

        Raises:
            RuntimeError: If the destination volume cannot be found.
        """
        dst_vol_name = f"{volume_name}{DST_VOLUME_SUFFIX}"

        results = list(
            Volume.get_collection(
                connection=self._dst_conn,
                fields="uuid",
                **{"svm.name": self._dst_svm, "name": dst_vol_name},
            )
        )
        if not results:
            raise RuntimeError(
                f"Destination volume '{dst_vol_name}' not found in SVM "
                f"'{self._dst_svm}'."
            )

        dst_vol = results[0]
        dst_vol.set_connection(self._dst_conn)
        dst_vol.nas = {"path": junction_path}
        dst_vol.patch()

        logging.info(
            "Destination volume '%s' mounted at '%s'.",
            dst_vol_name,
            junction_path,
        )

    def rename_source_volume_for_delete(self, volume_name: str) -> str:
        """Rename the source volume to a delete-marked name.

        Args:
            volume_name: Original source volume name.

        Returns:
            str: New source volume name with ``_delete`` suffix.

        Raises:
            RuntimeError: If source volume is missing or target name exists.
        """
        delete_name = f"{volume_name}_delete"
        existing_delete = list(
            Volume.get_collection(
                connection=self._src_conn,
                fields="uuid,name",
                **{"svm.name": self._src_svm, "name": delete_name},
            )
        )
        if existing_delete:
            raise RuntimeError(
                f"Cannot rename source volume '{volume_name}' because "
                f"'{delete_name}' already exists in SVM '{self._src_svm}'."
            )

        results = list(
            Volume.get_collection(
                connection=self._src_conn,
                fields="uuid,name",
                **{"svm.name": self._src_svm, "name": volume_name},
            )
        )
        if not results:
            raise RuntimeError(
                f"Source volume '{volume_name}' not found in SVM '{self._src_svm}'."
            )

        src_vol = results[0]
        src_vol.set_connection(self._src_conn)
        src_vol.name = delete_name
        src_vol.patch()

        logging.info(
            "Renamed source volume '%s' to '%s'.",
            volume_name,
            delete_name,
        )
        return delete_name

    def rename_destination_volume_to_source_name(self, volume_name: str) -> None:
        """Rename destination volume from suffixed name back to source name.

        Args:
            volume_name: Original source volume name.

        Returns:
            None

        Raises:
            RuntimeError: If destination volume is missing or name is taken.
        """
        dst_vol_name = f"{volume_name}{DST_VOLUME_SUFFIX}"
        existing_target = list(
            Volume.get_collection(
                connection=self._dst_conn,
                fields="uuid,name",
                **{"svm.name": self._dst_svm, "name": volume_name},
            )
        )
        if existing_target:
            raise RuntimeError(
                f"Cannot rename destination volume '{dst_vol_name}' because "
                f"target name '{volume_name}' already exists in "
                f"SVM '{self._dst_svm}'."
            )

        results = list(
            Volume.get_collection(
                connection=self._dst_conn,
                fields="uuid,name",
                **{"svm.name": self._dst_svm, "name": dst_vol_name},
            )
        )
        if not results:
            raise RuntimeError(
                f"Destination volume '{dst_vol_name}' not found in SVM "
                f"'{self._dst_svm}'."
            )

        dst_vol = results[0]
        dst_vol.set_connection(self._dst_conn)
        dst_vol.name = volume_name
        dst_vol.patch()

        logging.info(
            "Renamed destination volume '%s' to '%s'.",
            dst_vol_name,
            volume_name,
        )

    def offline_source_volume(self, volume_name: str) -> None:
        """Set the source volume state to offline after successful cutover.

        Args:
            volume_name: Name of the source volume to set offline.

        Returns:
            None

        Raises:
            RuntimeError: If the source volume cannot be found.
        """
        results = list(
            Volume.get_collection(
                connection=self._src_conn,
                fields="uuid,state",
                **{"svm.name": self._src_svm, "name": volume_name},
            )
        )
        if not results:
            raise RuntimeError(
                f"Source volume '{volume_name}' not found in SVM '{self._src_svm}'."
            )

        src_vol = results[0]
        src_vol.set_connection(self._src_conn)
        src_vol.state = "offline"
        src_vol.patch()

        logging.info(
            "Source volume '%s' set to offline.",
            volume_name,
        )

    # ------------------------------------------------------------------
    # Protocol recreation
    # ------------------------------------------------------------------

    def recreate_cifs_shares(
        self,
        volume_name: str,
        shares: list[ShareInfo],
    ) -> None:
        """Re-create CIFS shares on the destination volume.

        Skipped entirely when source and destination SVM are the same,
        as the existing share already references the SVM.

        Args:
            volume_name: Name of the source volume (used to filter shares).
            shares: Full list of ShareInfo instances from the cutover state.

        Returns:
            None
        """
        if self._same_svm:
            logging.info(
                "Same SVM detected — skipping CIFS share recreation for volume '%s'.",
                volume_name,
            )
            return

        dst_vol_name = f"{volume_name}{DST_VOLUME_SUFFIX}"
        volume_shares = [s for s in shares if s.volume_name == volume_name]

        for share in volume_shares:
            share_body: dict[str, object] = {
                "name": share.share_name,
                "path": share.path,
                "comment": share.comment,
                "svm": {"name": self._dst_svm},
                "volume": {"name": dst_vol_name},
            }
            if share.acls:
                share_body["acls"] = share.acls
            new_share = CifsShare.from_dict(share_body)
            new_share.set_connection(self._dst_conn)
            new_share.post()
            logging.info(
                "Re-created CIFS share '%s' on volume '%s'.",
                share.share_name,
                dst_vol_name,
            )

    # noinspection PyMethodMayBeStatic
    def _build_nfs_rule_body(self, rule: NfsRuleInfo) -> dict[str, object]:
        """Build a postable export rule body from a stored NFS rule object.

        Args:
            rule: Stored NfsRuleInfo to convert.

        Returns:
            dict[str, object]: Rule body compatible with ExportPolicy.post.
        """
        rule_body: dict[str, object] = {
            "clients": rule.clients,
            "protocols": rule.protocols,
            "ro_rule": rule.ro_rule,
            "rw_rule": rule.rw_rule,
            "superuser": rule.superuser,
        }
        if rule.index is not None:
            rule_body["index"] = rule.index
        if rule.anonymous_user is not None:
            rule_body["anonymous_user"] = rule.anonymous_user
        if rule.allow_suid is not None:
            rule_body["allow_suid"] = rule.allow_suid
        if rule.allow_device_creation is not None:
            rule_body["allow_device_creation"] = rule.allow_device_creation
        if rule.chown_mode is not None:
            rule_body["chown_mode"] = rule.chown_mode
        if rule.ntfs_unix_security is not None:
            rule_body["ntfs_unix_security"] = rule.ntfs_unix_security
        return rule_body

    def _policy_exists_on_destination(self, policy_name: str) -> bool:
        """Return whether an NFS export policy already exists on destination.

        Args:
            policy_name: Policy name to check on destination SVM.

        Returns:
            bool: True if policy exists, otherwise False.
        """
        existing = list(
            ExportPolicy.get_collection(
                connection=self._dst_conn,
                fields="id,name",
                **{"svm.name": self._dst_svm, "name": policy_name},
            )
        )
        return bool(existing)

    def _create_destination_nfs_policy(self, policy: NfsPolicyInfo) -> None:
        """Create one destination NFS policy with all rules in one post call.

        Args:
            policy: Policy and rule payload to create on destination SVM.

        Returns:
            None
        """
        policy_body = {
            "name": policy.destination_policy_name,
            "svm": {"name": self._dst_svm},
            "rules": [self._build_nfs_rule_body(rule) for rule in policy.rules],
        }
        dst_policy = ExportPolicy.from_dict(policy_body)
        dst_policy.set_connection(self._dst_conn)
        dst_policy.post()
        logging.info(
            "Created destination NFS export policy '%s' with %d rule(s).",
            policy.destination_policy_name,
            len(policy.rules),
        )

    def ensure_destination_nfs_policies(
        self,
        nfs_policies: list[NfsPolicyInfo] | None,
    ) -> dict[str, str]:
        """Ensure required NFS export policies exist on destination SVM.

        Policies are created via ExportPolicy.post including all rules.
        Existing destination policies are skipped and logged as warnings.

        Args:
            nfs_policies: Policy definitions loaded from cutover state.

        Returns:
            dict[str, str]: Mapping source policy name -> destination policy
                name for downstream volume assignment.
        """
        if self._same_svm or not nfs_policies:
            return {}

        policy_map: dict[str, str] = {}
        for policy in nfs_policies:
            policy_map[policy.source_policy_name] = policy.destination_policy_name
            if self._policy_exists_on_destination(policy.destination_policy_name):
                logging.warning(
                    "Destination NFS export policy '%s' already exists in "
                    "SVM '%s'; skipping create.",
                    policy.destination_policy_name,
                    self._dst_svm,
                )
                continue
            self._create_destination_nfs_policy(policy)

        return policy_map

    def _ensure_nfs_policy_sync_once(
        self,
        nfs_policies: list[NfsPolicyInfo] | None,
    ) -> dict[str, str]:
        """Synchronize NFS policies once per executor lifecycle.

        Args:
            nfs_policies: Policy definitions loaded from cutover state.

        Returns:
            dict[str, str]: Source-to-destination policy name mapping.
        """
        if not self._nfs_policy_sync_done:
            self._nfs_policy_map = self.ensure_destination_nfs_policies(nfs_policies)
            self._nfs_policy_sync_done = True
        return self._nfs_policy_map

    # noinspection PyMethodMayBeStatic
    def _policy_has_rules(
        self,
        policy_name: str,
        nfs_policies: list[NfsPolicyInfo] | None,
    ) -> bool | None:
        """Return whether a named NFS policy contains any export rules.

        Args:
            policy_name: Source policy name to inspect.
            nfs_policies: Optional NFS policy definitions from state.

        Returns:
            bool | None: True if rules exist, False if policy exists but has
                no rules, or None if no policy definition is available.
        """
        if not nfs_policies:
            return None

        for policy in nfs_policies:
            if policy.source_policy_name == policy_name:
                return bool(policy.rules)

        return None

    def recreate_nfs_exports(
        self,
        volume_name: str,
        exports: list[ExportInfo],
        nfs_policies: list[NfsPolicyInfo] | None = None,
    ) -> None:
        """Assign the original NFS export policy to the destination volume.

        Skipped entirely when source and destination SVM are the same,
        as the volume reassignment is sufficient after the remount.

        Args:
            volume_name: Name of the source volume (used to filter exports).
            exports: Full list of ExportInfo instances from the cutover state.
            nfs_policies: Optional NFS policy/rule definitions used to map
                source policy names to destination policy names.

        Returns:
            None
        """
        if self._same_svm:
            logging.info(
                "Same SVM detected - skipping NFS export recreation for volume '%s'.",
                volume_name,
            )
            return

        policy_map = self._ensure_nfs_policy_sync_once(nfs_policies)

        dst_vol_name = f"{volume_name}{DST_VOLUME_SUFFIX}"
        volume_exports = [e for e in exports if e.volume_name == volume_name]

        for export in volume_exports:
            has_rules = self._policy_has_rules(export.policy_name, nfs_policies)
            if has_rules is False:
                logging.info(
                    "Skipping NFS export policy reassign for volume '%s' "
                    "because source policy '%s' has no rules.",
                    dst_vol_name,
                    export.policy_name,
                )
                continue

            destination_policy_name = policy_map.get(
                export.policy_name,
                export.policy_name,
            )
            results = list(
                Volume.get_collection(
                    connection=self._dst_conn,
                    fields="uuid",
                    **{"svm.name": self._dst_svm, "name": dst_vol_name},
                )
            )
            if not results:
                logging.warning(
                    "Destination volume '%s' not found, cannot assign "
                    "export policy '%s'.",
                    dst_vol_name,
                    destination_policy_name,
                )
                continue

            dst_vol = results[0]
            dst_vol.set_connection(self._dst_conn)
            dst_vol.nas = {"export_policy": {"name": destination_policy_name}}
            dst_vol.patch()
            logging.info(
                "Assigned export policy '%s' to volume '%s'.",
                destination_policy_name,
                dst_vol_name,
            )

    # ------------------------------------------------------------------
    # Full cutover sequence
    # ------------------------------------------------------------------

    def execute(
        self,
        volume_name: str,
        junction_path: str,
        shares: list[ShareInfo],
        exports: list[ExportInfo],
        protocol: str,
        nfs_policies: list[NfsPolicyInfo] | None = None,
    ) -> None:
        """Run the complete cutover sequence for a single volume.

        Skips the volume if it is already present in the ``migrated_volumes``
        list of the cutover state file. On successful completion, the volume
        name is appended to ``migrated_volumes`` so that re-runs skip it.

        Steps executed in order:
          1. Guard — skip if already migrated.
          2. Run final SnapMirror update (blocking).
          3. Break SnapMirror relationship (state: broken_off).
          4. Unmount source volume.
          5. Mount destination volume at the original junction path.
          6. Re-create CIFS shares or NFS exports (skipped for same SVM).
          7. Rename source volume to ``<name>_delete``.
          8. Set renamed source volume state to offline.
          9. Rename destination volume back to original source name.
          10. Persist migration progress to state file.

        Args:
            volume_name: Name of the source volume to cut over.
            junction_path: Namespace path to assign to the destination
                volume after cutover.
            shares: CIFS share list from the cutover state (may be empty).
            exports: NFS export list from the cutover state (may be empty).
            protocol: One of ``cifs``, ``nfs``, or ``both``.
            nfs_policies: Optional NFS policy/rule list loaded from state.

        Returns:
            None
        """
        if self._state_path is not None and self._state_path.exists():
            state = load_cutover_state(self._state_path)
            already_done: list[str] = list(
                state.get("migrated_volumes", [])  # type: ignore[arg-type]
            )
            if volume_name in already_done:
                logging.warning(
                    "Volume '%s' is already marked as migrated — skipping.",
                    volume_name,
                )
                return

        logging.info("--- Starting cutover for volume '%s' ---", volume_name)

        self.update_snapmirror(volume_name)
        self.break_snapmirror(volume_name)
        self.unmount_source_volume(volume_name)
        self.mount_destination_volume(volume_name, junction_path)

        if protocol in ("cifs", "both"):
            self.recreate_cifs_shares(volume_name, shares)
        if protocol in ("nfs", "both"):
            self.recreate_nfs_exports(
                volume_name,
                exports,
                nfs_policies,
            )

        renamed_source_volume = self.rename_source_volume_for_delete(volume_name)
        self.offline_source_volume(renamed_source_volume)
        self.rename_destination_volume_to_source_name(volume_name)

        logging.info("--- Cutover complete for volume '%s' ---", volume_name)

        if self._state_path is not None:
            mark_volume_migrated(volume_name, self._state_path)
