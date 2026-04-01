"""migrate — ONTAP SnapMirror migration package.

Exposes the public API of the snapmirror and cutover modules for use
by the ontap_migrate entry point.
"""

from migrate.cutover import (
    CUTOVER_STATE_FILENAME,
    CutoverExecutor,
    CutoverStateMap,
    ExportInfo,
    ShareInfo,
    collect_cifs_shares,
    collect_nfs_exports,
    load_cutover_state,
    mark_volume_migrated,
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

__all__ = [
    # cutover
    "CUTOVER_STATE_FILENAME",
    "CutoverExecutor",
    "CutoverStateMap",
    "ExportInfo",
    "ShareInfo",
    "collect_cifs_shares",
    "collect_nfs_exports",
    "load_cutover_state",
    "mark_volume_migrated",
    "write_cutover_state",
    # snapmirror
    "DST_SVM_SUFFIX",
    "DST_VOLUME_SUFFIX",
    "ENV_DST_PASSWORD_VAR",
    "ENV_SRC_PASSWORD_VAR",
    "ReplicationContext",
    "_resolve_password",
    "create_connection",
    "create_snapmirror_relationships",
    "ensure_destination_svm",
    "ensure_svm_peer",
    "get_cluster_name",
    "get_source_volumes",
    "setup_logging",
    "validate_source_svm_exists",
]
