CHANGELOG
=========

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog,
and this project follows Semantic Versioning.


1.1.0 - 2026-06-23
------------------

Added
~~~~~

- ``vol_schedule.py`` — Interactive migration planner script for bulk volume
  move operations. Discovers all RW, online volumes in a specified SVM, renders
  per-volume aggregate selection UI, and writes YAML plan files to
  ``plans/<cluster_name>_<svm>.yaml``.
  
  - Supports ``--cluster``, ``--username``, ``--svm`` (required args)
  - ``--password`` falls back to ``$ONTAP_PASSWORD`` env var
  - Skips root volumes and volumes already in-flight
  - Prints numbered volume table and per-volume aggregate tables
  - Interactive prompts for volume selection and aggregate assignment
  - Writes timestamped logs to ``logs/vol_schedule_<YYYYMMDD_HHMMSS>.log``
  - Supports ``--dry-run``, ``--verify-ssl``, ``--log-dir``, ``--output``

- ``vol_move_exec.py`` — Scheduled migration executor script for YAML plan
  execution. Reads plan file(s) and orchestrates volume move operations with
  concurrency limits and idempotent status tracking.
  
  - Reads single plan file (``--plan <file> --cluster <host>``) or
    directory of plans (``--plans-dir <dir>``, defaults to ``./plans/``)
  - Per-run status refresh: queries ONTAP ``movement.state`` and updates
    ``in_progress`` statuses
  - Starts ``pending`` moves up to ``--max-concurrent`` slots (default 6)
  - Status lifecycle: ``pending`` → ``in_progress`` → ``done`` / ``failed``
  - Writes updated YAML back after each run
  - Per-plan + combined summary for multi-cluster runs
  - Idempotent design: safe to run as cron job with ``flock``
  - Writes timestamped logs to ``logs/vol_move_exec_<YYYYMMDD_HHMMSS>.log``
  - Supports ``--dry-run``, ``--verify-ssl``, ``--log-dir``, ``--max-concurrent``

- YAML plan schema for ``vol_schedule.py`` output and ``vol_move_exec.py``
  input:
  
  .. code-block:: yaml
  
    cluster: "bahamas.muccbc.hq.netapp.com"
    svm: "azvsvmmgt002"
    volumes:
      - name: "vol_data_01"
        uuid: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        target_aggregate: "aggr1_node1"
        status: "pending"   # pending | in_progress | done | failed
        error: null

- Dependency: ``PyYAML==6.0.2`` added to ``requirements.txt`` for plan
  serialization/deserialization.


1.0.2 - 2026-04-01
------------------

Changed
~~~~~~~

- Restored missing NFS policy helper methods in ``CutoverExecutor``
  (``ensure_destination_nfs_policies``,
  ``_ensure_nfs_policy_sync_once``, ``_policy_exists_on_destination``,
  ``_create_destination_nfs_policy``, ``_policy_has_rules``,
  ``recreate_nfs_exports``) to keep cutover flow and tests consistent.
- Strengthened typing in ``migrate/snapmirror.py`` for relationship-body
  construction and debug logging path extraction.
- Bumped CLI/app version in ``ontap_migrate.py`` to ``1.0.2``.


1.0.1 - 2026-03-31
------------------

Changed
~~~~~~~

- Refactored CLI argument processing into focused helpers
  (``_build_parser``, ``_resolve_credentials``, ``_apply_cli_defaults``)
  to reduce complexity in ``parse_args``.
- Refactored cutover orchestration in ``OntapMigrate`` into dedicated
  helper methods for improved readability and testability.
- Consolidated cutover summary output to the logging channel for
  consistent console and optional file-log behavior.
- Decoupled password resolution in ``ontap_migrate.py`` from private
  ``migrate.snapmirror`` internals by introducing ``resolve_password``.


1.0.0 - 2026-03-31
------------------

Added
~~~~~

- Semi-automatic migration CLI with subcommands:
  ``replicate``, ``collect``, ``cutover``.
- SnapMirror orchestration with explicit DP destination volume creation
  on unencrypted aggregates.
- Source-to-destination volume property inheritance for
  ``size``, ``language``, and ``security_style``.
- SVM validation and peering guardrails (including same-SVM skip logic).
- Protocol-state collection into ``cutover_state.json`` for CIFS and NFS.
- CIFS ACL migration support for cross-SVM cutover.
- NFS export policy and rule migration support for cross-SVM cutover.
- Per-volume migration progress tracking via ``migrated_volumes``.
- Primary cutover scope via explicit ``volume_names`` with
  backward-compatible fallback.
- Optional file logging via ``--log-file`` (console logging remains enabled).
- CLI version output via ``--version``.

Changed
~~~~~~~

- Cutover flow now performs final SnapMirror update (blocking),
  breaks relationship, remounts destination, renames source to
  ``<name>_delete``, sets source offline, and renames destination
  ``<name>_dst`` back to ``<name>``.
- Cutover summary output is logged consistently through the logging channel.

Fixed
~~~~~

- Cutover skips NFS export-policy reassign when the source policy
  has no rules.
- Optional file logging is idempotent and avoids duplicate handlers
  for the same log path.
