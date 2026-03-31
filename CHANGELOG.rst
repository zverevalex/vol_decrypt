CHANGELOG
=========

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog,
and this project follows Semantic Versioning.


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
