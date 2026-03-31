#!/usr/bin/env python3
"""smoke_test.py — Import and logic smoke tests for ontap_migrate.

Validates that all three modules import cleanly, that pure-logic
functions behave correctly, and that ONTAP SDK call sites are wired
up as expected — without requiring a live ONTAP cluster.

All SDK network calls are replaced by ``unittest.mock`` stubs so the
suite can run in any CI or developer environment.

Run from the project root:
    uv run python tests/smoke_test.py
    # or as module:
    uv run python -m tests.smoke_test
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure the project root is on sys.path when the file is run directly
# (i.e. `python tests/smoke_test.py` from the project root).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Module import checks
# ---------------------------------------------------------------------------


class TestImports(unittest.TestCase):
    """Verify that all project modules import without errors."""

    def test_snapmirror_imports(self) -> None:
        """migrate.snapmirror module must be importable."""
        import migrate.snapmirror  # noqa: F401

    def test_cutover_imports(self) -> None:
        """migrate.cutover module must be importable."""
        import migrate.cutover  # noqa: F401

    def test_ontap_migrate_imports(self) -> None:
        """ontap_migrate module must be importable."""
        import ontap_migrate  # noqa: F401


# ---------------------------------------------------------------------------
# snapmirror — pure logic
# ---------------------------------------------------------------------------


class TestSelectAggregate(unittest.TestCase):
    """Unit tests for migrate.snapmirror.select_aggregate()."""

    def setUp(self) -> None:
        from migrate.snapmirror import AggregateInfo

        self.AggregateInfo = AggregateInfo

    def test_single_candidate_auto_selected(self) -> None:
        """Single aggregate must be returned without user input."""
        from migrate.snapmirror import select_aggregate

        candidates = [self.AggregateInfo(name="aggr1", uuid="uuid-1")]
        result = select_aggregate(candidates)
        self.assertEqual(result.name, "aggr1")

    def test_empty_list_raises(self) -> None:
        """Empty candidate list must raise ValueError."""
        from migrate.snapmirror import select_aggregate

        with self.assertRaises(ValueError):
            select_aggregate([])

    def test_multiple_candidates_prompt(self) -> None:
        """Multiple candidates must trigger an input prompt."""
        from migrate.snapmirror import select_aggregate

        candidates = [
            self.AggregateInfo(name="aggr1", uuid="uuid-1"),
            self.AggregateInfo(name="aggr2", uuid="uuid-2"),
        ]
        # Simulate user entering "2"
        with patch("builtins.input", return_value="2"):
            result = select_aggregate(candidates)
        self.assertEqual(result.name, "aggr2")

    def test_invalid_then_valid_input(self) -> None:
        """Invalid input must loop until a valid selection is given."""
        from migrate.snapmirror import select_aggregate

        candidates = [
            self.AggregateInfo(name="aggr1", uuid="uuid-1"),
            self.AggregateInfo(name="aggr2", uuid="uuid-2"),
        ]
        with patch("builtins.input", side_effect=["0", "abc", "1"]):
            result = select_aggregate(candidates)
        self.assertEqual(result.name, "aggr1")


class TestBuildRelationshipBody(unittest.TestCase):
    """Unit tests for migrate.snapmirror.build_relationship_body()."""

    def _make_ctx(self) -> object:
        from migrate.snapmirror import ReplicationContext

        return ReplicationContext(
            src_cluster_name="src-cluster",
            src_svm_name="vs_prod",
            dst_svm_name="vs_prod_dst",
            dst_connection=MagicMock(),
        )

    def test_source_path_correct(self) -> None:
        """Source path must use the original volume name."""
        from migrate.snapmirror import build_relationship_body

        body = build_relationship_body(self._make_ctx(), "vol_sales")
        self.assertEqual(
            body["source"]["path"],  # type: ignore[index]
            "vs_prod:vol_sales",
        )

    def test_destination_path_has_suffix(self) -> None:
        """Destination path must append DST_VOLUME_SUFFIX."""
        from migrate.snapmirror import DST_VOLUME_SUFFIX, build_relationship_body

        body = build_relationship_body(self._make_ctx(), "vol_sales")
        self.assertEqual(
            body["destination"]["path"],  # type: ignore[index]
            f"vs_prod_dst:vol_sales{DST_VOLUME_SUFFIX}",
        )

    def test_no_create_destination_key(self) -> None:
        """create_destination must not appear in the request body."""
        from migrate.snapmirror import build_relationship_body

        body = build_relationship_body(self._make_ctx(), "vol_sales")
        self.assertNotIn("create_destination", body)

    def test_state_not_in_post_body(self) -> None:
        """state must not appear in the POST body (set via PATCH instead)."""
        from migrate.snapmirror import build_relationship_body

        body = build_relationship_body(self._make_ctx(), "vol_sales")
        self.assertNotIn("state", body)

    def test_policy_is_mirror_all_snapshots(self) -> None:
        """Policy must be MirrorAllSnapshots."""
        from migrate.snapmirror import DEFAULT_POLICY, build_relationship_body

        body = build_relationship_body(self._make_ctx(), "vol_sales")
        self.assertEqual(
            body["policy"]["name"],  # type: ignore[index]
            DEFAULT_POLICY,
        )

    def test_src_cluster_in_source(self) -> None:
        """Source cluster name must be present in the source block."""
        from migrate.snapmirror import build_relationship_body

        body = build_relationship_body(self._make_ctx(), "vol_sales")
        self.assertEqual(
            body["source"]["cluster"]["name"],  # type: ignore[index]
            "src-cluster",
        )


class TestResolvePassword(unittest.TestCase):
    """Unit tests for migrate.snapmirror._resolve_password()."""

    def test_explicit_value_returned(self) -> None:
        """Explicit password must be returned as-is."""
        from migrate.snapmirror import _resolve_password

        result = _resolve_password(
            explicit="hunter2",
            env_var="UNUSED_VAR",
            prompt_label="unused",
        )
        self.assertEqual(result, "hunter2")

    def test_env_var_used_when_no_explicit(self) -> None:
        """Environment variable must be used when explicit is None."""
        from migrate.snapmirror import _resolve_password

        with patch.dict("os.environ", {"TEST_PW_VAR": "fromenv"}):
            result = _resolve_password(
                explicit=None,
                env_var="TEST_PW_VAR",
                prompt_label="unused",
            )
        self.assertEqual(result, "fromenv")

    def test_getpass_called_as_fallback(self) -> None:
        """getpass must be called when no explicit value or env var exists."""
        from migrate.snapmirror import _resolve_password

        with patch("migrate.snapmirror.getpass.getpass", return_value="prompted"):
            result = _resolve_password(
                explicit=None,
                env_var="NONEXISTENT_VAR_XYZ",
                prompt_label="Enter password",
            )
        self.assertEqual(result, "prompted")


# ---------------------------------------------------------------------------
# snapmirror — SDK call sites (mocked)
# ---------------------------------------------------------------------------


class TestValidateSourceSvmExists(unittest.TestCase):
    """Unit tests for migrate.snapmirror.validate_source_svm_exists()."""

    def test_count_1_passes(self) -> None:
        """count_collection returning 1 must not raise."""
        from migrate.snapmirror import validate_source_svm_exists

        with patch("migrate.snapmirror.Svm.count_collection", return_value=1):
            validate_source_svm_exists("vs_prod", MagicMock())

    def test_count_0_raises(self) -> None:
        """count_collection returning 0 must raise RuntimeError."""
        from migrate.snapmirror import validate_source_svm_exists

        with patch("migrate.snapmirror.Svm.count_collection", return_value=0):
            with self.assertRaises(RuntimeError):
                validate_source_svm_exists("vs_ghost", MagicMock())

    def test_invalid_count_raises(self) -> None:
        """count_collection returning >1 must raise RuntimeError."""
        from migrate.snapmirror import validate_source_svm_exists

        with patch("migrate.snapmirror.Svm.count_collection", return_value=5):
            with self.assertRaises(RuntimeError):
                validate_source_svm_exists("vs_ambiguous", MagicMock())


class TestGetUnencryptedAggregates(unittest.TestCase):
    """Unit tests for migrate.snapmirror.get_unencrypted_aggregates()."""

    def _make_agg(self, name: str, encrypted: bool) -> MagicMock:
        agg = MagicMock()
        agg.name = name
        agg.uuid = f"uuid-{name}"
        agg.data_encryption.software_encryption_enabled = encrypted
        return agg

    def test_filters_encrypted_aggregates(self) -> None:
        """Encrypted aggregates must be excluded from results."""
        from migrate.snapmirror import get_unencrypted_aggregates

        mock_aggs = [
            self._make_agg("aggr_plain", False),
            self._make_agg("aggr_enc", True),
        ]
        with patch(
            "migrate.snapmirror.Aggregate.get_collection",
            return_value=iter(mock_aggs),
        ):
            result = get_unencrypted_aggregates(MagicMock())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "aggr_plain")

    def test_no_unencrypted_raises(self) -> None:
        """RuntimeError must be raised when no unencrypted aggregate exists."""
        from migrate.snapmirror import get_unencrypted_aggregates

        mock_aggs = [self._make_agg("aggr_enc", True)]
        with patch(
            "migrate.snapmirror.Aggregate.get_collection",
            return_value=iter(mock_aggs),
        ):
            with self.assertRaises(RuntimeError):
                get_unencrypted_aggregates(MagicMock())


class TestGetSourceVolumes(unittest.TestCase):
    """Unit tests for migrate.snapmirror.get_source_volumes()."""

    def _make_volume(self, name: str, security_style: str | None) -> MagicMock:
        vol = MagicMock()
        vol.uuid = f"uuid-{name}"
        vol.name = name
        vol.size = 1024
        vol.language = "c.utf_8"
        vol.nas.security_style = security_style
        return vol

    def test_reads_security_style_from_source_volume(self) -> None:
        """get_source_volumes must include nas.security_style in VolumeInfo."""
        from migrate.snapmirror import get_source_volumes

        source_volumes = [self._make_volume("vol_fin", "ntfs")]
        with patch(
            "migrate.snapmirror.Volume.get_collection",
            return_value=iter(source_volumes),
        ):
            result = get_source_volumes(
                svm_name="vs_src",
                exclude=[],
                connection=MagicMock(),
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].security_style, "ntfs")


class TestCreateDpVolume(unittest.TestCase):
    """Unit tests for migrate.snapmirror.create_dp_volume()."""

    def _make_volume_info(self, security_style: str | None) -> object:
        from migrate.snapmirror import VolumeInfo

        return VolumeInfo(
            name="vol_fin",
            uuid="uuid-vol_fin",
            svm_name="vs_src",
            size=1024,
            language="c.utf_8",
            security_style=security_style,
        )

    def test_includes_security_style_when_present(self) -> None:
        """create_dp_volume must set nas.security_style when available."""
        from migrate.snapmirror import AggregateInfo, create_dp_volume

        aggregate = AggregateInfo(name="aggr1", uuid="uuid-aggr1")
        dp_volume = MagicMock()

        with (
            patch(
                "migrate.snapmirror.Volume.get_collection",
                return_value=iter([]),
            ),
            patch(
                "migrate.snapmirror.Volume.from_dict",
                return_value=dp_volume,
            ) as from_dict_mock,
        ):
            create_dp_volume(
                vol=self._make_volume_info("ntfs"),
                dst_svm_name="vs_dst",
                aggregate=aggregate,
                connection=MagicMock(),
            )

        volume_body = from_dict_mock.call_args.args[0]
        self.assertEqual(
            volume_body["nas"]["security_style"],
            "ntfs",
        )

    def test_omits_security_style_when_missing(self) -> None:
        """create_dp_volume must not set nas when style is missing."""
        from migrate.snapmirror import AggregateInfo, create_dp_volume

        aggregate = AggregateInfo(name="aggr1", uuid="uuid-aggr1")
        dp_volume = MagicMock()

        with (
            patch(
                "migrate.snapmirror.Volume.get_collection",
                return_value=iter([]),
            ),
            patch(
                "migrate.snapmirror.Volume.from_dict",
                return_value=dp_volume,
            ) as from_dict_mock,
        ):
            create_dp_volume(
                vol=self._make_volume_info(None),
                dst_svm_name="vs_dst",
                aggregate=aggregate,
                connection=MagicMock(),
            )

        volume_body = from_dict_mock.call_args.args[0]
        self.assertNotIn("nas", volume_body)


# ---------------------------------------------------------------------------
# cutover — pure logic
# ---------------------------------------------------------------------------


class TestCutoverStatePersistence(unittest.TestCase):
    """Unit tests for cutover state JSON serialisation round-trip."""

    def setUp(self) -> None:
        from migrate.cutover import ExportInfo, ShareInfo

        self.tmp = Path("/tmp/smoke_cutover_state.json")
        self.shares = [
            ShareInfo(
                share_name="finance$",
                volume_name="vol_finance",
                path="/",
                comment="Finance share",
                acls=[],
            )
        ]
        self.exports = [
            ExportInfo(
                policy_name="default",
                volume_name="vol_data",
            )
        ]

    def tearDown(self) -> None:
        if self.tmp.exists():
            self.tmp.unlink()

    def test_write_and_load_round_trip(self) -> None:
        """State written by write_cutover_state must be loadable unchanged."""
        from migrate.cutover import load_cutover_state, write_cutover_state

        write_cutover_state(
            src_svm="vs_prod",
            dst_svm="vs_prod_dst",
            shares=self.shares,
            exports=self.exports,
            nfs_policies=[],
            state_path=self.tmp,
        )
        state = load_cutover_state(self.tmp)

        self.assertEqual(state["src_svm"], "vs_prod")
        self.assertEqual(state["dst_svm"], "vs_prod_dst")
        self.assertEqual(len(state["cifs_shares"]), 1)  # type: ignore[arg-type]
        self.assertEqual(len(state["nfs_exports"]), 1)  # type: ignore[arg-type]
        share = state["cifs_shares"][0]  # type: ignore[index]
        self.assertEqual(share["share_name"], "finance$")

    def test_load_missing_file_raises(self) -> None:
        """load_cutover_state must raise FileNotFoundError for missing file."""
        from migrate.cutover import load_cutover_state

        with self.assertRaises(FileNotFoundError):
            load_cutover_state(Path("/tmp/does_not_exist_xyz.json"))

    def test_load_invalid_json_raises(self) -> None:
        """load_cutover_state must raise ValueError for missing required keys."""
        self.tmp.write_text('{"src_svm": "vs_prod"}', encoding="utf-8")
        from migrate.cutover import load_cutover_state

        with self.assertRaises(ValueError):
            load_cutover_state(self.tmp)


class TestCollectCifsShares(unittest.TestCase):
    """Unit tests for migrate.cutover.collect_cifs_shares()."""

    def _make_share(self, name: str, vol_name: str) -> MagicMock:
        share = MagicMock()
        share.name = name
        share.volume.name = vol_name
        share.path = "/"
        share.comment = ""
        acl = MagicMock()
        acl.user_or_group = "DOMAIN\\user1"
        acl.permission = "full_control"
        acl.type = "windows"
        share.acls = [acl]
        return share

    def test_filters_to_requested_volumes(self) -> None:
        """Only shares for volumes in volume_names must be returned."""
        from migrate.cutover import collect_cifs_shares

        mock_shares = [
            self._make_share("share_finance", "vol_finance"),
            self._make_share("share_hr", "vol_hr"),
        ]
        with patch(
            "migrate.cutover.CifsShare.get_collection",
            return_value=iter(mock_shares),
        ):
            result = collect_cifs_shares(
                svm_name="vs_prod",
                volume_names=["vol_finance"],
                connection=MagicMock(),
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].share_name, "share_finance")
        self.assertEqual(result[0].acls[0]["user_or_group"], "DOMAIN\\user1")

    def test_empty_result_when_no_match(self) -> None:
        """Empty list must be returned when no share matches the volumes."""
        from migrate.cutover import collect_cifs_shares

        mock_shares = [self._make_share("share_other", "vol_other")]
        with patch(
            "migrate.cutover.CifsShare.get_collection",
            return_value=iter(mock_shares),
        ):
            result = collect_cifs_shares(
                svm_name="vs_prod",
                volume_names=["vol_finance"],
                connection=MagicMock(),
            )

        self.assertEqual(result, [])


class TestCollectNfsPolicies(unittest.TestCase):
    """Unit tests for migrate.cutover.collect_nfs_policies()."""

    def test_collects_policy_with_rules(self) -> None:
        """collect_nfs_policies must include rule fields in output."""
        from migrate.cutover import ExportInfo, collect_nfs_policies

        policy_obj = MagicMock()
        policy_obj.id = 123

        rule_obj = MagicMock()
        client_obj = MagicMock()
        client_obj.match = "10.0.0.0/24"
        rule_obj.clients = [client_obj]
        rule_obj.protocols = ["nfs3", "nfs4"]
        rule_obj.ro_rule = ["sys"]
        rule_obj.rw_rule = ["sys"]
        rule_obj.superuser = ["sys"]
        rule_obj.anonymous_user = "65534"
        rule_obj.allow_suid = True
        rule_obj.allow_device_creation = False
        rule_obj.chown_mode = "restricted"
        rule_obj.ntfs_unix_security = "fail"
        rule_obj.index = 1

        exports = [ExportInfo(policy_name="data_pol", volume_name="vol1")]
        with (
            patch(
                "migrate.cutover.ExportPolicy.get_collection",
                return_value=iter([policy_obj]),
            ),
            patch(
                "migrate.cutover.ExportRule.get_collection",
                return_value=iter([rule_obj]),
            ),
        ):
            result = collect_nfs_policies(
                svm_name="vs_src",
                exports=exports,
                connection=MagicMock(),
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].source_policy_name, "data_pol")
        self.assertEqual(result[0].destination_policy_name, "data_pol")
        self.assertEqual(len(result[0].rules), 1)
        self.assertEqual(result[0].rules[0].clients[0]["match"], "10.0.0.0/24")

    def test_returns_empty_when_no_exports(self) -> None:
        """collect_nfs_policies must return [] when no exports are provided."""
        from migrate.cutover import collect_nfs_policies

        result = collect_nfs_policies(
            svm_name="vs_src",
            exports=[],
            connection=MagicMock(),
        )
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# cutover — CutoverExecutor same-SVM logic
# ---------------------------------------------------------------------------


class TestCutoverExecutorSameSvm(unittest.TestCase):
    """Verify same-SVM detection skips share/export recreation."""

    def _make_executor(self) -> object:
        from migrate.cutover import CutoverExecutor

        return CutoverExecutor(
            src_svm="vs_prod",
            dst_svm="vs_prod",  # same SVM
            src_connection=MagicMock(),
            dst_connection=MagicMock(),
        )

    def test_cifs_recreation_skipped_for_same_svm(self) -> None:
        """recreate_cifs_shares must skip post() when SVMs are identical."""
        from migrate.cutover import ShareInfo

        executor = self._make_executor()
        shares = [
            ShareInfo(
                share_name="test$",
                volume_name="vol_sales",
                path="/",
                comment="",
                acls=[],
            )
        ]
        with patch("migrate.cutover.CifsShare") as mock_cifs:
            executor.recreate_cifs_shares("vol_sales", shares)
            mock_cifs.from_dict.assert_not_called()

    def test_nfs_recreation_skipped_for_same_svm(self) -> None:
        """recreate_nfs_exports must skip patch() when SVMs are identical."""
        from migrate.cutover import ExportInfo

        executor = self._make_executor()
        exports = [ExportInfo(policy_name="default", volume_name="vol_data")]
        with patch("migrate.cutover.Volume") as mock_vol:
            executor.recreate_nfs_exports("vol_data", exports)
            mock_vol.get_collection.assert_not_called()


class TestCutoverExecutorNfsPolicyMigration(unittest.TestCase):
    """Unit tests for NFS policy migration behavior in CutoverExecutor."""

    def _make_executor(self) -> object:
        from migrate.cutover import CutoverExecutor

        return CutoverExecutor(
            src_svm="vs_src",
            dst_svm="vs_dst",
            src_connection=MagicMock(),
            dst_connection=MagicMock(),
        )

    def test_skip_existing_destination_policy(self) -> None:
        """Existing destination policy must be skipped with mapping retained."""
        from migrate.cutover import NfsPolicyInfo

        executor = self._make_executor()
        policy = NfsPolicyInfo(
            source_policy_name="data_pol",
            destination_policy_name="data_pol",
            rules=[],
        )
        with (
            patch.object(
                executor,
                "_policy_exists_on_destination",
                return_value=True,
            ),
            patch.object(executor, "_create_destination_nfs_policy") as create_mock,
        ):
            policy_map = executor.ensure_destination_nfs_policies([policy])

        self.assertEqual(policy_map["data_pol"], "data_pol")
        create_mock.assert_not_called()

    def test_recreate_nfs_exports_uses_destination_policy_mapping(self) -> None:
        """Volume patch must use mapped destination policy name."""
        from migrate.cutover import ExportInfo

        executor = self._make_executor()
        export_info = ExportInfo(policy_name="src_pol", volume_name="vol_data")

        dst_vol = MagicMock()
        with (
            patch.object(
                executor,
                "_ensure_nfs_policy_sync_once",
                return_value={"src_pol": "dst_pol"},
            ),
            patch(
                "migrate.cutover.Volume.get_collection",
                return_value=iter([dst_vol]),
            ),
        ):
            executor.recreate_nfs_exports(
                volume_name="vol_data",
                exports=[export_info],
                nfs_policies=[],
            )

        self.assertEqual(dst_vol.nas, {"export_policy": {"name": "dst_pol"}})
        dst_vol.patch.assert_called_once()


class TestCutoverExecutorSnapmirrorUpdate(unittest.TestCase):
    """Unit tests for final blocking SnapMirror update behavior."""

    def _make_executor(self) -> object:
        from migrate.cutover import CutoverExecutor

        return CutoverExecutor(
            src_svm="vs_src",
            dst_svm="vs_dst",
            src_connection=MagicMock(),
            dst_connection=MagicMock(),
        )

    def test_update_snapmirror_uses_blocking_poll(self) -> None:
        """update_snapmirror must call patch with poll and poll_interval."""
        executor = self._make_executor()
        rel = MagicMock()

        with patch(
            "migrate.cutover.SnapmirrorRelationship.get_collection",
            return_value=iter([rel]),
        ):
            executor.update_snapmirror("vol_sales")

        rel.patch.assert_called_once_with(poll=True, poll_interval=10)

    def test_execute_calls_update_before_break(self) -> None:
        """execute must call update, break, rename/offline, then dst rename."""
        from migrate.cutover import ShareInfo

        executor = self._make_executor()
        call_order: list[str] = []

        def _mark_update(volume_name: str) -> None:
            call_order.append(f"update:{volume_name}")

        def _mark_break(volume_name: str) -> None:
            call_order.append(f"break:{volume_name}")

        def _mark_rename_source(volume_name: str) -> str:
            call_order.append(f"rename_src:{volume_name}")
            return f"{volume_name}_delete"

        def _mark_offline(volume_name: str) -> None:
            call_order.append(f"offline:{volume_name}")

        def _mark_rename_destination(volume_name: str) -> None:
            call_order.append(f"rename_dst:{volume_name}")

        with (
            patch.object(executor, "update_snapmirror", side_effect=_mark_update),
            patch.object(executor, "break_snapmirror", side_effect=_mark_break),
            patch.object(executor, "unmount_source_volume"),
            patch.object(executor, "mount_destination_volume"),
            patch.object(executor, "recreate_cifs_shares"),
            patch.object(
                executor,
                "rename_source_volume_for_delete",
                side_effect=_mark_rename_source,
            ),
            patch.object(
                executor,
                "offline_source_volume",
                side_effect=_mark_offline,
            ),
            patch.object(
                executor,
                "rename_destination_volume_to_source_name",
                side_effect=_mark_rename_destination,
            ),
        ):
            executor.execute(
                volume_name="vol_sales",
                junction_path="/vol_sales_dst",
                shares=[
                    ShareInfo(
                        share_name="share$",
                        volume_name="vol_sales",
                        path="/",
                        comment="",
                        acls=[],
                    )
                ],
                exports=[],
                protocol="cifs",
                nfs_policies=[],
            )

        self.assertEqual(
            call_order,
            [
                "update:vol_sales",
                "break:vol_sales",
                "rename_src:vol_sales",
                "offline:vol_sales_delete",
                "rename_dst:vol_sales",
            ],
        )


# ---------------------------------------------------------------------------
# snapmirror — transfer start via PATCH
# ---------------------------------------------------------------------------


class TestStartSnapmirrorTransfers(unittest.TestCase):
    """Unit tests for migrate.snapmirror._start_snapmirror_transfers()."""

    def _make_volume(self, name: str) -> object:
        from migrate.snapmirror import VolumeInfo

        return VolumeInfo(
            name=name,
            uuid=f"uuid-{name}",
            svm_name="vs_src",
            size=1073741824,
            language="c.utf_8",
        )

    def test_patches_relationship_to_snapmirrored(self) -> None:
        """Each relationship must be patched to state snapmirrored."""
        from migrate.snapmirror import _start_snapmirror_transfers

        mock_rel = MagicMock()
        with patch(
            "migrate.snapmirror.SnapmirrorRelationship.get_collection",
            return_value=iter([mock_rel]),
        ):
            _start_snapmirror_transfers(
                volumes=[self._make_volume("vol_sales")],
                dst_svm_name="vs_dst",
                connection=MagicMock(),
            )

        self.assertEqual(mock_rel.state, "snapmirrored")
        mock_rel.patch.assert_called_once()

    def test_warns_and_skips_when_relationship_not_found(self) -> None:
        """Missing relationship must log a warning and not raise."""
        from migrate.snapmirror import _start_snapmirror_transfers

        with patch(
            "migrate.snapmirror.SnapmirrorRelationship.get_collection",
            return_value=iter([]),
        ):
            # Must not raise even when no relationship is found
            _start_snapmirror_transfers(
                volumes=[self._make_volume("vol_missing")],
                dst_svm_name="vs_dst",
                connection=MagicMock(),
            )


# ---------------------------------------------------------------------------
# snapmirror — ensure_svm_peer guard rules
# ---------------------------------------------------------------------------


class TestEnsureSvmPeer(unittest.TestCase):
    """Unit tests for migrate.snapmirror.ensure_svm_peer()."""

    def test_skip_when_same_svm(self) -> None:
        """ensure_svm_peer must return immediately when SVMs are identical."""
        from migrate.snapmirror import ensure_svm_peer

        with patch("migrate.snapmirror.SvmPeer.get_collection") as mock_get:
            ensure_svm_peer(
                src_svm_name="vs_prod",
                dst_svm_name="vs_prod",
                src_connection=MagicMock(),
                dst_connection=MagicMock(),
            )
        mock_get.assert_not_called()

    def test_peer_created_when_different_svm(self) -> None:
        """ensure_svm_peer must call post() when SVMs differ and no peer exists."""
        from migrate.snapmirror import ensure_svm_peer

        mock_peer_instance = MagicMock()
        with (
            patch("migrate.snapmirror.get_cluster_name", return_value="dst-cluster"),
            patch(
                "migrate.snapmirror.SvmPeer.get_collection",
                return_value=iter([]),
            ),
            patch(
                "migrate.snapmirror.SvmPeer",
                return_value=mock_peer_instance,
            ),
        ):
            ensure_svm_peer(
                src_svm_name="vs_src",
                dst_svm_name="vs_dst",
                src_connection=MagicMock(),
                dst_connection=MagicMock(),
            )
        mock_peer_instance.post.assert_called_once()

    def test_skip_when_peer_already_exists(self) -> None:
        """ensure_svm_peer must not instantiate SvmPeer if peer already exists."""
        from migrate.snapmirror import ensure_svm_peer

        mock_peer_instance = MagicMock()
        with (
            patch("migrate.snapmirror.get_cluster_name", return_value="dst-cluster"),
            patch(
                "migrate.snapmirror.SvmPeer.get_collection",
                return_value=iter([MagicMock()]),
            ),
        ):
            ensure_svm_peer(
                src_svm_name="vs_src",
                dst_svm_name="vs_dst",
                src_connection=MagicMock(),
                dst_connection=MagicMock(),
            )
        # post() must never have been called — peer already existed
        mock_peer_instance.post.assert_not_called()


# ---------------------------------------------------------------------------
# ontap_migrate — same-cluster CLI guard
# ---------------------------------------------------------------------------


class TestParseArgsSameCluster(unittest.TestCase):
    """Unit tests for same-cluster credential reuse in ontap_migrate.parse_args()."""

    def test_same_cluster_reuses_source_password(self) -> None:
        """Same cluster must reuse source password for destination."""
        from ontap_migrate import parse_args

        argv = [
            "replicate",
            "--source-cluster",
            "10.0.0.1",
            "--source-username",
            "admin",
            "--destination-cluster",
            "10.0.0.1",
            "--destination-username",
            "admin",
            "--source-svm",
            "vs_prod",
        ]
        with patch("migrate.snapmirror.getpass.getpass", return_value="src-pw"):
            args = parse_args(argv)
        self.assertEqual(args.destination_password, args.source_password)

    def test_same_cluster_reuses_source_username(self) -> None:
        """Same cluster must reuse source username for destination."""
        from ontap_migrate import parse_args

        argv = [
            "replicate",
            "--source-cluster",
            "10.0.0.1",
            "--source-username",
            "admin",
            "--destination-cluster",
            "10.0.0.1",
            "--destination-username",
            "admin",
            "--source-svm",
            "vs_prod",
        ]
        with patch("migrate.snapmirror.getpass.getpass", return_value="pw"):
            args = parse_args(argv)
        self.assertEqual(args.destination_username, "admin")

    def test_same_cluster_comparison_is_case_insensitive(self) -> None:
        """Cluster name comparison must ignore case differences."""
        from ontap_migrate import parse_args

        argv = [
            "replicate",
            "--source-cluster",
            "Cluster1.example.com",
            "--source-username",
            "admin",
            "--destination-cluster",
            "cluster1.EXAMPLE.COM",
            "--destination-username",
            "admin",
            "--source-svm",
            "vs_prod",
        ]
        with patch("migrate.snapmirror.getpass.getpass", return_value="pw"):
            args = parse_args(argv)
        self.assertEqual(args.destination_password, args.source_password)

    def test_different_clusters_resolve_passwords_independently(self) -> None:
        """Different clusters must keep source and destination passwords separate."""
        from ontap_migrate import parse_args

        argv = [
            "replicate",
            "--source-cluster",
            "10.0.0.1",
            "--source-username",
            "admin",
            "--source-password",
            "src-pw",
            "--destination-cluster",
            "10.0.0.2",
            "--destination-username",
            "admin",
            "--destination-password",
            "dst-pw",
            "--source-svm",
            "vs_prod",
        ]
        args = parse_args(argv)
        self.assertEqual(args.source_password, "src-pw")
        self.assertEqual(args.destination_password, "dst-pw")


# ---------------------------------------------------------------------------
# ontap_migrate — CLI argument parsing
# ---------------------------------------------------------------------------


class TestParseArgs(unittest.TestCase):
    """Unit tests for ontap_migrate.parse_args()."""

    _BASE = [
        "--source-cluster",
        "10.0.0.1",
        "--source-username",
        "admin",
        "--destination-cluster",
        "10.0.0.2",
        "--destination-username",
        "admin",
        "--source-svm",
        "vs_prod",
    ]

    def _parse(self, cmd: str, extra: list[str] | None = None) -> object:
        from ontap_migrate import parse_args

        argv = [cmd] + self._BASE + (extra or [])
        with patch("migrate.snapmirror.getpass.getpass", return_value="pw"):
            return parse_args(argv)

    def test_replicate_command_parsed(self) -> None:
        """replicate subcommand must set args.command correctly."""
        args = self._parse("replicate")
        self.assertEqual(args.command, "replicate")

    def test_collect_command_parsed(self) -> None:
        """collect subcommand must set args.command correctly."""
        args = self._parse("collect")
        self.assertEqual(args.command, "collect")

    def test_cutover_command_parsed(self) -> None:
        """cutover subcommand must set args.command correctly."""
        args = self._parse("cutover")
        self.assertEqual(args.command, "cutover")

    def test_default_destination_svm(self) -> None:
        """Missing --destination-svm must default to <source-svm>_dst."""
        from migrate.snapmirror import DST_SVM_SUFFIX

        args = self._parse("replicate")
        self.assertEqual(args.destination_svm, f"vs_prod{DST_SVM_SUFFIX}")

    def test_explicit_destination_svm(self) -> None:
        """Explicit --destination-svm must be preserved."""
        args = self._parse("replicate", ["--destination-svm", "vs_dr"])
        self.assertEqual(args.destination_svm, "vs_dr")

    def test_default_protocol_is_cifs(self) -> None:
        """Default protocol must be 'cifs'."""
        args = self._parse("replicate")
        self.assertEqual(args.protocol, "cifs")

    def test_protocol_nfs_accepted(self) -> None:
        """Protocol 'nfs' must be accepted."""
        args = self._parse("replicate", ["--protocol", "nfs"])
        self.assertEqual(args.protocol, "nfs")

    def test_protocol_both_accepted(self) -> None:
        """Protocol 'both' must be accepted."""
        args = self._parse("replicate", ["--protocol", "both"])
        self.assertEqual(args.protocol, "both")

    def test_invalid_protocol_exits(self) -> None:
        """Invalid protocol must cause SystemExit."""
        with self.assertRaises(SystemExit):
            self._parse("replicate", ["--protocol", "ftp"])

    def test_exclude_volumes_parsed(self) -> None:
        """--exclude-volumes must produce a list of volume names."""
        args = self._parse(
            "replicate",
            ["--exclude-volumes", "vol_temp", "vol_scratch"],
        )
        self.assertIn("vol_temp", args.exclude_volumes)
        self.assertIn("vol_scratch", args.exclude_volumes)


# ---------------------------------------------------------------------------
# ontap_migrate — OntapMigrate.run_cutover aborted path
# ---------------------------------------------------------------------------


class TestRunCutoverAbort(unittest.TestCase):
    """Verify that answering 'no' at the cutover prompt aborts cleanly."""

    def test_abort_on_no_answer(self) -> None:
        """Typing 'no' at the cutover prompt must log abort and return."""
        from ontap_migrate import OntapMigrate, parse_args

        state = {
            "src_svm": "vs_prod",
            "dst_svm": "vs_prod_dst",
            "cifs_shares": [
                {
                    "share_name": "test$",
                    "volume_name": "vol_sales",
                    "path": "/",
                    "comment": "",
                }
            ],
            "nfs_exports": [],
        }

        argv = [
            "cutover",
            "--source-cluster",
            "10.0.0.1",
            "--source-username",
            "admin",
            "--destination-cluster",
            "10.0.0.2",
            "--destination-username",
            "admin",
            "--source-svm",
            "vs_prod",
        ]
        with patch("migrate.snapmirror.getpass.getpass", return_value="pw"):
            args = parse_args(argv)

        with patch("ontap_migrate.load_cutover_state", return_value=state):
            with patch("builtins.input", return_value="no"):
                migrator = OntapMigrate.__new__(OntapMigrate)
                migrator._args = args
                migrator._src_conn = MagicMock()
                migrator._dst_conn = MagicMock()
                migrator._state_path = Path("cutover_state.json")
                migrator.run_cutover()  # must not raise


class TestRunCutoverVolumeSelection(unittest.TestCase):
    """Verify cutover behavior for volume_names and unmounted volumes."""

    def test_executes_volume_from_volume_names_without_protocol_entries(self) -> None:
        """run_cutover must execute volumes listed in volume_names."""
        from ontap_migrate import OntapMigrate, parse_args

        state = {
            "src_svm": "vs_prod",
            "dst_svm": "vs_prod_dst",
            "volume_names": ["vol_orphan"],
            "cifs_shares": [],
            "nfs_exports": [],
            "nfs_policies": [],
        }

        argv = [
            "cutover",
            "--source-cluster",
            "10.0.0.1",
            "--source-username",
            "admin",
            "--destination-cluster",
            "10.0.0.2",
            "--destination-username",
            "admin",
            "--source-svm",
            "vs_prod",
        ]
        with patch("migrate.snapmirror.getpass.getpass", return_value="pw"):
            args = parse_args(argv)

        executor = MagicMock()
        with (
            patch("ontap_migrate.load_cutover_state", return_value=state),
            patch("ontap_migrate.CutoverExecutor", return_value=executor),
            patch("builtins.input", return_value="yes"),
            patch.object(
                OntapMigrate,
                "_resolve_junction_path",
                return_value="/vol_orphan_dst",
            ),
        ):
            migrator = OntapMigrate.__new__(OntapMigrate)
            migrator._args = args
            migrator._src_conn = MagicMock()
            migrator._dst_conn = MagicMock()
            migrator._state_path = Path("cutover_state.json")
            migrator.run_cutover()

        executor.execute.assert_called_once_with(
            volume_name="vol_orphan",
            junction_path="/vol_orphan_dst",
            shares=[],
            exports=[],
            protocol="cifs",
            nfs_policies=[],
        )

    def test_resolve_junction_path_unmounted_returns_default(self) -> None:
        """_resolve_junction_path must return default path when source is unmounted."""
        from migrate.snapmirror import DST_VOLUME_SUFFIX
        from ontap_migrate import OntapMigrate

        volume_obj = MagicMock()
        volume_obj.nas.path = None

        migrator = OntapMigrate.__new__(OntapMigrate)
        migrator._args = MagicMock()
        migrator._args.source_svm = "vs_prod"
        migrator._src_conn = MagicMock()

        with patch(
            "ontap_migrate.OntapVolume.get_collection",
            return_value=iter([volume_obj]),
        ):
            junction_path = migrator._resolve_junction_path("vol_orphan")

        self.assertEqual(junction_path, f"/vol_orphan{DST_VOLUME_SUFFIX}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def main() -> None:
    """Run all smoke tests and exit with an appropriate code.

    Returns:
        None
    """
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
