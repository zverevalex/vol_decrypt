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

    def test_recreate_nfs_exports_skips_policy_without_rules(self) -> None:
        """Policy reassign must be skipped when source policy has no rules."""
        from migrate.cutover import ExportInfo, NfsPolicyInfo

        executor = self._make_executor()
        export_info = ExportInfo(policy_name="src_pol", volume_name="vol_data")
        nfs_policies = [
            NfsPolicyInfo(
                source_policy_name="src_pol",
                destination_policy_name="dst_pol",
                rules=[],
            )
        ]

        with (
            patch.object(
                executor,
                "_ensure_nfs_policy_sync_once",
                return_value={"src_pol": "dst_pol"},
            ),
            patch("migrate.cutover.Volume.get_collection") as get_collection_mock,
        ):
            executor.recreate_nfs_exports(
                volume_name="vol_data",
                exports=[export_info],
                nfs_policies=nfs_policies,
            )

        get_collection_mock.assert_not_called()


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

    def test_log_file_parsed(self) -> None:
        """--log-file must be accepted and stored in parsed args."""
        args = self._parse("replicate", ["--log-file", "migration.log"])
        self.assertEqual(args.log_file, "migration.log")

    def test_version_flag_exits(self) -> None:
        """--version must terminate parsing with exit code 0."""
        from ontap_migrate import parse_args

        with self.assertRaises(SystemExit) as ctx:
            parse_args(["--version"])
        self.assertEqual(ctx.exception.code, 0)


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

    def test_run_cutover_skips_when_no_volume_scope(self) -> None:
        """run_cutover must stop early when no volume scope is available."""
        from ontap_migrate import OntapMigrate, parse_args

        state = {
            "src_svm": "vs_prod",
            "dst_svm": "vs_prod_dst",
            "volume_names": [],
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

        with (
            patch("ontap_migrate.load_cutover_state", return_value=state),
            patch("ontap_migrate.CutoverExecutor") as executor_cls,
            patch("builtins.input") as input_mock,
        ):
            migrator = OntapMigrate.__new__(OntapMigrate)
            migrator._args = args
            migrator._src_conn = MagicMock()
            migrator._dst_conn = MagicMock()
            migrator._state_path = Path("cutover_state.json")
            migrator.run_cutover()

        executor_cls.assert_not_called()
        input_mock.assert_not_called()


# ---------------------------------------------------------------------------
# vol_schedule — imports
# ---------------------------------------------------------------------------


class TestVolScheduleImports(unittest.TestCase):
    """Verify that vol_schedule imports without errors."""

    def test_vol_schedule_imports(self) -> None:
        """vol_schedule module must be importable."""
        import vol_schedule  # noqa: F401


# ---------------------------------------------------------------------------
# vol_move_exec — imports
# ---------------------------------------------------------------------------


class TestVolMoveExecImports(unittest.TestCase):
    """Verify that vol_move_exec imports without errors."""

    def test_vol_move_exec_imports(self) -> None:
        """vol_move_exec module must be importable."""
        import vol_move_exec  # noqa: F401


# ---------------------------------------------------------------------------
# vol_schedule — build_plan_entry
# ---------------------------------------------------------------------------


class TestBuildPlanEntry(unittest.TestCase):
    """Unit tests for vol_schedule.build_plan_entry()."""

    def _make_vol(self) -> dict[str, object]:
        return {
            "name": "vol_data_01",
            "uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "svm": "vs_prod",
            "size": 107374182400,
            "space_used": 48318382080,
            "current_aggr": "aggr1_node1",
        }

    def test_returns_pending_status(self) -> None:
        """build_plan_entry must set status to 'pending'."""
        from vol_schedule import build_plan_entry

        entry = build_plan_entry(self._make_vol(), "aggr2_node2")
        self.assertEqual(entry["status"], "pending")

    def test_error_is_none(self) -> None:
        """build_plan_entry must set error to None."""
        from vol_schedule import build_plan_entry

        entry = build_plan_entry(self._make_vol(), "aggr2_node2")
        self.assertIsNone(entry["error"])

    def test_correct_target_aggregate(self) -> None:
        """build_plan_entry must record the supplied target aggregate."""
        from vol_schedule import build_plan_entry

        entry = build_plan_entry(self._make_vol(), "aggr2_node2")
        self.assertEqual(entry["target_aggregate"], "aggr2_node2")

    def test_name_and_uuid_propagated(self) -> None:
        """build_plan_entry must copy name and uuid from the volume dict."""
        from vol_schedule import build_plan_entry

        vol = self._make_vol()
        entry = build_plan_entry(vol, "aggr2_node2")
        self.assertEqual(entry["name"], vol["name"])
        self.assertEqual(entry["uuid"], vol["uuid"])

    def test_required_keys_present(self) -> None:
        """build_plan_entry must return a dict with all required keys."""
        from vol_schedule import build_plan_entry

        entry = build_plan_entry(self._make_vol(), "aggr2_node2")
        for key in ("name", "uuid", "target_aggregate", "status", "error"):
            self.assertIn(key, entry)


# ---------------------------------------------------------------------------
# vol_schedule — format_volume_table
# ---------------------------------------------------------------------------


class TestFormatVolumeTable(unittest.TestCase):
    """Unit tests for vol_schedule.format_volume_table()."""

    def _make_volumes(self) -> list[dict[str, object]]:
        return [
            {
                "name": "vol_sales",
                "uuid": "uuid-1",
                "svm": "vs_prod",
                "size": 107374182400,
                "space_used": 53687091200,
                "current_aggr": "aggr1_node1",
            },
            {
                "name": "vol_finance",
                "uuid": "uuid-2",
                "svm": "vs_prod",
                "size": 214748364800,
                "space_used": 32212254720,
                "current_aggr": "aggr2_node2",
            },
        ]

    def test_returns_non_empty_string(self) -> None:
        """format_volume_table must return a non-empty string."""
        from vol_schedule import format_volume_table

        result = format_volume_table(self._make_volumes())
        self.assertIsInstance(result, str)
        self.assertTrue(result)

    def test_contains_volume_name(self) -> None:
        """format_volume_table must include each volume name in the output."""
        from vol_schedule import format_volume_table

        result = format_volume_table(self._make_volumes())
        self.assertIn("vol_sales", result)
        self.assertIn("vol_finance", result)

    def test_contains_aggregate_name(self) -> None:
        """format_volume_table must include current aggregate names."""
        from vol_schedule import format_volume_table

        result = format_volume_table(self._make_volumes())
        self.assertIn("aggr1_node1", result)

    def test_row_count_matches_volumes(self) -> None:
        """format_volume_table must produce one data row per volume."""
        from vol_schedule import format_volume_table

        result = format_volume_table(self._make_volumes())
        # Header + separator = 2 lines; remaining are data rows
        lines = [ln for ln in result.splitlines() if ln.strip()]
        data_rows = [ln for ln in lines[2:] if ln.strip()]
        self.assertEqual(len(data_rows), len(self._make_volumes()))

    def test_empty_list_returns_header_only(self) -> None:
        """format_volume_table with an empty list must still return a header."""
        from vol_schedule import format_volume_table

        result = format_volume_table([])
        self.assertIn("Name", result)


# ---------------------------------------------------------------------------
# vol_move_exec — load_plan
# ---------------------------------------------------------------------------


class TestLoadPlan(unittest.TestCase):
    """Unit tests for vol_move_exec.load_plan()."""

    def setUp(self) -> None:
        import tempfile

        self._tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self) -> None:
        import shutil

        shutil.rmtree(str(self._tmpdir), ignore_errors=True)

    def _write(self, content: str) -> Path:
        p = self._tmpdir / "plan.yaml"
        p.write_text(content, encoding="utf-8")
        return p

    _VALID_YAML = """\
cluster: "cluster1.example.com"
svm: "vs_prod"
volumes:
  - name: "vol_data_01"
    uuid: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    target_aggregate: "aggr1_node1"
    status: "pending"
    error: null
"""

    def test_valid_plan_returns_dict_with_volumes(self) -> None:
        """load_plan with a well-formed YAML file must return a dict."""
        from vol_move_exec import load_plan

        plan = load_plan(self._write(self._VALID_YAML))
        self.assertIn("volumes", plan)
        self.assertEqual(len(plan["volumes"]), 1)

    def test_valid_plan_cluster_preserved(self) -> None:
        """load_plan must preserve the cluster key value."""
        from vol_move_exec import load_plan

        plan = load_plan(self._write(self._VALID_YAML))
        self.assertEqual(plan["cluster"], "cluster1.example.com")

    def test_missing_cluster_raises_value_error(self) -> None:
        """load_plan must raise ValueError when 'cluster' key is absent."""
        from vol_move_exec import load_plan

        yaml_no_cluster = "svm: vs_prod\nvolumes: []\n"
        with self.assertRaises(ValueError) as ctx:
            load_plan(self._write(yaml_no_cluster))
        self.assertIn("cluster", str(ctx.exception))

    def test_missing_svm_raises_value_error(self) -> None:
        """load_plan must raise ValueError when 'svm' key is absent."""
        from vol_move_exec import load_plan

        yaml_no_svm = "cluster: c1\nvolumes: []\n"
        with self.assertRaises(ValueError):
            load_plan(self._write(yaml_no_svm))

    def test_missing_entry_key_raises_value_error(self) -> None:
        """load_plan must raise ValueError when a volume entry lacks a required key."""
        from vol_move_exec import load_plan

        yaml_bad_entry = """\
cluster: "c1"
svm: "vs_prod"
volumes:
  - name: "vol1"
    uuid: "xxx"
    target_aggregate: "aggr1"
"""
        with self.assertRaises(ValueError) as ctx:
            load_plan(self._write(yaml_bad_entry))
        self.assertIn("status", str(ctx.exception))

    def test_missing_file_raises_file_not_found(self) -> None:
        """load_plan must raise FileNotFoundError for a non-existent path."""
        from vol_move_exec import load_plan

        with self.assertRaises(FileNotFoundError):
            load_plan(self._tmpdir / "does_not_exist.yaml")


# ---------------------------------------------------------------------------
# vol_move_exec — update_in_progress_statuses
# ---------------------------------------------------------------------------


class TestUpdateInProgressStatuses(unittest.TestCase):
    """Unit tests for vol_move_exec.update_in_progress_statuses()."""

    def _make_entry(self, status: str = "in_progress") -> dict[str, object]:
        return {
            "name": "vol_data_01",
            "uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "target_aggregate": "aggr2_node2",
            "status": status,
            "error": None,
        }

    def test_no_movement_transitions_to_done(self) -> None:
        """in_progress entry with no movement attribute must transition to done."""
        from vol_move_exec import update_in_progress_statuses

        entries = [self._make_entry("in_progress")]
        mock_vol_instance = MagicMock()
        mock_vol_instance.movement = None  # SDK returns no movement object

        with patch("netapp_ontap.resources.Volume", return_value=mock_vol_instance):
            update_in_progress_statuses(entries)

        self.assertEqual(entries[0]["status"], "done")

    def test_active_state_stays_in_progress(self) -> None:
        """in_progress entry with an active movement state must stay in_progress."""
        from vol_move_exec import update_in_progress_statuses

        entries = [self._make_entry("in_progress")]
        mock_movement = MagicMock()
        mock_movement.state = "replicating"
        mock_vol_instance = MagicMock()
        mock_vol_instance.movement = mock_movement

        with patch("netapp_ontap.resources.Volume", return_value=mock_vol_instance):
            update_in_progress_statuses(entries)

        self.assertEqual(entries[0]["status"], "in_progress")

    def test_failed_state_transitions_to_failed(self) -> None:
        """in_progress entry with movement.state == 'failed' must become failed."""
        from vol_move_exec import update_in_progress_statuses

        entries = [self._make_entry("in_progress")]
        mock_movement = MagicMock()
        mock_movement.state = "failed"
        mock_vol_instance = MagicMock()
        mock_vol_instance.movement = mock_movement

        with patch("netapp_ontap.resources.Volume", return_value=mock_vol_instance):
            update_in_progress_statuses(entries)

        self.assertEqual(entries[0]["status"], "failed")

    def test_pending_entry_is_not_touched(self) -> None:
        """update_in_progress_statuses must not modify non-in_progress entries."""
        from vol_move_exec import update_in_progress_statuses

        entries = [self._make_entry("pending")]

        with patch("netapp_ontap.resources.Volume") as mock_vol_cls:
            update_in_progress_statuses(entries)
            mock_vol_cls.assert_not_called()

        self.assertEqual(entries[0]["status"], "pending")

    def test_get_error_logged_and_entry_unchanged(self) -> None:
        """NetAppRestError during .get() must be logged; status must not change."""
        from vol_move_exec import update_in_progress_statuses

        entries = [self._make_entry("in_progress")]
        mock_vol_instance = MagicMock()

        # Patch NetAppRestError in the module to plain Exception for this test
        with (
            patch("netapp_ontap.resources.Volume", return_value=mock_vol_instance),
            patch("netapp_ontap.error.NetAppRestError", Exception),
        ):
            mock_vol_instance.get.side_effect = Exception("simulated REST error")
            update_in_progress_statuses(entries)

        self.assertEqual(entries[0]["status"], "in_progress")


# ---------------------------------------------------------------------------
# vol_move_exec — start_pending_moves
# ---------------------------------------------------------------------------


class TestStartPendingMoves(unittest.TestCase):
    """Unit tests for vol_move_exec.start_pending_moves()."""

    def _make_entry(self, status: str = "pending") -> dict[str, object]:
        return {
            "name": "vol_data_01",
            "uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "target_aggregate": "aggr2_node2",
            "status": status,
            "error": None,
        }

    def _aggr_map(self) -> dict[str, object]:
        return {"aggr2_node2": {"node_name": "node-01"}}

    def test_successful_patch_transitions_to_in_progress(self) -> None:
        """pending entry must become in_progress when Volume.patch succeeds."""
        from vol_move_exec import start_pending_moves

        entries = [self._make_entry("pending")]
        mock_vol_instance = MagicMock()
        node_counts: dict[str, int] = {}

        with (
            patch("netapp_ontap.resources.Volume", return_value=mock_vol_instance),
            patch("vol_move_exec.get_volume_source_node", return_value="node-01"),
        ):
            started = start_pending_moves(
                entries, svm="vs_prod", aggr_map=self._aggr_map(),
                node_move_counts=node_counts, max_per_node=8,
            )

        self.assertEqual(entries[0]["status"], "in_progress")
        self.assertEqual(started, 1)
        self.assertEqual(node_counts.get("node-01"), 1)

    def test_failed_patch_transitions_to_failed(self) -> None:
        """pending entry must become failed when Volume.patch raises NetAppRestError."""
        from vol_move_exec import start_pending_moves

        entries = [self._make_entry("pending")]
        mock_vol_instance = MagicMock()
        mock_vol_instance.patch.side_effect = Exception("ONTAP REST failure")
        node_counts: dict[str, int] = {}

        with (
            patch("netapp_ontap.resources.Volume", return_value=mock_vol_instance),
            patch("netapp_ontap.error.NetAppRestError", Exception),
            patch("vol_move_exec.get_volume_source_node", return_value="node-01"),
        ):
            started = start_pending_moves(
                entries, svm="vs_prod", aggr_map=self._aggr_map(),
                node_move_counts=node_counts, max_per_node=8,
            )

        self.assertEqual(entries[0]["status"], "failed")
        self.assertIn("ONTAP REST failure", str(entries[0]["error"]))
        self.assertEqual(started, 0)

    def test_zero_slots_starts_nothing(self) -> None:
        """All slots occupied on the source node must prevent any new move."""
        from vol_move_exec import start_pending_moves

        entries = [self._make_entry("pending")]
        # Node already at capacity
        node_counts: dict[str, int] = {"node-01": 8}

        with (
            patch("netapp_ontap.resources.Volume") as mock_vol_cls,
            patch("vol_move_exec.get_volume_source_node", return_value="node-01"),
        ):
            started = start_pending_moves(
                entries, svm="vs_prod", aggr_map=self._aggr_map(),
                node_move_counts=node_counts, max_per_node=8,
            )
            mock_vol_cls.assert_not_called()

        self.assertEqual(entries[0]["status"], "pending")
        self.assertEqual(started, 0)

    def test_dry_run_does_not_call_patch(self) -> None:
        """dry_run=True must not call Volume.patch and must not change status."""
        from vol_move_exec import start_pending_moves

        entries = [self._make_entry("pending")]
        mock_vol_instance = MagicMock()
        node_counts: dict[str, int] = {}

        with patch("netapp_ontap.resources.Volume", return_value=mock_vol_instance):
            started = start_pending_moves(
                entries, svm="vs_prod", aggr_map=self._aggr_map(),
                node_move_counts=node_counts, max_per_node=8, dry_run=True,
            )
            mock_vol_instance.patch.assert_not_called()

        self.assertEqual(entries[0]["status"], "pending")
        self.assertEqual(started, 1)

    def test_respects_per_node_slot_limit(self) -> None:
        """Volumes beyond max_per_node on the same source node must be skipped."""
        from vol_move_exec import start_pending_moves

        entries = [self._make_entry("pending") for _ in range(5)]
        mock_vol_instance = MagicMock()
        node_counts: dict[str, int] = {"node-01": 6}  # 6 already in flight, limit 8

        with (
            patch("netapp_ontap.resources.Volume", return_value=mock_vol_instance),
            patch("vol_move_exec.get_volume_source_node", return_value="node-01"),
        ):
            started = start_pending_moves(
                entries, svm="vs_prod", aggr_map=self._aggr_map(),
                node_move_counts=node_counts, max_per_node=8,
            )

        self.assertEqual(started, 2)  # only 2 free slots (8 - 6)
        in_progress = sum(1 for e in entries if e["status"] == "in_progress")
        self.assertEqual(in_progress, 2)

    def test_skips_non_pending_entries(self) -> None:
        """start_pending_moves must skip entries that are not pending."""
        from vol_move_exec import start_pending_moves

        entries = [
            self._make_entry("done"),
            self._make_entry("in_progress"),
            self._make_entry("pending"),
        ]
        mock_vol_instance = MagicMock()
        node_counts: dict[str, int] = {}

        with (
            patch("netapp_ontap.resources.Volume", return_value=mock_vol_instance),
            patch("vol_move_exec.get_volume_source_node", return_value="node-01"),
        ):
            started = start_pending_moves(
                entries, svm="vs_prod", aggr_map=self._aggr_map(),
                node_move_counts=node_counts, max_per_node=8,
            )

        self.assertEqual(started, 1)
        self.assertEqual(entries[0]["status"], "done")
        self.assertEqual(entries[1]["status"], "in_progress")
        self.assertEqual(entries[2]["status"], "in_progress")


# ---------------------------------------------------------------------------
# vol_schedule — format_aggregate_table (extended)
# ---------------------------------------------------------------------------


class TestFormatAggregateTable(unittest.TestCase):
    """Unit tests for vol_schedule.format_aggregate_table()."""

    def _make_aggr(
        self,
        name: str = "aggr1",
        node: str = "node-01",
        total: int = 4 * 1024 ** 3,
        used: int = 1 * 1024 ** 3,
        create_time: str | None = "2025-01-15T10:00:00Z",
    ) -> dict[str, object]:
        available = total - used
        return {
            "name": name,
            "node_name": node,
            "total": total,
            "used": used,
            "available": available,
            "usage_pct": round(used / total * 100, 1),
            "create_time": create_time,
        }

    def test_returns_non_empty_string(self) -> None:
        """format_aggregate_table must return a non-empty string."""
        from vol_schedule import format_aggregate_table

        result = format_aggregate_table([self._make_aggr()])
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)

    def test_contains_aggregate_name(self) -> None:
        """Aggregate name must appear in the table."""
        from vol_schedule import format_aggregate_table

        result = format_aggregate_table([self._make_aggr(name="aggr_prod")])
        self.assertIn("aggr_prod", result)

    def test_contains_created_date(self) -> None:
        """Created date (YYYY-MM-DD) must appear in the table."""
        from vol_schedule import format_aggregate_table

        result = format_aggregate_table([self._make_aggr(create_time="2025-03-21T08:00:00Z")])
        self.assertIn("2025-03-21", result)

    def test_unknown_when_no_create_time(self) -> None:
        """Aggregates without create_time must show 'unknown' in Created column."""
        from vol_schedule import format_aggregate_table

        result = format_aggregate_table([self._make_aggr(create_time=None)])
        self.assertIn("unknown", result)

    def test_planned_bytes_adjusts_capacity(self) -> None:
        """planned_bytes must reduce Free and increase Used in the table."""
        from vol_schedule import format_aggregate_table

        aggr = self._make_aggr(name="aggr2", total=10 * 1024 ** 3, used=2 * 1024 ** 3)
        planned = {"aggr2": 1 * 1024 ** 3}
        result = format_aggregate_table([aggr], planned_bytes=planned)
        # Effective used = 3 GiB, shown in Used(GiB) column
        self.assertIn("3.00", result)

    def test_planned_bytes_adds_asterisk_marker(self) -> None:
        """Aggregates with planned bytes must have a '*' usage marker."""
        from vol_schedule import format_aggregate_table

        aggr = self._make_aggr(name="aggr2")
        result = format_aggregate_table([aggr], planned_bytes={"aggr2": 512 * 1024 ** 2})
        self.assertIn("*", result)
        self.assertIn("(* capacity adjusted", result)

    def test_no_planned_bytes_no_asterisk(self) -> None:
        """Without planned bytes no '*' marker or footnote should appear."""
        from vol_schedule import format_aggregate_table

        result = format_aggregate_table([self._make_aggr()])
        self.assertNotIn("*", result)

    def test_sorted_newest_first(self) -> None:
        """Aggregates must be sorted newest create_time first."""
        from vol_schedule import format_aggregate_table

        old = self._make_aggr(name="aggr_old", create_time="2020-01-01T00:00:00Z")
        new = self._make_aggr(name="aggr_new", create_time="2025-06-01T00:00:00Z")
        result = format_aggregate_table([old, new])
        self.assertLess(result.index("aggr_new"), result.index("aggr_old"))

    def test_empty_list_returns_header_only(self) -> None:
        """Empty aggregate list must return the header row and separator only."""
        from vol_schedule import format_aggregate_table

        result = format_aggregate_table([])
        self.assertIn("Aggregate", result)
        self.assertIn("Created", result)
        lines = result.splitlines()
        self.assertEqual(len(lines), 2)  # header + separator


# ---------------------------------------------------------------------------
# vol_schedule — get_volumes_on_aggregate
# ---------------------------------------------------------------------------


class TestGetVolumesOnAggregate(unittest.TestCase):
    """Unit tests for vol_schedule.get_volumes_on_aggregate()."""

    def _make_mock_vol(
        self,
        name: str = "vol_data",
        uuid: str = "uuid-01",
        size: int = 1024 ** 3,
        used: int = 512 * 1024 ** 2,
        aggr_name: str = "aggr1",
        svm_name: str = "vs_prod",
        move_state: str | None = None,
    ) -> MagicMock:
        vol = MagicMock()
        vol.name = name
        vol.uuid = uuid
        vol.size = size
        aggr_mock = MagicMock()
        aggr_mock.name = aggr_name
        vol.aggregates = [aggr_mock]
        vol.svm = MagicMock()
        vol.svm.name = svm_name
        space = MagicMock()
        space.used = used
        vol.space = space
        if move_state:
            movement = MagicMock()
            movement.state = move_state
            vol.movement = movement
        else:
            vol.movement = None
        return vol

    def test_returns_volumes_on_aggregate(self) -> None:
        """Eligible volumes on the aggregate must be returned."""
        from vol_schedule import get_volumes_on_aggregate

        mock_vol = self._make_mock_vol()
        with patch(
            "netapp_ontap.resources.Volume.get_collection", return_value=[mock_vol]
        ):
            mock_vol.get = MagicMock()
            result = get_volumes_on_aggregate("vs_prod", "aggr1")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "vol_data")
        self.assertEqual(result[0]["current_aggr"], "aggr1")

    def test_skips_root_volumes(self) -> None:
        """Root volumes (name ends with _root or equals vol0) must be skipped."""
        from vol_schedule import get_volumes_on_aggregate

        root_vol = self._make_mock_vol(name="svm_root")
        vol0 = self._make_mock_vol(name="vol0", uuid="uuid-02")
        with patch(
            "netapp_ontap.resources.Volume.get_collection",
            return_value=[root_vol, vol0],
        ):
            root_vol.get = MagicMock()
            vol0.get = MagicMock()
            result = get_volumes_on_aggregate("vs_prod", "aggr1")

        self.assertEqual(result, [])

    def test_skips_in_flight_volumes(self) -> None:
        """Volumes already in an active move must be skipped."""
        from vol_schedule import get_volumes_on_aggregate

        moving_vol = self._make_mock_vol(name="vol_moving", move_state="replicating")
        with patch(
            "netapp_ontap.resources.Volume.get_collection", return_value=[moving_vol]
        ):
            moving_vol.get = MagicMock()
            result = get_volumes_on_aggregate("vs_prod", "aggr1")

        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# vol_schedule — prompt_pick_aggregate
# ---------------------------------------------------------------------------


class TestPromptPickAggregate(unittest.TestCase):
    """Unit tests for vol_schedule.prompt_pick_aggregate()."""

    def _make_aggr(self, name: str) -> dict[str, str]:
        return {"name": name, "node_name": "node-01"}

    def test_valid_selection_returns_correct_aggregate(self) -> None:
        """A valid number must return the matching aggregate dict."""
        from vol_schedule import prompt_pick_aggregate

        aggrs = [self._make_aggr("aggr1"), self._make_aggr("aggr2")]
        with patch("builtins.input", return_value="2"):
            result = prompt_pick_aggregate(aggrs, "Pick: ")
        self.assertEqual(result["name"], "aggr2")

    def test_invalid_then_valid_input(self) -> None:
        """Non-numeric and out-of-range input must re-prompt until valid."""
        from vol_schedule import prompt_pick_aggregate

        aggrs = [self._make_aggr("aggr1")]
        with patch("builtins.input", side_effect=["abc", "0", "1"]):
            result = prompt_pick_aggregate(aggrs, "Pick: ")
        self.assertEqual(result["name"], "aggr1")


# ---------------------------------------------------------------------------
# vol_move_exec — _compute_plan_status
# ---------------------------------------------------------------------------


class TestComputePlanStatus(unittest.TestCase):
    """Unit tests for vol_move_exec._compute_plan_status()."""

    def test_all_done_returns_done(self) -> None:
        """All done entries must return 'done'."""
        from vol_move_exec import _compute_plan_status

        entries = [{"status": "done"}, {"status": "done"}]
        self.assertEqual(_compute_plan_status(entries), "done")

    def test_all_pending_returns_pending(self) -> None:
        """All pending entries must return 'pending'."""
        from vol_move_exec import _compute_plan_status

        entries = [{"status": "pending"}, {"status": "pending"}]
        self.assertEqual(_compute_plan_status(entries), "pending")

    def test_mixed_returns_in_progress(self) -> None:
        """Mixed statuses with at least one pending/in_progress must return 'in_progress'."""
        from vol_move_exec import _compute_plan_status

        entries = [
            {"status": "done"},
            {"status": "pending"},
            {"status": "in_progress"},
        ]
        self.assertEqual(_compute_plan_status(entries), "in_progress")

    def test_all_failed_returns_failed(self) -> None:
        """All failed entries (no pending/in_progress) must return 'failed'."""
        from vol_move_exec import _compute_plan_status

        entries = [{"status": "failed"}, {"status": "failed"}]
        self.assertEqual(_compute_plan_status(entries), "failed")


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
