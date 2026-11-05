"""Regression tests for the "Version number N is not found" bug: Universe
search discovery used to hardcode version=1, which fails permanently (not
transiently) whenever a project's real version differs."""

from unittest.mock import MagicMock, patch

import pytest

from app.connectors.roboflow_connector import RoboflowConnector, _looks_inaccessible
from app.core.exceptions import TransientConnectorError


class TestLooksInaccessible:
    def test_version_not_found_is_skippable(self):
        assert _looks_inaccessible(Exception("Version number 1 is not found."))

    def test_does_not_exist_is_skippable(self):
        assert _looks_inaccessible(Exception("Project does not exist or cannot be loaded"))

    def test_unrelated_error_is_not_skippable(self):
        assert not _looks_inaccessible(Exception("Connection reset by peer"))


class TestParseUniverseItem:
    def test_no_longer_hardcodes_version(self):
        item = RoboflowConnector._parse_universe_item(
            {"owner": "bibi-kclpc", "slug": "cabinet-arprb", "license": "CC BY 4.0"}
        )
        assert item is not None
        assert "version" not in item.external_ref
        assert item.external_ref == {"workspace": "bibi-kclpc", "project": "cabinet-arprb"}


class TestResolveLatestVersion:
    def test_picks_max_version_number(self):
        connector = RoboflowConnector(api_keys=["key1"])
        resp = MagicMock()
        resp.json.return_value = {
            "project": {"versions": [{"id": "bibi-kclpc/cabinet-arprb/1"}, {"id": "bibi-kclpc/cabinet-arprb/3"}]}
        }
        resp.raise_for_status = MagicMock()
        with patch("app.connectors.roboflow_connector.requests.get", return_value=resp):
            version = connector._resolve_latest_version("bibi-kclpc", "cabinet-arprb")
        assert version == 3

    def test_returns_none_on_failure(self):
        connector = RoboflowConnector(api_keys=["key1"])
        with patch("app.connectors.roboflow_connector.requests.get", side_effect=Exception("network error")):
            assert connector._resolve_latest_version("bibi-kclpc", "cabinet-arprb") is None


class TestSdkDownloadSkipsVersionNotFound:
    def test_version_not_found_is_skipped_not_raised(self, tmp_path):
        connector = RoboflowConnector(api_keys=["key1"])

        fake_rf = MagicMock()
        fake_rf.workspace.return_value.project.return_value.version.side_effect = Exception(
            "Version number 1 is not found."
        )

        with patch.dict("sys.modules", {"roboflow": MagicMock(Roboflow=MagicMock(return_value=fake_rf))}):
            results = list(
                connector._sdk_download(
                    "bibi-kclpc",
                    "cabinet-arprb",
                    1,
                    "Unknown",
                    "https://universe.roboflow.com/bibi-kclpc/cabinet-arprb",
                    tmp_path,
                    {},
                    lambda: False,
                    lambda _: None,
                )
            )

        assert results == []

    def test_other_sdk_error_still_raises_transient(self, tmp_path):
        connector = RoboflowConnector(api_keys=["key1"])

        fake_rf = MagicMock()
        fake_rf.workspace.return_value.project.return_value.version.side_effect = Exception("network blip")

        with (
            patch.dict("sys.modules", {"roboflow": MagicMock(Roboflow=MagicMock(return_value=fake_rf))}),
            pytest.raises(TransientConnectorError),
        ):
            list(
                connector._sdk_download(
                    "bibi-kclpc",
                    "cabinet-arprb",
                    1,
                    "Unknown",
                    "https://universe.roboflow.com/bibi-kclpc/cabinet-arprb",
                    tmp_path,
                    {},
                    lambda: False,
                    lambda _: None,
                )
            )


class TestClassificationAndCacheSupport:
    def test_resolve_project_details_classification(self):
        connector = RoboflowConnector(api_keys=["key1"])
        resp = MagicMock()
        resp.json.return_value = {
            "project": {
                "type": "classification",
                "versions": [{"id": "tunjacv/cat-and-dog/1"}],
            }
        }
        resp.raise_for_status = MagicMock()
        with patch("app.connectors.roboflow_connector.requests.get", return_value=resp):
            details = connector._resolve_project_details("tunjacv", "cat-and-dog")
        assert details["type"] == "classification"
        assert details["latest_version"] == 1

    def test_walk_classification_folder_layout(self, tmp_path):
        from PIL import Image

        connector = RoboflowConnector(api_keys=["key1"], min_resolution=(10, 10))
        export_dir = tmp_path / "roboflow_cache" / "tunjacv__cat-and-dog__v1"
        cat_dir = export_dir / "train" / "cats"
        cat_dir.mkdir(parents=True)

        img_path = cat_dir / "cat1.jpg"
        img = Image.new("RGB", (100, 100), color="red")
        img.save(img_path)

        drafts = list(
            connector._walk_export_dir(
                export_dir,
                "tunjacv",
                "cat-and-dog",
                1,
                "CC BY 4.0",
                "https://universe.roboflow.com/tunjacv/cat-and-dog",
                {},
                lambda: False,
                lambda _: None,
            )
        )
        assert len(drafts) == 1
        assert drafts[0].classes == ["cats"]
        assert drafts[0].source_item_id.startswith("rf_tunjacv__cat-and-dog")

