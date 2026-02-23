import base64
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from ai.chronon.repo.admin import (
    _authenticate_docker_hub,
    _base_domain,
    _creds_from_helper,
    _get_docker_credentials,
    _parse_registry,
)
from ai.chronon.repo.registry_client import DOCKER_HUB_REGISTRY


# --- _parse_registry ---


class TestParseRegistry:
    def test_gcloud_style_with_path(self):
        host, prefix = _parse_registry("us-central1-docker.pkg.dev/canary-443022/canary-images")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == "canary-443022/canary-images"

    def test_oci_style_hostname_only(self):
        host, prefix = _parse_registry("us-central1-docker.pkg.dev")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == ""

    def test_with_https_scheme(self):
        host, prefix = _parse_registry("https://us-central1-docker.pkg.dev/project/repo")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == "project/repo"

    def test_ecr(self):
        host, prefix = _parse_registry("123456789.dkr.ecr.us-east-1.amazonaws.com")
        assert host == "123456789.dkr.ecr.us-east-1.amazonaws.com"
        assert prefix == ""

    def test_trailing_slash_stripped(self):
        host, prefix = _parse_registry("us-central1-docker.pkg.dev/project/repo/")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == "project/repo"

    def test_scheme_only_hostname(self):
        host, prefix = _parse_registry("https://ghcr.io")
        assert host == "ghcr.io"
        assert prefix == ""


# --- _base_domain ---


class TestBaseDomain:
    def test_full_url_with_path(self):
        assert _base_domain("https://index.docker.io/v1/") == "docker.io"

    def test_bare_hostname(self):
        assert _base_domain("registry-1.docker.io") == "docker.io"

    def test_subdomain_hostname(self):
        assert _base_domain("index.docker.io") == "docker.io"

    def test_gcr_artifact_registry(self):
        assert _base_domain("us-docker.pkg.dev") == "pkg.dev"

    def test_ecr(self):
        assert _base_domain("123456789.dkr.ecr.us-east-1.amazonaws.com") == "amazonaws.com"

    def test_ghcr(self):
        assert _base_domain("ghcr.io") == "ghcr.io"

    def test_single_label(self):
        assert _base_domain("localhost") == "localhost"

    def test_docker_hub_entries_match_each_other(self):
        """The canonical docker login URL and the registry API hostname should resolve to the same base domain."""
        assert _base_domain("https://index.docker.io/v1/") == _base_domain("registry-1.docker.io")
        assert _base_domain("https://index.docker.io/v1/") == _base_domain(DOCKER_HUB_REGISTRY)


# --- _get_docker_credentials ---


class TestGetDockerCredentials:
    def test_no_config_file(self, tmp_path):
        with patch.dict(os.environ, {"HOME": str(tmp_path)}):
            with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
                assert _get_docker_credentials("registry-1.docker.io") == (None, None)

    def test_inline_auths(self, tmp_path):
        auth_value = base64.b64encode(b"myuser:mypass").decode()
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            username, password = _get_docker_credentials("registry-1.docker.io")
            assert username == "myuser"
            assert password == "mypass"

    def test_inline_auths_no_match(self, tmp_path):
        auth_value = base64.b64encode(b"myuser:mypass").decode()
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            assert _get_docker_credentials("ghcr.io") == (None, None)

    def test_cred_helpers(self, tmp_path):
        config = {
            "credHelpers": {
                "https://index.docker.io/v1/": "osxkeychain"
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            with patch("ai.chronon.repo.admin._creds_from_helper", return_value=("helperuser", "helperpass")) as mock:
                username, password = _get_docker_credentials("registry-1.docker.io")
                assert username == "helperuser"
                assert password == "helperpass"
                mock.assert_called_once_with("osxkeychain", "https://index.docker.io/v1/")

    def test_creds_store(self, tmp_path):
        config = {
            "credsStore": "desktop",
            "auths": {
                "https://index.docker.io/v1/": {}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            with patch("ai.chronon.repo.admin._creds_from_helper", return_value=("storeuser", "storepass")) as mock:
                username, password = _get_docker_credentials("registry-1.docker.io")
                assert username == "storeuser"
                assert password == "storepass"
                mock.assert_called_once_with("desktop", "https://index.docker.io/v1/")

    def test_cred_helpers_takes_priority_over_creds_store(self, tmp_path):
        config = {
            "credsStore": "desktop",
            "credHelpers": {
                "https://index.docker.io/v1/": "osxkeychain"
            },
            "auths": {
                "https://index.docker.io/v1/": {}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            with patch("ai.chronon.repo.admin._creds_from_helper", return_value=("helperuser", "helperpass")) as mock:
                _get_docker_credentials("registry-1.docker.io")
                mock.assert_called_once_with("osxkeychain", "https://index.docker.io/v1/")

    def test_malformed_base64_skipped(self, tmp_path):
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": "not-valid-base64!!!"}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            assert _get_docker_credentials("registry-1.docker.io") == (None, None)

    def test_missing_colon_skipped(self, tmp_path):
        auth_value = base64.b64encode(b"nocolonhere").decode()
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            assert _get_docker_credentials("registry-1.docker.io") == (None, None)

    def test_non_docker_hub_registry(self, tmp_path):
        auth_value = base64.b64encode(b"gcruser:gcrpass").decode()
        config = {
            "auths": {
                "us-docker.pkg.dev": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            username, password = _get_docker_credentials("us-docker.pkg.dev")
            assert username == "gcruser"
            assert password == "gcrpass"


# --- _creds_from_helper ---


class TestCredsFromHelper:
    def test_successful_helper_call(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"Username": "user1", "Secret": "secret1"})

        with patch("ai.chronon.repo.admin.subprocess.run", return_value=mock_result) as mock_run:
            username, password = _creds_from_helper("osxkeychain", "https://index.docker.io/v1/")
            assert username == "user1"
            assert password == "secret1"
            mock_run.assert_called_once_with(
                ["docker-credential-osxkeychain", "get"],
                input="https://index.docker.io/v1/",
                capture_output=True,
                text=True,
            )

    def test_helper_not_found(self):
        with patch("ai.chronon.repo.admin.subprocess.run", side_effect=FileNotFoundError):
            assert _creds_from_helper("missing", "https://index.docker.io/v1/") == (None, None)

    def test_helper_nonzero_exit(self):
        mock_result = MagicMock()
        mock_result.returncode = 1

        with patch("ai.chronon.repo.admin.subprocess.run", return_value=mock_result):
            assert _creds_from_helper("osxkeychain", "https://index.docker.io/v1/") == (None, None)


# --- _authenticate_docker_hub ---


class TestAuthenticateDockerHub:
    def test_with_api_token(self):
        client = MagicMock()
        _authenticate_docker_hub(client, "my-token")
        client.authenticate.assert_called_once_with(DOCKER_HUB_REGISTRY, username="ziplineai", password="my-token")

    def test_with_local_credentials(self):
        client = MagicMock()
        with patch("ai.chronon.repo.admin._get_docker_credentials", return_value=("localuser", "localpass")):
            _authenticate_docker_hub(client, None)
            client.authenticate.assert_called_once_with(DOCKER_HUB_REGISTRY, username="localuser", password="localpass")

    def test_no_credentials_raises(self):
        client = MagicMock()
        with patch("ai.chronon.repo.admin._get_docker_credentials", return_value=(None, None)):
            with pytest.raises(Exception, match="Docker Hub credentials not found"):
                _authenticate_docker_hub(client, None)

    def test_api_token_takes_priority(self):
        client = MagicMock()
        with patch("ai.chronon.repo.admin._get_docker_credentials", return_value=("localuser", "localpass")) as mock:
            _authenticate_docker_hub(client, "my-token")
            mock.assert_not_called()
            client.authenticate.assert_called_once_with(DOCKER_HUB_REGISTRY, username="ziplineai", password="my-token")
