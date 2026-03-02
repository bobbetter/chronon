"""OCI Distribution API client and image target abstraction.

RegistryClient implements the OCI Distribution Spec (v2) for pulling/pushing manifests and blobs.
ImageTarget wraps either a RegistryClient (remote) or the local Docker CLI.
"""

import hashlib
import json
import logging
import os
import subprocess
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import urllib3

logger = logging.getLogger(__name__)

# Docker Hub uses a separate auth service
DOCKER_HUB_REGISTRY = "registry-1.docker.io"
DOCKER_HUB_AUTH_URL = "https://auth.docker.io/token"

# OCI media types
MANIFEST_V2 = "application/vnd.docker.distribution.manifest.v2+json"
MANIFEST_LIST_V2 = "application/vnd.docker.distribution.manifest.list.v2+json"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
ALL_MANIFEST_TYPES = ", ".join([MANIFEST_V2, MANIFEST_LIST_V2, OCI_MANIFEST, OCI_INDEX])
_MULTI_PLATFORM_TYPES = (MANIFEST_LIST_V2, OCI_INDEX)

# Stream blobs in 8MB chunks to avoid loading multi-GB layers into memory
BLOB_CHUNK_SIZE = 8 * 1024 * 1024

# Max concurrent connections per host — shared between urllib3 pool and thread workers
_CONCURRENCY = 10


class _ProgressReader:
    """File-like wrapper that calls a callback on each read(), enabling byte-level progress tracking."""

    def __init__(self, source, callback=None):
        self._source = source
        self._callback = callback

    def read(self, amt=-1):
        data = self._source.read(amt)
        if data and self._callback:
            self._callback(len(data))
        return data


@dataclass
class RegistryAuth:
    """Holds auth state for a single registry."""

    token: str | None = None
    username: str | None = None
    password: str | None = None
    scheme: str = "Bearer"


@dataclass
class RegistryClient:
    """Thin OCI registry HTTP client.

    Handles Docker Hub token exchange and generic Bearer token auth.
    Streams blobs to avoid loading multi-GB layers into memory.
    """

    http: urllib3.PoolManager = field(
        default_factory=lambda: urllib3.PoolManager(num_pools=4, maxsize=_CONCURRENCY)
    )
    _auth_cache: dict[str, RegistryAuth] = field(default_factory=dict)

    def authenticate(
        self, registry: str, username: str | None = None, password: str | None = None
    ) -> None:
        """Store credentials for a registry. Token exchange happens lazily on first request."""
        self._auth_cache[registry] = RegistryAuth(username=username, password=password)

    # ── Auth helpers ──────────────────────────────────────────────────

    def _get_auth_header(self, registry: str, repo: str) -> dict[str, str]:
        """Get Authorization header, performing token exchange if needed."""
        auth = self._auth_cache.get(registry)
        if not auth:
            return {}

        if auth.token:
            return {"Authorization": f"{auth.scheme} {auth.token}"}

        if registry == DOCKER_HUB_REGISTRY:
            return self._docker_hub_token_exchange(auth, repo)

        if auth.username and auth.password:
            headers = urllib3.make_headers(basic_auth=f"{auth.username}:{auth.password}")
            return dict(headers)

        return {}

    def _docker_hub_token_exchange(self, auth: RegistryAuth, repo: str) -> dict[str, str]:
        """Exchange Docker Hub PAT for a short-lived Bearer token."""
        headers = {}
        if auth.username and auth.password:
            headers = dict(urllib3.make_headers(basic_auth=f"{auth.username}:{auth.password}"))

        resp = self.http.request(
            "GET",
            DOCKER_HUB_AUTH_URL,
            fields={"service": "registry.docker.io", "scope": f"repository:{repo}:pull"},
            headers=headers,
        )
        if resp.status != 200:
            raise RegistryError(
                f"Docker Hub auth failed (HTTP {resp.status}): {resp.data.decode()}"
            )

        token = json.loads(resp.data)["token"]
        auth.token = token
        return {"Authorization": f"Bearer {token}"}

    def _registry_token_exchange(
        self, registry: str, repo: str, www_authenticate: str
    ) -> dict[str, str]:
        """Handle WWW-Authenticate challenge for non-Docker-Hub registries."""
        auth = self._auth_cache.get(registry, RegistryAuth())

        params = {}
        if "Bearer " in www_authenticate:
            parts = www_authenticate.split("Bearer ", 1)[1]
            for part in parts.split(","):
                key, _, value = part.strip().partition("=")
                params[key] = value.strip('"')

        realm = params.get("realm", "")
        if not realm:
            if auth.username and auth.password:
                headers = urllib3.make_headers(basic_auth=f"{auth.username}:{auth.password}")
                return dict(headers)
            return {}

        fields = {}
        if "service" in params:
            fields["service"] = params["service"]
        if "scope" in params:
            fields["scope"] = params["scope"]
        else:
            fields["scope"] = f"repository:{repo}:pull,push"

        headers = {}
        if auth.username and auth.password:
            headers = dict(urllib3.make_headers(basic_auth=f"{auth.username}:{auth.password}"))

        resp = self.http.request("GET", realm, fields=fields, headers=headers)
        if resp.status != 200:
            raise RegistryError(
                f"Token exchange failed at {realm} (HTTP {resp.status}): {resp.data.decode()}"
            )

        data = json.loads(resp.data)
        token = data.get("token") or data.get("access_token")
        if token:
            auth.token = token
            self._auth_cache[registry] = auth
            return {"Authorization": f"Bearer {token}"}

        return {}

    def _request(
        self,
        method: str,
        registry: str,
        path: str,
        repo: str,
        headers: dict | None = None,
        body: bytes | None = None,
        preload_content: bool = True,
    ) -> urllib3.HTTPResponse:
        """Make an authenticated request to a registry, handling 401 challenges."""
        url = f"https://{registry}{path}"
        hdrs = self._get_auth_header(registry, repo)
        if headers:
            hdrs.update(headers)

        resp = self.http.request(
            method, url, headers=hdrs, body=body, preload_content=preload_content
        )

        if resp.status == 401:
            www_auth = resp.headers.get("WWW-Authenticate", "")
            if www_auth:
                hdrs = self._registry_token_exchange(registry, repo, www_auth)
                if headers:
                    hdrs.update(headers)
                resp = self.http.request(
                    method, url, headers=hdrs, body=body, preload_content=preload_content
                )

        return resp

    # ── Manifest operations ───────────────────────────────────────────

    def get_manifest(self, registry: str, repo: str, reference: str) -> tuple[bytes, str, str]:
        """Pull a manifest by tag or digest. Returns (body, content_type, digest)."""
        resp = self._request(
            "GET",
            registry,
            f"/v2/{repo}/manifests/{reference}",
            repo,
            headers={"Accept": ALL_MANIFEST_TYPES},
        )
        if resp.status != 200:
            raise RegistryError(
                f"Failed to get manifest {repo}:{reference} from {registry} (HTTP {resp.status})"
            )

        content_type = resp.headers.get("Content-Type", MANIFEST_V2)
        digest = resp.headers.get("Docker-Content-Digest", "")
        if not digest:
            digest = "sha256:" + hashlib.sha256(resp.data).hexdigest()

        return resp.data, content_type, digest

    def resolve_single_platform(self, registry: str, repo: str, reference: str) -> dict:
        """Resolve a tag/digest to a single-platform manifest dict.

        For multi-platform images, picks the first platform entry.
        """
        manifest_bytes, content_type, _ = self.get_manifest(registry, repo, reference)
        manifest = json.loads(manifest_bytes)
        if content_type in _MULTI_PLATFORM_TYPES:
            platform_digest = manifest["manifests"][0]["digest"]
            manifest_bytes, _, _ = self.get_manifest(registry, repo, platform_digest)
            manifest = json.loads(manifest_bytes)
        return manifest

    def get_layer_sizes(self, registry: str, repo: str, reference: str) -> tuple[int, list[int]]:
        """Return (total_bytes, [per_layer_size]) for one platform of an image."""
        manifest = self.resolve_single_platform(registry, repo, reference)
        sizes = [layer.get("size", 0) for layer in manifest.get("layers", [])]
        return sum(sizes), sizes

    def get_total_image_size(self, registry: str, repo: str, reference: str) -> int:
        """Return total bytes of all layers across all platforms in an image."""
        manifest_bytes, content_type, _ = self.get_manifest(registry, repo, reference)
        manifest = json.loads(manifest_bytes)
        if content_type in _MULTI_PLATFORM_TYPES:
            total = 0
            for pm in manifest.get("manifests", []):
                plat = self.resolve_single_platform(registry, repo, pm["digest"])
                total += sum(layer.get("size", 0) for layer in plat.get("layers", []))
            return total
        return sum(layer.get("size", 0) for layer in manifest.get("layers", []))

    def put_manifest(
        self, registry: str, repo: str, reference: str, body: bytes, content_type: str
    ) -> str:
        """Push a manifest. Returns the digest."""
        resp = self._request(
            "PUT",
            registry,
            f"/v2/{repo}/manifests/{reference}",
            repo,
            headers={"Content-Type": content_type},
            body=body,
        )
        if resp.status not in (201, 202):
            raise RegistryError(
                f"Failed to push manifest {repo}:{reference} to {registry} (HTTP {resp.status}): {resp.data.decode()}"
            )

        digest = resp.headers.get("Docker-Content-Digest", "")
        if not digest:
            digest = "sha256:" + hashlib.sha256(body).hexdigest()
        return digest

    # ── Blob operations ───────────────────────────────────────────────

    def blob_exists(self, registry: str, repo: str, digest: str) -> bool:
        """Check if a blob already exists in the target registry (HEAD request)."""
        resp = self._request("HEAD", registry, f"/v2/{repo}/blobs/{digest}", repo)
        return resp.status == 200

    def pull_blob(self, registry: str, repo: str, digest: str) -> bytes:
        """Pull a blob by digest. For small blobs (configs). Use pull_blob_stream for layers."""
        resp = self._request("GET", registry, f"/v2/{repo}/blobs/{digest}", repo)
        if resp.status != 200:
            raise RegistryError(
                f"Failed to pull blob {digest} from {registry}/{repo} (HTTP {resp.status})"
            )
        return resp.data

    def pull_blob_stream(self, registry: str, repo: str, digest: str) -> urllib3.HTTPResponse:
        """Pull a blob as a streaming response."""
        resp = self._request(
            "GET", registry, f"/v2/{repo}/blobs/{digest}", repo, preload_content=False
        )
        if resp.status != 200:
            raise RegistryError(
                f"Failed to pull blob {digest} from {registry}/{repo} (HTTP {resp.status})"
            )
        return resp

    def _upload_blob(
        self, registry: str, repo: str, digest: str, body, content_length: int
    ) -> None:
        """Initiate and complete a monolithic blob upload.

        ``body`` may be bytes or a file-like object.
        """
        resp = self._request("POST", registry, f"/v2/{repo}/blobs/uploads/", repo)
        if resp.status not in (200, 202):
            raise RegistryError(
                f"Failed to initiate blob upload to {registry}/{repo} (HTTP {resp.status})"
            )

        location = resp.headers.get("Location", "")
        if not location:
            raise RegistryError("No Location header in upload initiation response")

        sep = "&" if "?" in location else "?"
        url = f"{location}{sep}digest={digest}"
        if url.startswith("/"):
            url = f"https://{registry}{url}"

        hdrs = self._get_auth_header(registry, repo)
        hdrs["Content-Type"] = "application/octet-stream"
        hdrs["Content-Length"] = str(content_length)

        resp = self.http.request("PUT", url, headers=hdrs, body=body)
        if resp.status not in (201, 202):
            raise RegistryError(
                f"Failed to push blob {digest} to {registry}/{repo} (HTTP {resp.status}): {resp.data.decode()}"
            )

    def push_blob(self, registry: str, repo: str, digest: str, data: bytes) -> None:
        """Push a small blob (config) via monolithic upload."""
        self._upload_blob(registry, repo, digest, data, len(data))

    def push_blob_stream(
        self,
        registry: str,
        repo: str,
        digest: str,
        source_stream: urllib3.HTTPResponse,
        content_length: int,
        on_progress=None,
    ) -> None:
        """Push a large blob by streaming from a source response."""
        chunks = []
        while True:
            chunk = source_stream.read(BLOB_CHUNK_SIZE)
            if not chunk:
                break
            chunks.append(chunk)
            if on_progress:
                on_progress(len(chunk))
        source_stream.release_conn()
        data = b"".join(chunks)
        self._upload_blob(registry, repo, digest, data, len(data))

    def _push_blob_from_file(
        self,
        registry: str,
        repo: str,
        digest: str,
        file_path: str,
        content_length: int,
        on_progress=None,
    ) -> None:
        """Push a blob from a local file."""
        with open(file_path, "rb") as f:
            body = _ProgressReader(f, on_progress) if on_progress else f
            self._upload_blob(registry, repo, digest, body, content_length)

    # ── High-level copy / push operations ─────────────────────────────

    def _hash_and_push_layer(
        self, layer_file: str, registry: str, repo: str, on_progress=None
    ) -> tuple[str, int]:
        """Hash a layer file and push it if missing. Returns (digest, size)."""
        with open(layer_file, "rb") as f:
            digest = "sha256:" + hashlib.file_digest(f, "sha256").hexdigest()
        size = os.path.getsize(layer_file)
        if not self.blob_exists(registry, repo, digest):
            self._push_blob_from_file(
                registry, repo, digest, layer_file, size, on_progress=on_progress
            )
        elif on_progress:
            on_progress(size)
        return digest, size

    def copy_image(
        self,
        src_registry: str,
        src_repo: str,
        dst_registry: str,
        dst_repo: str,
        tag: str,
        on_progress=None,
    ) -> str:
        """Copy an image (manifest + all blobs) from source to destination registry.

        Handles both single-platform manifests and multi-platform manifest lists/OCI indexes.
        Returns the manifest digest.
        """
        manifest_bytes, content_type, digest = self.get_manifest(src_registry, src_repo, tag)
        manifest = json.loads(manifest_bytes)

        if content_type in _MULTI_PLATFORM_TYPES:
            for platform_manifest in manifest.get("manifests", []):
                platform_digest = platform_manifest["digest"]
                self._copy_single_manifest(
                    src_registry,
                    src_repo,
                    dst_registry,
                    dst_repo,
                    platform_digest,
                    on_progress=on_progress,
                )
        else:
            self._copy_blobs_for_manifest(
                src_registry, src_repo, dst_registry, dst_repo, manifest, on_progress=on_progress
            )

        result_digest = self.put_manifest(dst_registry, dst_repo, tag, manifest_bytes, content_type)
        return result_digest

    def _copy_single_manifest(
        self,
        src_registry: str,
        src_repo: str,
        dst_registry: str,
        dst_repo: str,
        digest: str,
        on_progress=None,
    ) -> None:
        """Copy a single-platform manifest and its blobs."""
        manifest_bytes, content_type, _ = self.get_manifest(src_registry, src_repo, digest)
        manifest = json.loads(manifest_bytes)

        self._copy_blobs_for_manifest(
            src_registry, src_repo, dst_registry, dst_repo, manifest, on_progress=on_progress
        )
        self.put_manifest(dst_registry, dst_repo, digest, manifest_bytes, content_type)

    def _copy_blobs_for_manifest(
        self,
        src_registry: str,
        src_repo: str,
        dst_registry: str,
        dst_repo: str,
        manifest: dict,
        on_progress=None,
    ) -> None:
        """Copy all blobs (config + layers) referenced by a manifest."""
        config_digest = manifest.get("config", {}).get("digest")
        if config_digest:
            self._copy_blob(
                src_registry,
                src_repo,
                dst_registry,
                dst_repo,
                config_digest,
                on_progress=on_progress,
            )

        layers = manifest.get("layers", [])
        with ThreadPoolExecutor(max_workers=_CONCURRENCY) as pool:
            futures = [
                pool.submit(
                    self._copy_blob,
                    src_registry,
                    src_repo,
                    dst_registry,
                    dst_repo,
                    layer["digest"],
                    on_progress=on_progress,
                )
                for layer in layers
            ]
            for future in futures:
                future.result()

    def _copy_blob(
        self,
        src_registry: str,
        src_repo: str,
        dst_registry: str,
        dst_repo: str,
        digest: str,
        on_progress=None,
    ) -> None:
        """Copy a single blob, skipping if it already exists at the destination."""
        if self.blob_exists(dst_registry, dst_repo, digest):
            logger.debug(
                f"Blob {digest[:19]}... already exists at {dst_registry}/{dst_repo}, skipping"
            )
            if on_progress:
                resp = self._request(
                    "HEAD", dst_registry, f"/v2/{dst_repo}/blobs/{digest}", dst_repo
                )
                size = int(resp.headers.get("Content-Length", 0))
                if size:
                    on_progress(size)
            return

        stream = self.pull_blob_stream(src_registry, src_repo, digest)
        content_length = int(stream.headers.get("Content-Length", 0))
        self.push_blob_stream(
            dst_registry, dst_repo, digest, stream, content_length, on_progress=on_progress
        )

    def extract_blob_to_file(
        self, registry: str, repo: str, digest: str, output_path: str, on_progress=None
    ) -> None:
        """Download a blob to a local file. Used for extracting JARs from engine image layers."""
        resp = self._request(
            "GET", registry, f"/v2/{repo}/blobs/{digest}", repo, preload_content=False
        )
        if resp.status != 200:
            raise RegistryError(f"Failed to pull blob {digest} (HTTP {resp.status})")

        with open(output_path, "wb") as f:
            while True:
                chunk = resp.read(BLOB_CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)
                if on_progress:
                    on_progress(len(chunk))
        resp.release_conn()

    def push_oci_archive(
        self, archive_path: str, registry: str, repo: str, tag: str, on_progress=None
    ) -> str:
        """Push a ``docker save`` OCI archive to a registry. Returns the manifest digest."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with tarfile.open(archive_path, "r") as tar:
                tar.extractall(tmpdir)

            manifest_path = os.path.join(tmpdir, "manifest.json")
            with open(manifest_path) as f:
                archive_manifests = json.load(f)

            if not archive_manifests:
                raise RegistryError(f"No manifests found in {archive_path}")

            entry = archive_manifests[0]

            # Push config blob
            config_file = os.path.join(tmpdir, entry["Config"])
            with open(config_file, "rb") as f:
                config_data = f.read()
            config_digest = "sha256:" + hashlib.sha256(config_data).hexdigest()
            if not self.blob_exists(registry, repo, config_digest):
                self.push_blob(registry, repo, config_digest, config_data)

            # Push layer blobs in parallel
            layer_files = [os.path.join(tmpdir, rel) for rel in entry["Layers"]]
            with ThreadPoolExecutor(max_workers=_CONCURRENCY) as pool:
                futures = [
                    pool.submit(
                        self._hash_and_push_layer, lf, registry, repo, on_progress=on_progress
                    )
                    for lf in layer_files
                ]
                results = [f.result() for f in futures]
            layer_digests = [d for d, _ in results]
            layer_sizes = [s for _, s in results]

            # Build and push manifest
            manifest = {
                "schemaVersion": 2,
                "mediaType": MANIFEST_V2,
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "digest": config_digest,
                    "size": len(config_data),
                },
                "layers": [
                    {
                        "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                        "digest": d,
                        "size": s,
                    }
                    for d, s in zip(layer_digests, layer_sizes, strict=True)
                ],
            }
            manifest_bytes = json.dumps(manifest, indent=2).encode()
            return self.put_manifest(registry, repo, tag, manifest_bytes, MANIFEST_V2)


class RegistryError(Exception):
    """Raised when an OCI registry operation fails."""


class ImageTarget:
    """Target for image loading — local Docker daemon or remote OCI registry.

    When ``client`` is provided, operations go through the OCI registry API.
    When ``client`` is None, operations use the local Docker CLI.
    """

    def __init__(
        self, client=None, registry_host=None, repo_prefix=None, on_progress=None, on_status=None
    ):
        self._client = client
        self._host = registry_host
        self._prefix = repo_prefix
        self._on_progress = on_progress
        self._on_status = on_status

    def with_callbacks(self, on_progress=None, on_status=None):
        """Return a copy of this target with the given progress/status callbacks."""
        return ImageTarget(self._client, self._host, self._prefix, on_progress, on_status)

    @property
    def is_local(self):
        return self._client is None

    @property
    def client(self):
        return self._client

    @property
    def registry_host(self):
        return self._host

    def dst_repo(self, repo):
        """Return the destination repo path (with prefix) for a source repo."""
        return f"{self._prefix}/{repo}" if self._prefix else repo

    def ref(self, repo, tag):
        """Return the full image reference for a repo and tag."""
        if self._host:
            return f"{self._host}/{self.dst_repo(repo)}:{tag}"
        return f"{repo}:{tag}"

    def _run_docker(self, cmd):
        """Run a Docker CLI command, streaming stdout lines to the on_status callback."""
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:
            stripped = line.strip()
            if stripped and self._on_status:
                self._on_status(stripped)
        proc.wait()
        if proc.returncode != 0:
            raise RegistryError(f"{cmd[0]} {cmd[1]} failed (exit code {proc.returncode})")

    def copy_from_hub(self, repo, tag):
        """Copy an image from Docker Hub to this target. Returns digest.

        Raises RegistryError on failure.
        """
        if self._client is not None:
            return self._client.copy_image(
                DOCKER_HUB_REGISTRY,
                repo,
                self._host,
                self.dst_repo(repo),
                tag,
                on_progress=self._on_progress,
            )
        self._run_docker(["docker", "pull", f"{repo}:{tag}"])
        return ""

    def load_archive(self, archive_path, repo, tag):
        """Load an OCI archive to this target. Returns digest.

        Raises RegistryError on failure.
        """
        if self._client is not None:
            return self._client.push_oci_archive(
                archive_path, self._host, self.dst_repo(repo), tag, on_progress=self._on_progress
            )
        self._run_docker(["docker", "load", "-i", archive_path])
        return ""
