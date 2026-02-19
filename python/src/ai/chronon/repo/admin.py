"""Zipline admin CLI commands for loading images into customer registries and verifying deployments."""

import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile

import click
from rich.console import Console
from rich.table import Table

from ai.chronon.repo.registry_client import (
    DOCKER_HUB_REGISTRY,
    RegistryClient,
    RegistryError,
)
from ai.chronon.repo.utils import upload_to_blob_store

logger = logging.getLogger(__name__)
console = Console()

VALID_CLOUDS = ("gcp", "aws", "azure")


def _sha256(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


@click.group(help="Administrative commands for loading Zipline images and verifying deployments.")
def admin():
    pass


@admin.command("install", help="Install Zipline images into a private registry or the local Docker daemon.")
@click.option(
    "--api-token",
    envvar="ZIPLINE_API_TOKEN",
    default=None,
    help="Zipline API token for Docker Hub access. Can also be set via ZIPLINE_API_TOKEN env var. "
    "Optional when using --bundle or when already authenticated to Docker Hub.",
)
@click.option("--release", default="latest", show_default=True, help="Zipline release to load (e.g. 0.1.42).")
@click.option(
    "--cloud",
    required=True,
    type=click.Choice(VALID_CLOUDS, case_sensitive=False),
    help="Cloud provider variant.",
)
@click.option(
    "--registry",
    required=True,
    help='Target registry URL (e.g. us-docker.pkg.dev/project/repo) or "local" for the local Docker daemon.',
)
@click.option(
    "--artifact-store",
    default=None,
    help="Target store for engine JARs: a blob store URI (e.g. gs://bucket/zipline) or a local filesystem path.",
)
@click.option(
    "--bundle",
    default=None,
    type=click.Path(exists=True),
    help="Path to air-gap tarball (alternative to pulling from Docker Hub).",
)
def load(api_token, release, cloud, registry, artifact_store, bundle):
    """Load Zipline images into a private container registry or the local Docker daemon."""
    is_local = registry == "local"

    if is_local:
        _check_docker_available()

    client = RegistryClient()

    if bundle:
        if is_local:
            results = _load_locally_from_bundle(bundle, release, cloud)
        else:
            results = _load_from_bundle(client, bundle, release, cloud, registry)
    else:
        if is_local:
            results = _load_locally_from_docker_hub(api_token, release, cloud)
        else:
            results = _load_from_docker_hub(client, api_token, release, cloud, registry)

    if artifact_store:
        if bundle and is_local:
            jar_results = _extract_engine_jars_from_bundle(bundle, cloud, release, artifact_store)
        else:
            source_registry = DOCKER_HUB_REGISTRY if is_local else registry
            if is_local and api_token:
                client.authenticate(DOCKER_HUB_REGISTRY, username="ziplineai", password=api_token)
            jar_results = _upload_engine_jars_to_store(client, source_registry, release, cloud, artifact_store)
        results.extend(jar_results)

    _print_summary(results, release, cloud, registry)


def _load_from_docker_hub(client, api_token, release, cloud, target_registry):
    """Pull images from Docker Hub and push to target registry."""
    if api_token:
        client.authenticate(DOCKER_HUB_REGISTRY, username="ziplineai", password=api_token)
    _authenticate_target_registry(client, target_registry)

    results = []

    images = [
        ("hub", f"ziplineai/hub-{cloud}", f"ziplineai/hub-{cloud}"),
        ("frontend", "ziplineai/web-ui", "ziplineai/web-ui"),
        ("engine", f"ziplineai/engine-{cloud}", f"ziplineai/engine-{cloud}"),
    ]
    if cloud == "gcp":
        images.insert(1, ("eval", "ziplineai/eval-gcp", "ziplineai/eval-gcp"))

    for image_type, src_repo, dst_repo in images:
        console.print(f"[bold]Copying {src_repo}:{release}...[/bold]")
        try:
            digest = client.copy_image(DOCKER_HUB_REGISTRY, src_repo, target_registry, dst_repo, release)
            results.append((image_type, f"{target_registry}/{dst_repo}:{release}", digest, "ok"))
        except RegistryError as e:
            results.append((image_type, f"{target_registry}/{dst_repo}:{release}", "", f"FAILED: {e}"))

    return results


def _load_from_bundle(client, bundle_path, release, cloud, target_registry):
    """Load images from an air-gap tarball and push to target registry."""
    _authenticate_target_registry(client, target_registry)

    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        console.print(f"[bold]Extracting bundle {bundle_path}...[/bold]")
        with tarfile.open(bundle_path, "r:gz") as tar:
            tar.extractall(tmpdir)

        image_names = [f"hub-{cloud}", "web-ui", f"engine-{cloud}"]
        if cloud == "gcp":
            image_names.insert(1, "eval-gcp")

        for image_name in image_names:
            archive_path = os.path.join(tmpdir, f"{image_name}.tar")
            if not os.path.exists(archive_path):
                results.append((image_name, "", "", f"FAILED: {archive_path} not found in bundle"))
                continue

            dst_repo = f"ziplineai/{image_name}"
            console.print(f"[bold]Pushing {image_name}:{release} from bundle...[/bold]")
            try:
                digest = _push_oci_archive(client, archive_path, target_registry, dst_repo, release)
                results.append((image_name, f"{target_registry}/{dst_repo}:{release}", digest, "ok"))
            except RegistryError as e:
                results.append((image_name, f"{target_registry}/{dst_repo}:{release}", "", f"FAILED: {e}"))

    return results


def _check_docker_available():
    """Verify that the Docker CLI is on PATH and the daemon is running."""
    if not shutil.which("docker"):
        raise click.UsageError("Docker CLI not found on PATH. Install Docker to use --registry local.")
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True)
    except subprocess.CalledProcessError as exc:
        raise click.UsageError("Docker daemon is not running. Start Docker to use --registry local.") from exc


def _load_locally_from_docker_hub(api_token, release, cloud):
    """Pull Zipline images from Docker Hub into the local Docker daemon."""
    if api_token:
        console.print("[bold]Logging in to Docker Hub...[/bold]")
        proc = subprocess.run(
            ["docker", "login", "-u", "ziplineai", "--password-stdin"],
            input=api_token,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            console.print(f"[yellow]Warning: docker login failed: {proc.stderr.strip()}[/yellow]")

    results = []
    images = [
        ("hub", f"ziplineai/hub-{cloud}"),
        ("frontend", "ziplineai/web-ui"),
    ]
    if cloud == "gcp":
        images.insert(1, ("eval", "ziplineai/eval-gcp"))

    for image_type, repo in images:
        ref = f"{repo}:{release}"
        console.print(f"[bold]Pulling {ref}...[/bold]")
        proc = subprocess.run(["docker", "pull", ref], capture_output=True, text=True)
        if proc.returncode == 0:
            results.append((image_type, ref, "", "ok"))
        else:
            results.append((image_type, ref, "", f"FAILED: {proc.stderr.strip()}"))

    return results


def _load_locally_from_bundle(bundle_path, release, cloud):
    """Load Zipline images from an air-gap tarball into the local Docker daemon."""
    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        console.print(f"[bold]Extracting bundle {bundle_path}...[/bold]")
        with tarfile.open(bundle_path, "r:gz") as tar:
            tar.extractall(tmpdir)

        image_names = [f"hub-{cloud}", "web-ui"]
        if cloud == "gcp":
            image_names.insert(1, "eval-gcp")

        for image_name in image_names:
            archive_path = os.path.join(tmpdir, f"{image_name}.tar")
            if not os.path.exists(archive_path):
                results.append((image_name, "", "", f"FAILED: {archive_path} not found in bundle"))
                continue

            console.print(f"[bold]Loading {image_name}:{release} from bundle...[/bold]")
            proc = subprocess.run(["docker", "load", "-i", archive_path], capture_output=True, text=True)
            if proc.returncode == 0:
                results.append((image_name, f"ziplineai/{image_name}:{release}", "", "ok"))
            else:
                results.append((image_name, f"ziplineai/{image_name}:{release}", "", f"FAILED: {proc.stderr.strip()}"))

    return results


def _extract_engine_jars_from_bundle(bundle_path, cloud, release, artifact_store):
    """Extract engine JARs from a bundle's engine image archive and copy them to the artifact store."""
    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        console.print("[bold]Extracting engine JARs from bundle...[/bold]")
        with tarfile.open(bundle_path, "r:gz") as tar:
            tar.extractall(tmpdir)

        engine_archive = os.path.join(tmpdir, f"engine-{cloud}.tar")
        if not os.path.exists(engine_archive):
            results.append(("engine-jars", artifact_store, "", f"FAILED: engine-{cloud}.tar not found in bundle"))
            return results

        # Extract OCI archive
        oci_dir = os.path.join(tmpdir, "oci")
        with tarfile.open(engine_archive, "r") as tar:
            tar.extractall(oci_dir)

        manifest_path = os.path.join(oci_dir, "manifest.json")
        with open(manifest_path) as f:
            archive_manifests = json.load(f)

        if not archive_manifests:
            results.append(("engine-jars", artifact_store, "", "FAILED: no manifests in engine archive"))
            return results

        entry = archive_manifests[0]

        # Extract JARs/JSON from layers into a separate dir to avoid manifest.json collision
        jars_tmpdir = os.path.join(tmpdir, "jars_extract")
        os.makedirs(jars_tmpdir, exist_ok=True)

        for layer_file_rel in entry["Layers"]:
            layer_path = os.path.join(oci_dir, layer_file_rel)
            try:
                with tarfile.open(layer_path, "r") as layer_tar:
                    for member in layer_tar.getmembers():
                        if member.name.endswith(".jar") or member.name.endswith(".json"):
                            layer_tar.extract(member, jars_tmpdir)
            except tarfile.ReadError:
                continue

        jars_dir = os.path.join(jars_tmpdir, "jars")
        if os.path.isdir(jars_dir):
            for jar_file in os.listdir(jars_dir):
                local_path = os.path.join(jars_dir, jar_file)
                remote_path = f"{artifact_store.rstrip('/')}/release/{release}/jars/{jar_file}"
                try:
                    upload_to_blob_store(local_path, remote_path)
                    results.append(("jar", remote_path, "", "ok"))
                except Exception as e:
                    results.append(("jar", remote_path, "", f"FAILED: {e}"))

    return results


def _push_oci_archive(client, archive_path, registry, repo, tag):
    """Push a `docker save` OCI archive to a registry."""
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
        config_digest = "sha256:" + _sha256(config_data)
        if not client.blob_exists(registry, repo, config_digest):
            client.push_blob(registry, repo, config_digest, config_data)

        # Push layer blobs
        layer_digests = []
        layer_sizes = []
        for layer_file_rel in entry["Layers"]:
            layer_file = os.path.join(tmpdir, layer_file_rel)
            with open(layer_file, "rb") as f:
                layer_data = f.read()
            layer_digest = "sha256:" + _sha256(layer_data)
            layer_size = len(layer_data)
            if not client.blob_exists(registry, repo, layer_digest):
                client.push_blob(registry, repo, layer_digest, layer_data)
            layer_digests.append(layer_digest)
            layer_sizes.append(layer_size)

        # Build and push manifest
        manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
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
        return client.put_manifest(
            registry, repo, tag, manifest_bytes, "application/vnd.docker.distribution.manifest.v2+json"
        )


def _upload_engine_jars_to_store(client, registry, release, cloud, artifact_store):
    """Extract JARs from the engine image and upload to a blob store."""
    results = []
    engine_repo = f"ziplineai/engine-{cloud}"

    console.print(f"[bold]Extracting engine JARs from {engine_repo}:{release}...[/bold]")

    try:
        manifest_bytes, _, _ = client.get_manifest(registry, engine_repo, release)
        manifest = json.loads(manifest_bytes)
    except RegistryError as e:
        results.append(("engine-jars", artifact_store, "", f"FAILED: {e}"))
        return results

    with tempfile.TemporaryDirectory() as tmpdir:
        for layer in manifest.get("layers", []):
            layer_digest = layer["digest"]
            layer_path = os.path.join(tmpdir, "layer.tar")
            client.extract_blob_to_file(registry, engine_repo, layer_digest, layer_path)

            try:
                with tarfile.open(layer_path, "r") as tar:
                    for member in tar.getmembers():
                        if member.name.endswith(".jar") or member.name.endswith(".json"):
                            tar.extract(member, tmpdir)
            except tarfile.ReadError:
                continue

        jars_dir = os.path.join(tmpdir, "jars")
        if os.path.isdir(jars_dir):
            for jar_file in os.listdir(jars_dir):
                local_path = os.path.join(jars_dir, jar_file)
                remote_path = f"{artifact_store.rstrip('/')}/release/{release}/jars/{jar_file}"
                try:
                    upload_to_blob_store(local_path, remote_path)
                    results.append(("jar", remote_path, "", "ok"))
                except Exception as e:
                    results.append(("jar", remote_path, "", f"FAILED: {e}"))

    return results




def _authenticate_target_registry(client, registry):
    """Set up auth for the target registry using ambient cloud credentials."""
    if "pkg.dev" in registry:
        try:
            result = subprocess.run(
                ["gcloud", "auth", "print-access-token"], capture_output=True, text=True, check=True
            )
            client.authenticate(registry, username="oauth2accesstoken", password=result.stdout.strip())
        except (subprocess.CalledProcessError, FileNotFoundError):
            console.print("[yellow]Warning: Could not get gcloud access token. Target registry auth may fail.[/yellow]")
    elif ".dkr.ecr." in registry:
        try:
            import base64

            import boto3

            ecr = boto3.client("ecr")
            token_resp = ecr.get_authorization_token()
            auth_data = token_resp["authorizationData"][0]
            decoded = base64.b64decode(auth_data["authorizationToken"]).decode()
            username, password = decoded.split(":", 1)
            client.authenticate(registry, username=username, password=password)
        except Exception:
            console.print("[yellow]Warning: Could not get ECR auth token. Target registry auth may fail.[/yellow]")
    elif ".azurecr.io" in registry:
        try:
            result = subprocess.run(
                ["az", "acr", "login", "--name", registry.split(".")[0], "--expose-token"],
                capture_output=True,
                text=True,
                check=True,
            )
            token_data = json.loads(result.stdout)
            client.authenticate(
                registry, username="00000000-0000-0000-0000-000000000000", password=token_data["accessToken"]
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            console.print("[yellow]Warning: Could not get ACR token. Target registry auth may fail.[/yellow]")


def _print_summary(results, release, cloud, registry):
    """Print a summary table of the load operation."""
    is_local = registry == "local"
    title = f"Zipline {release} ({cloud}) -> local Docker" if is_local else f"Zipline {release} ({cloud}) -> {registry}"
    table = Table(title=title)
    table.add_column("Type", style="cyan")
    table.add_column("Reference", style="white")
    table.add_column("Status", style="green")

    all_ok = True
    for entry_type, ref, _digest, status in results:
        style = "green" if status == "ok" else "red"
        table.add_row(entry_type, ref, f"[{style}]{status}[/{style}]")
        if status != "ok":
            all_ok = False

    console.print(table)

    if all_ok:
        console.print("\n[bold green]All artifacts loaded successfully.[/bold green]")
        if is_local:
            console.print("\nImages available in local Docker daemon:")
            console.print(f"  ziplineai/hub-{cloud}:{release}")
            if cloud == "gcp":
                console.print(f"  ziplineai/eval-gcp:{release}")
            console.print(f"  ziplineai/web-ui:{release}")
        else:
            console.print("\nFor terraform.tfvars:")
            console.print(f'  hub_image      = "{registry}/ziplineai/hub-{cloud}:{release}"')
            if cloud == "gcp":
                console.print(f'  eval_image     = "{registry}/ziplineai/eval-gcp:{release}"')
            console.print(f'  frontend_image = "{registry}/ziplineai/web-ui:{release}"')
            console.print(f'  engine_image   = "{registry}/ziplineai/engine-{cloud}:{release}"')
    else:
        console.print("\n[bold red]Some artifacts failed to load. See errors above.[/bold red]")
        raise SystemExit(1)


@admin.command(help="Verify a running Zipline deployment.")
@click.option("--hub-url", required=True, help="URL of the running Zipline hub (e.g. https://hub.example.com).")
@click.option("--expected-version", default=None, help="Expected Zipline version (optional).")
def verify(hub_url, expected_version):
    """Check that a Zipline hub is reachable and healthy."""
    import urllib3

    http = urllib3.PoolManager()
    hub_url = hub_url.rstrip("/")

    results = []

    # Check hub health via /debug
    console.print(f"[bold]Checking hub health at {hub_url}/debug...[/bold]")
    try:
        resp = http.request("GET", f"{hub_url}/debug", timeout=10.0)
        if resp.status == 200:
            body = json.loads(resp.data)
            actual_version = body.get("version", "unknown")
            results.append(("Hub Health", f"{hub_url}/debug", "ok", f"version={actual_version}"))

            if expected_version and actual_version != expected_version:
                results.append((
                    "Version Check", "", "WARN", f"Expected {expected_version}, got {actual_version}"
                ))
            elif expected_version:
                results.append(("Version Check", "", "ok", f"matches {expected_version}"))
        else:
            results.append(("Hub Health", f"{hub_url}/debug", "FAIL", f"HTTP {resp.status}"))
    except Exception as e:
        results.append(("Hub Health", f"{hub_url}/debug", "FAIL", str(e)))

    # Check upload API reachability
    console.print(f"[bold]Checking upload API at {hub_url}/upload/v2/diff...[/bold]")
    try:
        resp = http.request("POST", f"{hub_url}/upload/v2/diff", timeout=10.0, body=b"{}")
        if resp.status < 500:
            results.append(("Upload API", f"{hub_url}/upload/v2/diff", "ok", f"HTTP {resp.status} (reachable)"))
        else:
            results.append(("Upload API", f"{hub_url}/upload/v2/diff", "FAIL", f"HTTP {resp.status}"))
    except Exception as e:
        results.append(("Upload API", f"{hub_url}/upload/v2/diff", "FAIL", str(e)))

    # Print results
    table = Table(title=f"Zipline Deployment Verification: {hub_url}")
    table.add_column("Check", style="cyan")
    table.add_column("Endpoint", style="white")
    table.add_column("Status", style="green")
    table.add_column("Detail", style="white")

    all_ok = True
    for check, endpoint, status, detail in results:
        if status == "ok":
            style = "green"
        elif status == "WARN":
            style = "yellow"
        else:
            style = "red"
            all_ok = False
        table.add_row(check, endpoint, f"[{style}]{status}[/{style}]", detail)

    console.print(table)

    if all_ok:
        console.print("\n[bold green]All checks passed.[/bold green]")
    else:
        console.print("\n[bold red]Some checks failed.[/bold red]")
        raise SystemExit(1)
