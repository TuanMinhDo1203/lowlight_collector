from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path, PurePosixPath

from dotenv import load_dotenv

load_dotenv()

RCLONE_TIMEOUT_SECONDS = 300
IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".bmp"}


def rclone_remote() -> str:
    return os.environ.get("RCLONE_REMOTE", "").strip().rstrip(":")


def rclone_config_path() -> Path | None:
    value = os.environ.get("RCLONE_CONFIG", "").strip()
    return Path(value).expanduser() if value else None


def rclone_dataset_root() -> str:
    return os.environ.get("RCLONE_DATASET_ROOT", "").strip().strip("/")


def remote_dataset_root() -> str:
    remote = rclone_remote()
    root = rclone_dataset_root()
    if not remote or not root:
        raise RuntimeError("Rclone storage is not configured.")
    return f"{remote}:{root}"


def missing_rclone_settings() -> list[str]:
    missing = []
    if not rclone_remote():
        missing.append("RCLONE_REMOTE")
    if not rclone_config_path():
        missing.append("RCLONE_CONFIG")
    if not rclone_dataset_root():
        missing.append("RCLONE_DATASET_ROOT")
    return missing


def rclone_enabled() -> bool:
    config = rclone_config_path()
    return not missing_rclone_settings() and config is not None and config.is_file() and shutil.which("rclone") is not None


def _safe_error(stderr: str) -> str:
    message = (stderr or "Unknown rclone error").strip()
    config = rclone_config_path()
    if config:
        message = message.replace(str(config), "[rclone-config]")
    return message[-1000:]


def _run_rclone(arguments: list[str], timeout: int = RCLONE_TIMEOUT_SECONDS) -> subprocess.CompletedProcess[str]:
    config = rclone_config_path()
    if not rclone_enabled() or config is None:
        raise RuntimeError("Rclone is not ready. Check RCLONE_REMOTE, RCLONE_CONFIG, RCLONE_DATASET_ROOT, and the rclone binary.")

    try:
        return subprocess.run(
            ["rclone", *arguments, "--config", str(config)],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Rclone command timed out after {timeout} seconds.") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Rclone command failed: {_safe_error(exc.stderr)}") from exc


def build_remote_path(relative_path: str) -> str:
    relative = PurePosixPath(relative_path.strip().lstrip("/"))
    if not relative.parts or ".." in relative.parts:
        raise ValueError("Invalid remote path.")
    return f"{remote_dataset_root()}/{relative.as_posix()}"


def validate_remote_path(remote_path: str) -> str:
    allowed_prefix = f"{remote_dataset_root()}/"
    if not remote_path.startswith(allowed_prefix):
        raise ValueError("Remote path is outside the configured dataset root.")
    relative = PurePosixPath(remote_path[len(allowed_prefix) :])
    if not relative.parts or ".." in relative.parts:
        raise ValueError("Invalid remote path.")
    return remote_path


def upload_file(path: Path, remote_relative_path: str, content_type: str = "application/octet-stream") -> str:
    del content_type  # Kept for compatibility with the existing app call sites.
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"Upload source does not exist: {source}")

    remote_path = build_remote_path(remote_relative_path)
    with tempfile.TemporaryDirectory(prefix="llie-rclone-upload-") as temp_dir:
        temp_path = Path(temp_dir) / source.name
        shutil.copy2(source, temp_path)
        _run_rclone(["copyto", str(temp_path), remote_path])
    return remote_path


def download_file(remote_path: str) -> Path:
    validated_path = validate_remote_path(remote_path)
    suffix = Path(validated_path).suffix or ".bin"
    handle = tempfile.NamedTemporaryFile(prefix="llie-rclone-read-", suffix=suffix, delete=False)
    temp_path = Path(handle.name)
    handle.close()
    try:
        _run_rclone(["copyto", validated_path, str(temp_path)])
        return temp_path
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def _pair_key(filename: str, suffixes: tuple[str, ...]) -> str:
    stem = Path(filename).stem
    for suffix in suffixes:
        if stem.endswith(suffix):
            return stem[: -len(suffix)]
    return stem


def _list_remote_files(relative_path: str) -> list[str]:
    try:
        result = _run_rclone(["lsjson", build_remote_path(relative_path), "--recursive", "--files-only"], timeout=60)
    except RuntimeError as exc:
        if "directory not found" in str(exc).lower():
            return []
        raise
    try:
        items = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise RuntimeError("Rclone returned invalid JSON while counting dataset files.") from exc
    return [
        str(item["Path"])
        for item in items
        if not item.get("IsDir") and item.get("Path") and Path(str(item["Path"])).suffix.lower() in IMAGE_SUFFIXES
    ]


def dataset_stats() -> dict:
    low_files = _list_remote_files("raw/low")
    reference_files = _list_remote_files("raw/reference")
    low_by_key: dict[str, list[str]] = {}
    reference_by_key: dict[str, list[str]] = {}
    for name in low_files:
        key = _pair_key(name, ("_low",))
        low_by_key.setdefault(key, []).append(build_remote_path(f"raw/low/{name}"))
    for name in reference_files:
        key = _pair_key(name, ("_reference", "_high"))
        reference_by_key.setdefault(key, []).append(build_remote_path(f"raw/reference/{name}"))
    low_keys = set(low_by_key)
    reference_keys = set(reference_by_key)
    paired_keys = low_keys & reference_keys
    return {
        "pair_count": len(paired_keys),
        "low_count": len(low_files),
        "reference_count": len(reference_files),
        "unmatched_low_count": len(low_keys - reference_keys),
        "unmatched_reference_count": len(reference_keys - low_keys),
        "paired_files": {
            key: {
                "low": sorted(low_by_key[key]),
                "reference": sorted(reference_by_key[key]),
            }
            for key in paired_keys
        },
    }


def rclone_health() -> dict:
    missing = missing_rclone_settings()
    config = rclone_config_path()
    diagnostics = {
        "remote": rclone_remote() or None,
        "dataset_root": rclone_dataset_root() or None,
        "config_present": bool(config and config.is_file()),
        "binary_present": shutil.which("rclone") is not None,
    }
    if missing:
        return {"configured": False, "ok": False, "error": "Rclone environment variables are missing.", "missing": missing, "env": diagnostics}
    if not diagnostics["config_present"]:
        return {"configured": True, "ok": False, "error": "Rclone config file does not exist.", "env": diagnostics}
    if not diagnostics["binary_present"]:
        return {"configured": True, "ok": False, "error": "Rclone binary is not installed or not on PATH.", "env": diagnostics}

    try:
        _run_rclone(["lsd", f"{rclone_remote()}:", "--max-depth", "1"], timeout=30)
        return {"configured": True, "ok": True, "status": "ok", "env": diagnostics}
    except Exception as exc:
        return {"configured": True, "ok": False, "error": str(exc), "env": diagnostics}
