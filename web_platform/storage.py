from __future__ import annotations

import os
import tempfile
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def first_env(*keys: str) -> str | None:
    for key in keys:
        value = os.environ.get(key)
        if value:
            return value
    return None


def cloudinary_enabled() -> bool:
    if os.environ.get("CLOUDINARY_URL"):
        return True
    return not missing_cloudinary_vars()


def missing_cloudinary_vars() -> list[str]:
    if os.environ.get("CLOUDINARY_URL"):
        return []
    missing = []
    if not first_env("CLOUDINARY_CLOUD_NAME", "CLOUD_NAME", "CLOUDINARY_NAME"):
        missing.append("CLOUDINARY_CLOUD_NAME")
    if not first_env("CLOUDINARY_API_KEY", "CLOUDINARY_KEY"):
        missing.append("CLOUDINARY_API_KEY")
    if not first_env("CLOUDINARY_API_SECRET", "CLOUDINARY_SECRET", "CLOUDINARY_API_SECRET_KEY"):
        missing.append("CLOUDINARY_API_SECRET")
    return missing


def cloudinary_env_diagnostics() -> dict:
    api_key = first_env("CLOUDINARY_API_KEY", "CLOUDINARY_KEY") or ""
    return {
        "cloudinary_url_present": bool(os.environ.get("CLOUDINARY_URL")),
        "cloud_name_present": bool(first_env("CLOUDINARY_CLOUD_NAME", "CLOUD_NAME", "CLOUDINARY_NAME")),
        "cloud_name": first_env("CLOUDINARY_CLOUD_NAME", "CLOUD_NAME", "CLOUDINARY_NAME"),
        "api_key_present": bool(api_key),
        "api_key_last4": api_key[-4:] if api_key else None,
        "api_secret_present": bool(first_env("CLOUDINARY_API_SECRET", "CLOUDINARY_SECRET", "CLOUDINARY_API_SECRET_KEY")),
        "folder": storage_prefix(),
    }


def storage_prefix() -> str:
    return os.environ.get("CLOUDINARY_FOLDER", "lowlight_datasets").strip("/")


def cleanup_after_save() -> bool:
    value = os.environ.get("CLEANUP_AFTER_SAVE")
    if value is None:
        return cloudinary_enabled()
    return value.lower() in {"1", "true", "yes", "on"}


@lru_cache(maxsize=1)
def configure_cloudinary():
    if not cloudinary_enabled():
        return False

    import cloudinary

    if os.environ.get("CLOUDINARY_URL"):
        cloudinary.config(secure=True)
    else:
        cloudinary.config(
            cloud_name=first_env("CLOUDINARY_CLOUD_NAME", "CLOUD_NAME", "CLOUDINARY_NAME"),
            api_key=first_env("CLOUDINARY_API_KEY", "CLOUDINARY_KEY"),
            api_secret=first_env("CLOUDINARY_API_SECRET", "CLOUDINARY_SECRET", "CLOUDINARY_API_SECRET_KEY"),
            secure=True,
        )
    return True


def public_id_from_key(key: str) -> str:
    path = Path(key)
    if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp", ".csv", ".json", ".txt"}:
        key = str(path.with_suffix(""))
    return key.strip("/")


def upload_file(path: Path, key: str, content_type: str = "application/octet-stream") -> str:
    if not configure_cloudinary():
        raise RuntimeError("Cloudinary is not configured.")

    import cloudinary.uploader

    is_image = content_type.startswith("image/")
    result = cloudinary.uploader.upload(
        str(path),
        public_id=public_id_from_key(key),
        resource_type="image" if is_image else "raw",
        overwrite=True,
        invalidate=True,
    )
    return result["secure_url"]


def upload_bytes(payload: bytes, key: str, content_type: str = "application/octet-stream") -> str:
    if not configure_cloudinary():
        raise RuntimeError("Cloudinary is not configured.")

    suffix = Path(key).suffix or ".bin"
    with tempfile.NamedTemporaryFile(suffix=suffix) as handle:
        handle.write(payload)
        handle.flush()
        return upload_file(Path(handle.name), key, content_type)


def cloudinary_health() -> dict:
    if not configure_cloudinary():
        missing = missing_cloudinary_vars()
        return {
            "configured": False,
            "ok": False,
            "error": "Cloudinary env vars are missing.",
            "missing": missing,
            "env": cloudinary_env_diagnostics(),
        }

    try:
        import cloudinary.api

        result = cloudinary.api.ping()
        return {
            "configured": True,
            "ok": result.get("status") == "ok",
            "status": result.get("status"),
            "env": cloudinary_env_diagnostics(),
        }
    except Exception as exc:
        return {"configured": True, "ok": False, "error": str(exc), "env": cloudinary_env_diagnostics()}
