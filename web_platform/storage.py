from __future__ import annotations

import os
import tempfile
from functools import lru_cache
from pathlib import Path


def cloudinary_enabled() -> bool:
    if os.environ.get("CLOUDINARY_URL"):
        return True
    required = [
        "CLOUDINARY_CLOUD_NAME",
        "CLOUDINARY_API_KEY",
        "CLOUDINARY_API_SECRET",
    ]
    return all(os.environ.get(key) for key in required)


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
            cloud_name=os.environ["CLOUDINARY_CLOUD_NAME"],
            api_key=os.environ["CLOUDINARY_API_KEY"],
            api_secret=os.environ["CLOUDINARY_API_SECRET"],
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
        return {"configured": False, "ok": False, "error": "Cloudinary env vars are missing."}

    try:
        import cloudinary.api

        result = cloudinary.api.ping()
        return {
            "configured": True,
            "ok": result.get("status") == "ok",
            "status": result.get("status"),
        }
    except Exception as exc:
        return {"configured": True, "ok": False, "error": str(exc)}
