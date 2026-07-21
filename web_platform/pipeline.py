from __future__ import annotations

import csv
import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

import cv2
import numpy as np


IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".bmp")


@dataclass
class PipelineConfig:
    frame_step: int = 5
    use_dedup: bool = True
    hash_diff_threshold: int = 8
    brightness_diff_threshold: float = 5.0
    pixel_diff_threshold: float = 6.0
    local_dedup_window: int = 5
    local_dedup_max_raw_distance: int = 20
    force_keep_every_n: int | None = None
    low_brightness_max: float = 60.0
    low_brightness_min: float = 0.0
    high_brightness_min: float = 100.0
    min_brightness_gap: float = 40.0
    high_context_before: int = 8
    high_context_after: int = 8
    top_high_per_low: int = 1
    alternatives_per_low: int = 8
    min_good_matches: int = 10
    ratio_test: float = 0.80
    min_inlier_ratio: float = 0.20
    min_pair_inlier_ratio: float = 0.15
    sift_max_size: int = 960
    edge_gamma: float = 0.45
    edge_canny_low: int = 50
    edge_canny_high: int = 150
    edge_diff_score_weight: float = 8.0
    hog_person_score_penalty: float = 25.0
    pair_low_dedup_recent: int = 12
    pair_low_hash_diff_threshold: int = 4
    pair_low_brightness_diff_threshold: float = 8.0
    pair_low_pixel_diff_threshold: float = 8.0


def config_from_form(data: dict) -> PipelineConfig:
    config = PipelineConfig()
    for key, value in data.items():
        if not hasattr(config, key) or value in ("", None):
            continue
        current = getattr(config, key)
        if isinstance(current, bool):
            setattr(config, key, str(value).lower() in {"1", "true", "yes", "on"})
        elif isinstance(current, int) or current is None:
            setattr(config, key, int(value))
        elif isinstance(current, float):
            setattr(config, key, float(value))
    return config


def compute_brightness(img) -> float:
    return float(np.mean(cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)))


def average_hash(img, hash_size: int = 8):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    small = cv2.resize(gray, (hash_size, hash_size))
    return (small > small.mean()).astype(np.uint8).flatten()


def hash_distance(hash1, hash2) -> int:
    return int(np.count_nonzero(hash1 != hash2))


def small_gray_for_diff(img, size=(64, 64)):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return cv2.resize(gray, size).astype(np.float32)


def duplicate_stats(img, ref) -> dict:
    return {
        "hash_dist": hash_distance(average_hash(img), average_hash(ref)),
        "brightness_diff": abs(compute_brightness(img) - compute_brightness(ref)),
        "pixel_diff": float(np.mean(np.abs(small_gray_for_diff(img) - small_gray_for_diff(ref)))),
    }


def is_near_duplicate(img, ref, config: PipelineConfig) -> bool:
    stats = duplicate_stats(img, ref)
    return (
        stats["hash_dist"] <= config.hash_diff_threshold
        and stats["brightness_diff"] <= config.brightness_diff_threshold
        and stats["pixel_diff"] <= config.pixel_diff_threshold
    )


def extract_frames(video_path: Path, frame_dir: Path, frame_step: int) -> list[Path]:
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {video_path}")

    frame_paths: list[Path] = []
    frame_idx = 0
    saved_idx = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        if frame_idx % frame_step == 0:
            path = frame_dir / f"frame_{saved_idx:06d}.png"
            cv2.imwrite(str(path), frame)
            frame_paths.append(path)
            saved_idx += 1
        frame_idx += 1

    cap.release()
    return frame_paths


def remove_duplicate_frames(frame_paths: list[Path], output_dir: Path, config: PipelineConfig) -> list[Path]:
    unique_paths: list[Path] = []
    recent_kept: list[dict] = []

    for raw_idx, path in enumerate(frame_paths):
        img = cv2.imread(str(path))
        if img is None:
            continue

        duplicate = False
        checked = 0
        for kept in reversed(recent_kept):
            raw_distance = raw_idx - kept["raw_idx"]
            if raw_distance > config.local_dedup_max_raw_distance:
                break
            checked += 1
            if is_near_duplicate(img, kept["img"], config):
                duplicate = True
                break
            if checked >= config.local_dedup_window:
                break

        force_keep = (
            config.force_keep_every_n is not None
            and recent_kept
            and raw_idx - recent_kept[-1]["raw_idx"] >= config.force_keep_every_n
        )
        if duplicate and not force_keep:
            continue

        save_path = output_dir / f"frame_{len(unique_paths):06d}.png"
        cv2.imwrite(str(save_path), img)
        unique_paths.append(save_path)
        recent_kept.append({"raw_idx": raw_idx, "img": img, "path": save_path})
        recent_kept = recent_kept[-max(config.local_dedup_window * 4, config.local_dedup_window + 1) :]

    return unique_paths


def compute_all_brightness(frame_paths: list[Path]) -> np.ndarray:
    values = []
    for path in frame_paths:
        img = cv2.imread(str(path))
        values.append(compute_brightness(img) if img is not None else 0.0)
    return np.array(values)


def find_dark_segments(brightness_values: np.ndarray, config: PipelineConfig) -> list[tuple[int, int]]:
    segments = []
    start = None
    for i, brightness in enumerate(brightness_values):
        if brightness <= config.low_brightness_max and start is None:
            start = i
        elif brightness > config.low_brightness_max and start is not None:
            segments.append((start, i - 1))
            start = None
    if start is not None:
        segments.append((start, len(brightness_values) - 1))
    return segments


def resize_max(img, max_size: int):
    h, w = img.shape[:2]
    scale = max_size / max(h, w)
    return cv2.resize(img, (int(w * scale), int(h * scale))) if scale < 1 else img


def clahe_gray(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8)).apply(gray)


def gamma_brighten(gray, gamma: float):
    x = gray.astype(np.float32) / 255.0
    return np.clip(np.power(x, gamma) * 255.0, 0, 255).astype(np.uint8)


def sift_check(img_low, img_high, config: PipelineConfig) -> tuple[bool, int, float]:
    if not hasattr(cv2, "SIFT_create"):
        return False, 0, 0.0
    low_gray = gamma_brighten(clahe_gray(resize_max(img_low, config.sift_max_size)), config.edge_gamma)
    high_gray = clahe_gray(resize_max(img_high, config.sift_max_size))
    sift = cv2.SIFT_create(nfeatures=3000, contrastThreshold=0.01, edgeThreshold=10)
    kp1, des1 = sift.detectAndCompute(low_gray, None)
    kp2, des2 = sift.detectAndCompute(high_gray, None)
    if des1 is None or des2 is None:
        return False, 0, 0.0

    matches = cv2.BFMatcher(cv2.NORM_L2).knnMatch(des1, des2, k=2)
    good = []
    for pair in matches:
        if len(pair) != 2:
            continue
        m, n = pair
        if m.distance < config.ratio_test * n.distance:
            good.append(m)
    if len(good) < config.min_good_matches:
        return False, len(good), 0.0

    pts1 = np.float32([kp1[m.queryIdx].pt for m in good]).reshape(-1, 1, 2)
    pts2 = np.float32([kp2[m.trainIdx].pt for m in good]).reshape(-1, 1, 2)
    _, mask = cv2.findHomography(pts1, pts2, cv2.RANSAC, 5.0)
    if mask is None:
        return False, len(good), 0.0
    inlier_ratio = float(mask.sum()) / len(mask)
    return inlier_ratio >= config.min_inlier_ratio, len(good), inlier_ratio


def edge_structure_check(img_low, img_high, config: PipelineConfig) -> tuple[bool, dict]:
    low = resize_max(img_low, 640)
    high = resize_max(img_high, 640)
    if low.shape[:2] != high.shape[:2]:
        high = cv2.resize(high, (low.shape[1], low.shape[0]))

    low_gray = gamma_brighten(clahe_gray(low), config.edge_gamma)
    high_gray = clahe_gray(high)
    low_edge = cv2.Canny(cv2.GaussianBlur(low_gray, (5, 5), 0), config.edge_canny_low, config.edge_canny_high)
    high_edge = cv2.Canny(cv2.GaussianBlur(high_gray, (5, 5), 0), config.edge_canny_low, config.edge_canny_high)
    kernel = np.ones((3, 3), np.uint8)
    low_mask = cv2.dilate(low_edge, kernel, iterations=1) > 0
    high_mask = cv2.dilate(high_edge, kernel, iterations=1) > 0
    a = low_mask.astype(np.float32).ravel()
    b = high_mask.astype(np.float32).ravel()
    edge_corr = 0.0 if a.std() < 1e-6 or b.std() < 1e-6 else float(np.corrcoef(a, b)[0, 1])
    edge_diff = float(np.mean(np.logical_xor(low_mask, high_mask)))
    edge_overlap = float(np.mean(np.logical_and(low_mask, high_mask)))
    stats = {
        "edge_corr": edge_corr,
        "edge_diff_ratio": edge_diff,
        "edge_overlap": edge_overlap,
        "low_edge_density": float(np.mean(low_mask)),
        "high_edge_density": float(np.mean(high_mask)),
    }
    return edge_diff <= 0.56, stats


def detect_hog_person(img) -> int:
    # HOG is used only as a warning/penalty. Haar face produced too many false positives for this footage.
    hog = cv2.HOGDescriptor()
    hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
    small = resize_max(img, 800)
    _, weights = hog.detectMultiScale(small, winStride=(8, 8), padding=(8, 8), scale=1.05)
    return sum(1 for weight in weights if float(weight) >= 0.20)


def structure_score(
    good_matches: int,
    inlier_ratio: float,
    edge_valid: bool,
    edge_stats: dict,
    low_idx: int,
    high_idx: int,
    hog_hits: int,
    config: PipelineConfig,
) -> float:
    sift_score = min(max(good_matches, 0), 80) * max(inlier_ratio, 0.0)
    edge_score = max(edge_stats["edge_corr"], 0.0) * 20.0
    temporal_score = 4.0 / (1.0 + abs(high_idx - low_idx))
    score = sift_score + edge_score + temporal_score
    if edge_valid:
        score += 2.0
    score -= edge_stats["edge_diff_ratio"] * config.edge_diff_score_weight
    score -= hog_hits * config.hog_person_score_penalty
    return float(score)


def candidate_is_valid(good_matches: int, inlier_ratio: float, edge_valid: bool, score: float, config: PipelineConfig) -> bool:
    return (
        (good_matches >= config.min_good_matches and inlier_ratio >= config.min_pair_inlier_ratio)
        or edge_valid
        or score >= 3.0
    )


def _read_uploaded_image(path: Path):
    # imdecode handles image content independently of the filename extension and
    # avoids OpenCV path handling issues with non-ASCII upload names.
    try:
        encoded = np.fromfile(path, dtype=np.uint8)
    except OSError:
        return None
    return cv2.imdecode(encoded, cv2.IMREAD_COLOR) if encoded.size else None


def _invalid_image_message(side: str, invalid_files: list[tuple[str, int]]) -> str:
    details = ", ".join(f"{name} ({size} bytes)" for name, size in invalid_files[:5])
    if len(invalid_files) > 5:
        details += f", và {len(invalid_files) - 5} file khác"
    return (
        f"Không đọc được ảnh {side}: {details}. "
        "Hãy dùng file PNG, JPEG, WebP, BMP hoặc HEIC/HEIF hợp lệ; AVIF/DNG chưa được hỗ trợ."
    )


def match_image_groups(
    low_paths: list[Path],
    reference_paths: list[Path],
    config: PipelineConfig | None = None,
    low_names: list[str] | None = None,
    reference_names: list[str] | None = None,
    progress_callback: Callable[[dict], None] | None = None,
) -> dict:
    config = config or PipelineConfig()
    low_records = []
    reference_records = []
    invalid_low = []
    invalid_reference = []
    for idx, path in enumerate(low_paths):
        image = _read_uploaded_image(path)
        if image is not None:
            low_records.append((idx, path, image, compute_brightness(image)))
        else:
            invalid_low.append(((low_names or [])[idx] if low_names and idx < len(low_names) else path.name, path.stat().st_size))
    for idx, path in enumerate(reference_paths):
        image = _read_uploaded_image(path)
        if image is not None:
            reference_records.append((idx, path, image, compute_brightness(image)))
        else:
            invalid_reference.append(
                ((reference_names or [])[idx] if reference_names and idx < len(reference_names) else path.name, path.stat().st_size)
            )
    if invalid_low:
        raise ValueError(_invalid_image_message("LOW", invalid_low))
    if invalid_reference:
        raise ValueError(_invalid_image_message("reference", invalid_reference))

    total_candidates = len(low_records) * len(reference_records)
    reference_hog_hits = {}
    for completed, (reference_idx, _, reference_img, _) in enumerate(reference_records, start=1):
        reference_hog_hits[reference_idx] = detect_hog_person(reference_img)
        if progress_callback:
            progress_callback(
                {
                    "status": "analyzing_references",
                    "completed_references": completed,
                    "total_references": len(reference_records),
                    "completed_comparisons": 0,
                    "total_comparisons": total_candidates,
                }
            )

    candidates = []
    completed_comparisons = 0
    for low_idx, low_path, low_img, low_brightness in low_records:
        for reference_idx, reference_path, reference_img, reference_brightness in reference_records:
            low_name = low_names[low_idx] if low_names and low_idx < len(low_names) else low_path.name
            reference_name = (
                reference_names[reference_idx]
                if reference_names and reference_idx < len(reference_names)
                else reference_path.name
            )
            if progress_callback:
                progress_callback(
                    {
                        "status": "matching",
                        "completed_comparisons": completed_comparisons,
                        "total_comparisons": total_candidates,
                        "current_low": low_name,
                        "current_reference": reference_name,
                    }
                )
            _, good_matches, inlier_ratio = sift_check(low_img, reference_img, config)
            edge_valid, edge_stats = edge_structure_check(low_img, reference_img, config)
            hog_hits = reference_hog_hits[reference_idx]
            score = structure_score(
                good_matches,
                inlier_ratio,
                edge_valid,
                edge_stats,
                0,
                0,
                hog_hits,
                config,
            )
            brightness_gap = reference_brightness - low_brightness
            candidates.append(
                {
                    "low_idx": low_idx,
                    "high_idx": reference_idx,
                    "low_path": str(low_path),
                    "high_path": str(reference_path),
                    "low_brightness": low_brightness,
                    "high_brightness": reference_brightness,
                    "brightness_gap": brightness_gap,
                    "score": score,
                    "good_matches": good_matches,
                    "inlier_ratio": inlier_ratio,
                    "hog_hits": hog_hits,
                    "valid": brightness_gap >= config.min_brightness_gap
                    and candidate_is_valid(good_matches, inlier_ratio, edge_valid, score, config),
                }
            )
            completed_comparisons += 1
            if progress_callback:
                progress_callback(
                    {
                        "status": "matching",
                        "completed_comparisons": completed_comparisons,
                        "total_comparisons": total_candidates,
                        "current_low": low_name,
                        "current_reference": reference_name,
                    }
                )

    assigned_lows = set()
    assigned_references = set()
    selected = []
    for candidate in sorted(candidates, key=lambda item: (item["valid"], item["score"]), reverse=True):
        if candidate["low_idx"] in assigned_lows or candidate["high_idx"] in assigned_references:
            continue
        selected.append(candidate)
        assigned_lows.add(candidate["low_idx"])
        assigned_references.add(candidate["high_idx"])
        if len(selected) >= min(len(low_records), len(reference_records)):
            break

    pairs = []
    for pair_id, item in enumerate(selected):
        alternatives = sorted(
            (candidate for candidate in candidates if candidate["low_idx"] == item["low_idx"]),
            key=lambda candidate: (candidate["valid"], candidate["score"]),
            reverse=True,
        )[: config.alternatives_per_low]
        pairs.append(
            {
                "pair_id": pair_id,
                "segment_id": "direct_match",
                "mode": "direct_upload",
                "low_idx": item["low_idx"],
                "high_idx": item["high_idx"],
                "selected_high_idx": item["high_idx"],
                "low_file_index": item["low_idx"],
                "high_file_index": item["high_idx"],
                "low_name": (low_names or [])[item["low_idx"]] if low_names else low_paths[item["low_idx"]].name,
                "high_name": (reference_names or [])[item["high_idx"]]
                if reference_names
                else reference_paths[item["high_idx"]].name,
                "low_path": item["low_path"],
                "high_path": item["high_path"],
                "low_brightness": item["low_brightness"],
                "high_brightness": item["high_brightness"],
                "brightness_gap": item["brightness_gap"],
                "score": item["score"],
                "good_matches": item["good_matches"],
                "inlier_ratio": item["inlier_ratio"],
                "hog_hits": item["hog_hits"],
                "accepted": True,
                "alternatives": [
                    {
                        "high_idx": alternative["high_idx"],
                        "high_path": alternative["high_path"],
                        "high_brightness": alternative["high_brightness"],
                        "brightness_gap": alternative["brightness_gap"],
                        "score": alternative["score"],
                        "good_matches": alternative["good_matches"],
                        "inlier_ratio": alternative["inlier_ratio"],
                        "hog_hits": alternative["hog_hits"],
                    }
                    for alternative in alternatives
                ],
            }
        )

    return {
        "pairs": pairs,
        "low_options": [
            {
                "idx": idx,
                "file_index": idx,
                "name": low_names[idx] if low_names and idx < len(low_names) else path.name,
                "path": str(path),
                "brightness": brightness,
                "direct_upload": True,
            }
            for idx, path, _, brightness in low_records
        ],
        "high_options": [
            {
                "idx": idx,
                "file_index": idx,
                "name": reference_names[idx] if reference_names and idx < len(reference_names) else path.name,
                "path": str(path),
                "brightness": brightness,
                "direct_upload": True,
            }
            for idx, path, _, brightness in reference_records
        ],
        "summary": {
            "mode": "direct_group_matching",
            "low_images": len(low_records),
            "reference_images": len(reference_records),
            "candidates": len(candidates),
            "selected_pairs": len(pairs),
        },
    }


def collect_decisions(frame_paths: list[Path], brightness_values: np.ndarray, config: PipelineConfig) -> list[dict]:
    decisions: list[dict] = []
    for segment_id, (start_idx, end_idx) in enumerate(find_dark_segments(brightness_values, config)):
        low_indices = [i for i in range(start_idx, end_idx + 1) if brightness_values[i] <= config.low_brightness_max]
        highs = (
            [(i, "before") for i in range(max(0, start_idx - config.high_context_before), start_idx)]
            + [(i, "after") for i in range(end_idx + 1, min(len(frame_paths), end_idx + 1 + config.high_context_after))]
        )
        highs = [(i, side) for i, side in highs if brightness_values[i] >= config.high_brightness_min]

        for low_idx in low_indices:
            low_img = cv2.imread(str(frame_paths[low_idx]))
            if low_img is None:
                continue
            low_b = float(brightness_values[low_idx])
            for high_idx, high_side in highs:
                high_img = cv2.imread(str(frame_paths[high_idx]))
                if high_img is None:
                    continue
                high_b = float(brightness_values[high_idx])
                gap = high_b - low_b
                record = {
                    "segment_id": segment_id,
                    "low_idx": low_idx,
                    "high_idx": high_idx,
                    "high_side": high_side,
                    "low_path": str(frame_paths[low_idx]),
                    "high_path": str(frame_paths[high_idx]),
                    "low_brightness": low_b,
                    "high_brightness": high_b,
                    "brightness_gap": gap,
                    "status": "accepted",
                    "reason": "accepted",
                }
                if low_b < config.low_brightness_min:
                    record.update(status="rejected", reason="low too dark", score=0.0)
                    decisions.append(record)
                    continue
                if gap < config.min_brightness_gap:
                    record.update(status="rejected", reason="brightness gap", score=0.0)
                    decisions.append(record)
                    continue

                sift_valid, good_matches, inlier_ratio = sift_check(low_img, high_img, config)
                edge_valid, edge_stats = edge_structure_check(low_img, high_img, config)
                hog_hits = detect_hog_person(high_img)
                score = structure_score(good_matches, inlier_ratio, edge_valid, edge_stats, low_idx, high_idx, hog_hits, config)
                record.update(
                    sift_valid=sift_valid,
                    good_matches=good_matches,
                    inlier_ratio=inlier_ratio,
                    edge_valid=edge_valid,
                    edge_stats=edge_stats,
                    hog_hits=hog_hits,
                    score=score,
                )
                if not candidate_is_valid(good_matches, inlier_ratio, edge_valid, score, config):
                    record.update(status="rejected", reason="structure")
                decisions.append(record)
    return decisions


def is_duplicate_output_low(img, saved_low_refs: list, config: PipelineConfig) -> bool:
    for ref in reversed(saved_low_refs[-config.pair_low_dedup_recent :]):
        stats = duplicate_stats(img, ref)
        if (
            stats["hash_dist"] <= config.pair_low_hash_diff_threshold
            and stats["brightness_diff"] <= config.pair_low_brightness_diff_threshold
            and stats["pixel_diff"] <= config.pair_low_pixel_diff_threshold
        ):
            return True
    return False


def select_final_pair_decisions(decisions: list[dict], config: PipelineConfig) -> tuple[list[dict], list[dict]]:
    selected: list[dict] = []
    skipped: list[dict] = []
    saved_low_refs: list = []
    accepted = [d for d in decisions if d["status"] == "accepted"]
    grouped: dict[tuple[int, int], list[dict]] = {}
    for item in accepted:
        grouped.setdefault((item["segment_id"], item["low_idx"]), []).append(item)

    for key in sorted(grouped.keys()):
        for item in sorted(grouped[key], key=lambda d: d["score"], reverse=True)[: config.top_high_per_low]:
            low_img = cv2.imread(item["low_path"])
            if low_img is None:
                continue
            copy = dict(item)
            if is_duplicate_output_low(low_img, saved_low_refs, config):
                copy.update(status="final_rejected", reason="duplicate output low")
                skipped.append(copy)
                continue
            copy.update(status="selected", reason="selected")
            selected.append(copy)
            saved_low_refs.append(low_img)
    return selected, skipped


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        if not rows:
            return
        fieldnames = sorted({key for row in rows for key in row.keys() if key != "edge_stats"})
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            clean = {key: value for key, value in row.items() if key in fieldnames}
            writer.writerow(clean)


def save_manifest(output_dir: Path, payload: dict) -> None:
    (output_dir / "manifest.json").write_text(json.dumps(payload, indent=2))


def load_manifest(job_dir: Path) -> dict:
    return json.loads((job_dir / "manifest.json").read_text())


def run_pipeline(video_path: Path, output_dir: Path, config: PipelineConfig | None = None) -> dict:
    config = config or PipelineConfig()
    if output_dir.exists():
        shutil.rmtree(output_dir)

    frame_dir = output_dir / "frames_raw"
    dedup_dir = output_dir / "frames_dedup"
    metadata_dir = output_dir / "metadata"
    pair_low_dir = output_dir / "pairs" / "low"
    pair_high_dir = output_dir / "pairs" / "high"
    for folder in [frame_dir, dedup_dir, metadata_dir, pair_low_dir, pair_high_dir]:
        folder.mkdir(parents=True, exist_ok=True)

    raw_frame_paths = extract_frames(video_path, frame_dir, config.frame_step)
    if not raw_frame_paths:
        raise RuntimeError("No frames extracted.")
    frame_paths = remove_duplicate_frames(raw_frame_paths, dedup_dir, config) if config.use_dedup else raw_frame_paths
    brightness_values = compute_all_brightness(frame_paths)
    decisions = collect_decisions(frame_paths, brightness_values, config)
    selected, duplicate_skips = select_final_pair_decisions(decisions, config)
    low_options = [
        {
            "idx": idx,
            "path": str(path),
            "brightness": float(brightness_values[idx]),
        }
        for idx, path in enumerate(frame_paths)
        if brightness_values[idx] <= config.low_brightness_max
    ]
    high_options = [
        {
            "idx": idx,
            "path": str(path),
            "brightness": float(brightness_values[idx]),
        }
        for idx, path in enumerate(frame_paths)
        if brightness_values[idx] >= config.high_brightness_min
    ]

    accepted_by_low: dict[tuple[int, int], list[dict]] = {}
    for item in decisions:
        if item["status"] == "accepted":
            accepted_by_low.setdefault((item["segment_id"], item["low_idx"]), []).append(item)

    pairs = []
    for pair_id, item in enumerate(selected):
        low_img = cv2.imread(item["low_path"])
        high_img = cv2.imread(item["high_path"])
        if low_img is None or high_img is None:
            continue
        low_out = pair_low_dir / f"pair_{pair_id:06d}.png"
        high_out = pair_high_dir / f"pair_{pair_id:06d}.png"
        cv2.imwrite(str(low_out), low_img)
        cv2.imwrite(str(high_out), high_img)

        alternatives = sorted(
            accepted_by_low.get((item["segment_id"], item["low_idx"]), []),
            key=lambda d: d["score"],
            reverse=True,
        )[: config.alternatives_per_low]
        pairs.append(
            {
                "pair_id": pair_id,
                "segment_id": item["segment_id"],
                "low_idx": item["low_idx"],
                "high_idx": item["high_idx"],
                "selected_high_idx": item["high_idx"],
                "low_path": item["low_path"],
                "high_path": item["high_path"],
                "low_brightness": item["low_brightness"],
                "high_brightness": item["high_brightness"],
                "brightness_gap": item["brightness_gap"],
                "score": item["score"],
                "good_matches": item.get("good_matches", 0),
                "inlier_ratio": item.get("inlier_ratio", 0.0),
                "hog_hits": item.get("hog_hits", 0),
                "accepted": True,
                "alternatives": [
                    {
                        "high_idx": alt["high_idx"],
                        "high_path": alt["high_path"],
                        "high_brightness": alt["high_brightness"],
                        "brightness_gap": alt["brightness_gap"],
                        "score": alt["score"],
                        "good_matches": alt.get("good_matches", 0),
                        "inlier_ratio": alt.get("inlier_ratio", 0.0),
                        "hog_hits": alt.get("hog_hits", 0),
                    }
                    for alt in alternatives
                ],
            }
        )

    write_csv(decisions, metadata_dir / "candidates.csv")
    write_csv(selected, metadata_dir / "selected_decisions.csv")
    write_csv(duplicate_skips, metadata_dir / "duplicate_skips.csv")
    manifest = {
        "config": asdict(config),
        "video_path": str(video_path),
        "frame_paths": [str(path) for path in frame_paths],
        "low_options": low_options,
        "high_options": high_options,
        "pairs": pairs,
        "summary": {
            "raw_frames": len(raw_frame_paths),
            "dedup_frames": len(frame_paths),
            "candidates": len(decisions),
            "selected_pairs": len(pairs),
            "duplicate_skips": len(duplicate_skips),
        },
    }
    save_manifest(output_dir, manifest)
    return manifest


def save_reviewed_pairs(job_dir: Path, reviewed_pairs: list[dict], dataset_dir: Path) -> int:
    low_dir = dataset_dir / "low"
    high_dir = dataset_dir / "high"
    metadata_dir = dataset_dir / "metadata"
    for folder in [low_dir, high_dir, metadata_dir]:
        folder.mkdir(parents=True, exist_ok=True)

    existing = [p for p in low_dir.iterdir() if p.suffix.lower() in IMAGE_EXTENSIONS]
    start_idx = len(existing)
    rows = []
    copied = 0

    for item in reviewed_pairs:
        if not item.get("accepted", True):
            continue
        low_path = Path(item["low_path"])
        high_path = Path(item["high_path"])
        if not low_path.exists() or not high_path.exists():
            continue
        dst_idx = start_idx + copied
        dst_low = low_dir / f"pair_{dst_idx:06d}_low.png"
        dst_high = high_dir / f"pair_{dst_idx:06d}_high.png"
        shutil.copy2(low_path, dst_low)
        shutil.copy2(high_path, dst_high)
        rows.append(
            {
                "pair_id": dst_idx,
                "source_pair_id": item.get("pair_id"),
                "job_id": item.get("job_id"),
                "submitted_by": item.get("submitted_by"),
                "objective_pairs": item.get("objective_pairs"),
                "low_path": str(low_path),
                "high_path": str(high_path),
                "saved_low": str(dst_low),
                "saved_high": str(dst_high),
                "score": item.get("score"),
                "human_decision": item.get("human_decision", "accepted"),
            }
        )
        copied += 1

    write_csv(rows, metadata_dir / "reviewed_pairs.csv")
    return copied
