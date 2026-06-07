# Capstone Project Context

## Overview

This project builds a data creation pipeline for low-light image enhancement.
The goal is to extract low-light / normal-light frame pairs from uploaded videos.

The pipeline is intentionally human-in-the-loop:

- Automatically extract and rank candidate pairs.
- Keep useful metadata for debugging and later web review.
- Let users manually accept, reject, or rematch low/high frames.

The main workspace is:

```text
/home/bendo/Desktop/Ben/CAPSTONE
```

## Main Files

```text
dataset.ipynb
web_platform/pipeline.py
web_platform/app.py
web_platform/README.md
```

`dataset.ipynb` is the notebook debugging workflow.
`web_platform` is the local web app for upload, review, rematch, and save.

## Notebook Workflow

`dataset.ipynb` is phase-based. The definition cell should not auto-run the
full pipeline or auto-reset output folders.

Current phases:

1. **Phase 0 - Reset Output Folders**
2. **Phase 1 - Extract Raw Frames**
3. **Phase 2 - Local Dedup**
4. **Phase 3 - Brightness And Dark Segments**
5. **Phase 4 - Inspect One Dark Segment**
6. **Phase 5 - Pair Decision Debug**
7. **Phase 6 - Save Final Pairs**
8. **Phase 7 - Visualize Saved Pairs And Quality Report**

Important notebook behavior:

- Very dark low frames are kept:

```python
LOW_BRIGHTNESS_MIN = 0.0
```

- Person detector is warning/metadata only because Haar/HOG caused false
positives on correct door frames:

```python
USE_PERSON_FILTER = True
REQUIRE_PERSON_FREE_HIGH = False
```

- Final selection uses one source of truth:

```python
select_final_pair_decisions(decisions)
```

Both Phase 5 debug and Phase 6 saving should use this same logic.

## Output Structure

The target output structure is:

```text
paired_output/
├── frames_raw/
├── frames_filtered/
├── frames_dedup/
├── debug/
│   ├── brightness_curve/
│   ├── segments/
│   ├── candidates/
│   ├── decisions/
│   └── visualization/
├── pairs/
│   ├── low/
│   └── high/
└── metadata/
    ├── raw_frames.csv
    ├── frames.csv
    ├── segments.csv
    ├── candidates.csv
    ├── decisions.csv
    ├── selected_decisions.csv
    ├── duplicate_skips.csv
    ├── final_pairs.csv
    └── quality_report.csv
```

## Pairing Logic

The pairing logic is segment-based:

1. Extract frames from video.
2. Locally deduplicate neighboring frames.
3. Compute brightness and frame quality metadata.
4. Detect dark segments.
5. For each low frame inside a dark segment, search high candidates both:
   - before the dark segment
   - after the dark segment
6. Score candidates using:
   - brightness gap
   - SIFT feature matches / inlier ratio
   - edge structure similarity
   - temporal distance
   - HOG person penalty
7. Select top high per low.
8. Deduplicate output low frames.
9. Save selected pairs and metadata.

Useful params:

```python
FRAME_STEP = 5
LOW_BRIGHTNESS_MAX = 60
LOW_BRIGHTNESS_MIN = 0.0
HIGH_BRIGHTNESS_MIN = 100
MIN_BRIGHTNESS_GAP = 40
HIGH_CONTEXT_BEFORE = 8
HIGH_CONTEXT_AFTER = 8
TOP_HIGH_PER_LOW = 1
EDGE_DIFF_SCORE_WEIGHT = 8.0
HOG_PERSON_SCORE_PENALTY = 25.0
```

## Known Debug Findings

- The door-opening dark scene was not missing; it was in a later dark segment.
  Use `overview_dark_segments()` before inspecting a segment.
- Some correct high frames were falsely marked as person by Haar face detection.
  This is why person filtering should not hard reject by default.
- Some very dark frames had brightness around `1.36` but still matched
  structurally, so `LOW_BRIGHTNESS_MIN` should stay `0.0`.
- Raw accepted candidates can contain duplicates. Debug should show
  `selected_decisions`, not raw accepted candidates, when checking final pairs.
- Auto matching is not expected to be perfect. The web platform must support
  manual rematching.

## Web Platform

The web app supports:

- Upload video.
- Tune core parameters.
- Generate default selected pairs.
- Review pairs.
- Choose a different **LOW** frame.
- Choose a different **HIGH** frame.
- Add a custom pair.
- Reject pairs.
- Save reviewed pairs.

Run:

```bash
source .venv/bin/activate
uvicorn web_platform.app:app --host 0.0.0.0 --port 8000
```

Open:

```text
http://localhost:8000
```

Members on the same LAN can use the machine IP with port `8000`.

Reviewed output is saved to:

```text
web_platform/web_data/selected_dataset/low
web_platform/web_data/selected_dataset/high
web_platform/web_data/selected_dataset/metadata/reviewed_pairs.csv
```

## Web Parameters Exposed To Users

The web form exposes:

```text
frame_step
low_brightness_max
high_brightness_min
min_brightness_gap
high_context_before
high_context_after
alternatives_per_low
edge_diff_score_weight
hog_person_score_penalty
pair_low_pixel_diff_threshold
pair_low_brightness_diff_threshold
top_high_per_low
```

## Important Design Direction

Do not try to make the auto pipeline perfect.

The best direction is:

- Auto-generate ranked candidates.
- Preserve alternatives and metadata.
- Let humans review and rematch pairs in the web UI.
- Save final reviewed data for training.

