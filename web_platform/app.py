from __future__ import annotations

import json
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy import text

from .pipeline import PipelineConfig, config_from_form, load_manifest, run_pipeline
from .database import init_db, SessionLocal, Job as DBJob, PairCandidate as DBPair, ReviewedPair as DBReviewedPair
from .storage import cleanup_after_save, cloudinary_enabled, cloudinary_health, storage_prefix, upload_file

# Initialize database tables on startup
DB_INIT_ERROR: str | None = None
try:
    init_db()
except Exception as exc:
    DB_INIT_ERROR = str(exc)
    print(f"Database initialization failed: {exc}")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "web_data"
UPLOAD_DIR = DATA_DIR / "uploads"
JOB_DIR = DATA_DIR / "jobs"
DATASET_DIR = DATA_DIR / "selected_dataset"

for folder in [UPLOAD_DIR, JOB_DIR, DATASET_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Low Light Pair Builder")
executor = ThreadPoolExecutor(max_workers=1)
jobs: dict[str, dict] = {}
TEAM_OBJECTIVE_PAIRS = 500


INDEX_HTML = """<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>LLE-PairBuilder AI - Trình Tạo Cặp Ảnh Dataset</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');

    :root {
      color-scheme: dark;
      --bg-gradient: linear-gradient(135deg, #090d16 0%, #0d1424 100%);
      --panel-bg: rgba(22, 33, 54, 0.65);
      --card-bg: rgba(26, 38, 62, 0.4);
      --border-color: rgba(255, 255, 255, 0.08);
      --border-focus: rgba(6, 182, 212, 0.5);
      --input-bg: rgba(10, 15, 30, 0.7);
      --input-border: rgba(255, 255, 255, 0.1);
      --text-primary: #f8fafc;
      --text-secondary: #cbd5e1;
      --text-muted: #64748b;
      --accent-cyan: #06b6d4;
      --accent-teal: #14b8a6;
      --accent-blue: #3b82f6;
      --accent-gradient: linear-gradient(135deg, #06b6d4 0%, #3b82f6 100%);
      --success-gradient: linear-gradient(135deg, #10b981 0%, #059669 100%);
      --error-gradient: linear-gradient(135deg, #ef4444 0%, #b91c1c 100%);
      --shadow-sm: 0 2px 8px rgba(0, 0, 0, 0.2);
      --shadow-md: 0 8px 24px rgba(0, 0, 0, 0.35);
      --shadow-lg: 0 16px 40px rgba(0, 0, 0, 0.5);
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: 'Plus Jakarta Sans', ui-sans-serif, system-ui, sans-serif;
      background: var(--bg-gradient);
      color: var(--text-primary);
      min-height: 100vh;
      line-height: 1.5;
    }

    .dashboard-header {
      padding: 16px 28px;
      background: rgba(15, 23, 42, 0.6);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      border-bottom: 1px solid var(--border-color);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
      position: sticky;
      top: 0;
      z-index: 100;
      box-shadow: var(--shadow-sm);
    }

    .logo-area {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .logo-icon {
      width: 38px;
      height: 38px;
      background: var(--accent-gradient);
      border-radius: 8px;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 0 14px rgba(6, 182, 212, 0.4);
      position: relative;
    }

    .logo-icon::after {
      content: '';
      width: 14px;
      height: 14px;
      border: 2.5px solid #fff;
      border-radius: 50%;
      border-top-color: transparent;
      animation: logo-rotate 3s linear infinite;
    }

    @keyframes logo-rotate {
      to { transform: rotate(360deg); }
    }

    .logo-text h1 {
      font-size: 18px;
      font-weight: 800;
      letter-spacing: -0.5px;
      background: var(--accent-gradient);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }

    .logo-text p {
      font-size: 11px;
      color: var(--text-secondary);
      opacity: 0.8;
    }

    .header-actions {
      display: flex;
      gap: 12px;
    }

    .btn {
      border: 0;
      border-radius: 8px;
      padding: 10px 18px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
      color: #fff;
      text-transform: none;
    }

    .btn-primary {
      background: var(--accent-gradient);
      box-shadow: 0 4px 12px rgba(6, 182, 212, 0.25);
    }

    .btn-primary:hover:not(:disabled) {
      transform: translateY(-1px);
      box-shadow: 0 6px 16px rgba(6, 182, 212, 0.4);
      filter: brightness(1.1);
    }

    .btn-secondary {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(255, 255, 255, 0.08);
    }

    .btn-secondary:hover:not(:disabled) {
      background: rgba(255, 255, 255, 0.12);
      border-color: rgba(255, 255, 255, 0.18);
      transform: translateY(-1px);
    }

    .btn-submit {
      background: var(--success-gradient);
      box-shadow: 0 4px 12px rgba(16, 185, 129, 0.2);
      width: 100%;
      justify-content: center;
      padding: 12px;
    }

    .btn-submit:hover:not(:disabled) {
      transform: translateY(-1px);
      box-shadow: 0 6px 16px rgba(16, 185, 129, 0.35);
      filter: brightness(1.05);
    }

    .btn:disabled {
      background: rgba(255, 255, 255, 0.03) !important;
      border: 1px solid rgba(255, 255, 255, 0.05) !important;
      color: var(--text-muted) !important;
      box-shadow: none !important;
      cursor: not-allowed;
      transform: none !important;
    }

    .icon {
      width: 16px;
      height: 16px;
      fill: currentColor;
    }

    .dashboard-container {
      max-width: 1600px;
      margin: 0 auto;
      padding: 24px;
    }

    .dashboard-grid {
      display: grid;
      grid-template-columns: 380px 1fr;
      gap: 24px;
      align-items: start;
    }

    @media (max-width: 1200px) {
      .dashboard-grid {
        grid-template-columns: 1fr;
      }
    }

    .sidebar {
      display: flex;
      flex-direction: column;
      gap: 20px;
    }

    .card {
      background: var(--panel-bg);
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      border: 1px solid var(--border-color);
      border-radius: 12px;
      padding: 20px;
      box-shadow: var(--shadow-md);
    }

    .card-title {
      font-size: 14px;
      font-weight: 700;
      color: var(--text-primary);
      border-left: 3px solid var(--accent-cyan);
      padding-left: 10px;
      margin-bottom: 16px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    .card-header-flex {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 16px;
      border-left: 3px solid var(--accent-cyan);
      padding-left: 10px;
    }

    .card-header-flex .card-title {
      margin-bottom: 0;
      border-left: none;
      padding-left: 0;
    }

    .btn-reset {
      background: none;
      border: none;
      color: var(--text-muted);
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 4px;
      border-radius: 6px;
      transition: all 0.2s;
    }

    .btn-reset:hover {
      color: var(--text-secondary);
      background: rgba(255, 255, 255, 0.05);
    }

    .upload-zone {
      border: 2px dashed rgba(255, 255, 255, 0.12);
      border-radius: 8px;
      padding: 24px 16px;
      text-align: center;
      cursor: pointer;
      background: rgba(0, 0, 0, 0.15);
      transition: all 0.25s ease;
      position: relative;
      margin-bottom: 16px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 10px;
    }

    .upload-zone:hover {
      border-color: var(--accent-cyan);
      background: rgba(6, 182, 212, 0.02);
    }

    .upload-icon {
      width: 40px;
      height: 40px;
      fill: var(--text-muted);
      transition: fill 0.2s;
    }

    .upload-zone:hover .upload-icon {
      fill: var(--accent-cyan);
    }

    .upload-prompt {
      font-size: 13px;
      font-weight: 500;
      color: var(--text-secondary);
    }

    .upload-zone input[type="file"] {
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      opacity: 0;
      cursor: pointer;
    }

    .file-info {
      font-size: 11px;
      color: var(--text-muted);
      word-break: break-all;
      max-width: 100%;
    }

    .param-section {
      display: flex;
      flex-direction: column;
      gap: 14px;
      margin-bottom: 20px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
      padding-bottom: 20px;
    }

    .param-section:last-child {
      border-bottom: none;
      padding-bottom: 0;
      margin-bottom: 0;
    }

    .param-section-title {
      font-size: 12px;
      font-weight: 700;
      color: var(--accent-teal);
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    .param-grid {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .param-item {
      background: rgba(255, 255, 255, 0.015);
      border: 1px solid rgba(255, 255, 255, 0.04);
      border-radius: 8px;
      padding: 12px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }

    .param-label-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
    }

    .param-name {
      font-size: 12.5px;
      font-weight: 600;
      color: var(--text-primary);
    }

    .param-item input {
      width: 76px;
      background: var(--input-bg);
      border: 1px solid var(--input-border);
      color: #fff;
      border-radius: 6px;
      padding: 6px 10px;
      font-size: 12.5px;
      font-family: inherit;
      font-weight: 600;
      outline: none;
      transition: all 0.2s;
      text-align: right;
    }

    .param-item input:focus {
      border-color: var(--accent-cyan);
      box-shadow: 0 0 0 2px rgba(6, 182, 212, 0.15);
    }

    .param-desc {
      font-size: 11px;
      color: var(--text-muted);
      line-height: 1.4;
    }

    .workspace {
      display: flex;
      flex-direction: column;
      gap: 20px;
    }

    .status-card {
      display: flex;
      flex-direction: column;
      gap: 12px;
      border-left: 4px solid var(--accent-blue);
    }

    .status-indicator {
      font-size: 13.5px;
      font-weight: 500;
      color: var(--text-secondary);
    }

    .progress-container {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .spinner {
      width: 18px;
      height: 18px;
      border: 2px solid rgba(255, 255, 255, 0.1);
      border-top-color: var(--accent-cyan);
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }

    @keyframes spin {
      to { transform: rotate(360deg); }
    }

    .success-message {
      color: #10b981;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .error-message {
      color: #ef4444;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .summary-grid {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .summary-grid span {
      font-size: 11.5px;
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 6px;
      padding: 5px 9px;
      color: var(--text-secondary);
      font-family: monospace;
    }

    .job-fields {
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
      margin: 14px 0;
    }

    .job-field {
      display: grid;
      gap: 6px;
    }

    .job-field label {
      font-size: 12px;
      font-weight: 700;
      color: var(--text-secondary);
    }

    .job-field input {
      width: 100%;
      background: var(--input-bg);
      border: 1px solid var(--input-border);
      color: var(--text-primary);
      border-radius: 8px;
      padding: 10px 12px;
      font-size: 13px;
      outline: none;
    }

    .job-field input:focus {
      border-color: var(--border-focus);
      box-shadow: 0 0 0 3px rgba(6, 182, 212, 0.12);
    }

    .objective-progress {
      margin-top: 12px;
      display: grid;
      grid-template-columns: minmax(220px, 1.35fr) repeat(3, minmax(120px, 1fr));
      gap: 10px;
    }

    .objective-chart-card {
      border-radius: 10px;
      padding: 12px;
      border: 1px solid rgba(6, 182, 212, 0.25);
      background: rgba(6, 182, 212, 0.08);
      display: grid;
      grid-template-columns: 92px 1fr;
      gap: 12px;
      align-items: center;
      min-height: 112px;
    }

    .pie-chart {
      width: 86px;
      height: 86px;
      border-radius: 50%;
      background: conic-gradient(var(--accent-cyan) var(--pie-deg, 0deg), rgba(255, 255, 255, 0.08) 0deg);
      display: grid;
      place-items: center;
      box-shadow: inset 0 0 0 1px rgba(255,255,255,0.08), 0 0 20px rgba(6, 182, 212, 0.12);
    }

    .pie-chart::after {
      content: attr(data-percent);
      width: 58px;
      height: 58px;
      border-radius: 50%;
      background: rgba(10, 15, 30, 0.92);
      display: grid;
      place-items: center;
      color: var(--text-primary);
      font-size: 15px;
      font-weight: 800;
    }

    .objective-chart-copy {
      display: grid;
      gap: 5px;
      min-width: 0;
    }

    .objective-pill {
      border-radius: 10px;
      padding: 10px 12px;
      border: 1px solid rgba(6, 182, 212, 0.2);
      color: var(--text-primary);
      background: rgba(6, 182, 212, 0.08);
      display: grid;
      gap: 4px;
      min-height: 64px;
    }

    .objective-label {
      font-size: 10.5px;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.04em;
      font-weight: 800;
    }

    .objective-value {
      color: var(--accent-cyan);
      font-size: 16px;
      font-weight: 800;
      line-height: 1.2;
      overflow-wrap: anywhere;
    }

    .objective-note {
      font-size: 11px;
      color: var(--text-secondary);
    }

    .objective-pill.done {
      border-color: rgba(16, 185, 129, 0.35);
      background: rgba(16, 185, 129, 0.1);
    }

    .objective-pill.done .objective-value {
      color: #10b981;
    }

    @media (max-width: 900px) {
      .objective-progress {
        grid-template-columns: repeat(2, 1fr);
      }

      .objective-chart-card {
        grid-column: 1 / -1;
      }
    }

    @media (max-width: 560px) {
      .objective-progress {
        grid-template-columns: 1fr;
      }

      .objective-chart-card {
        grid-template-columns: 1fr;
        justify-items: center;
        text-align: center;
      }
    }

    .section-title-flex {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 16px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.08);
      padding-bottom: 10px;
    }

    .section-title-flex h2 {
      font-size: 16px;
      font-weight: 700;
    }

    .pairs-count {
      font-size: 12px;
      background: rgba(6, 182, 212, 0.15);
      color: var(--accent-cyan);
      border: 1px solid rgba(6, 182, 212, 0.25);
      border-radius: 20px;
      padding: 3px 10px;
      font-weight: 600;
    }

    .pairs {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .pair {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: 12px;
      overflow: hidden;
      display: grid;
      grid-template-columns: 52px 1fr;
      transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
      box-shadow: var(--shadow-sm);
    }

    .pair:hover {
      border-color: rgba(6, 182, 212, 0.2);
      box-shadow: var(--shadow-md);
      transform: translateY(-2px);
    }

    .pair:has(.pick:not(:checked)) {
      opacity: 0.45;
      filter: grayscale(0.5);
      border-color: rgba(239, 68, 68, 0.15);
    }

    .pickbox {
      background: rgba(255, 255, 255, 0.015);
      border-right: 1px solid var(--border-color);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 16px 0;
      transition: background 0.3s;
    }

    .pickbox input[type="checkbox"] {
      appearance: none;
      -webkit-appearance: none;
      width: 22px;
      height: 22px;
      border: 2px solid rgba(255, 255, 255, 0.2);
      border-radius: 6px;
      outline: none;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: all 0.2s;
      background: rgba(0, 0, 0, 0.2);
    }

    .pickbox input[type="checkbox"]:checked {
      background: var(--accent-teal);
      border-color: var(--accent-teal);
    }

    .pickbox input[type="checkbox"]:checked::after {
      content: "✓";
      color: #fff;
      font-size: 13px;
      font-weight: bold;
    }

    .pair-body {
      padding: 18px;
      display: flex;
      flex-direction: column;
      gap: 14px;
    }

    .images {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
    }

    @media (max-width: 768px) {
      .images {
        grid-template-columns: 1fr;
      }
    }

    figure {
      margin: 0;
      border: 1px solid var(--border-color);
      border-radius: 8px;
      overflow: hidden;
      background: #090d16;
      position: relative;
    }

    figure img {
      display: block;
      width: 100%;
      height: 250px;
      object-fit: contain;
      transition: transform 0.4s ease;
    }

    figure:hover img {
      transform: scale(1.03);
    }

    figcaption {
      background: rgba(13, 22, 38, 0.95);
      border-top: 1px solid var(--border-color);
      color: var(--text-secondary);
      font-size: 11.5px;
      padding: 8px 12px;
      font-family: monospace;
    }

    .img-tag {
      position: absolute;
      top: 10px;
      left: 10px;
      background: rgba(0, 0, 0, 0.7);
      backdrop-filter: blur(4px);
      padding: 4px 8px;
      border-radius: 4px;
      font-size: 9.5px;
      font-weight: 700;
      letter-spacing: 0.5px;
      z-index: 2;
      border: 1px solid rgba(255, 255, 255, 0.15);
    }

    .img-tag.low {
      color: #fca5a5;
      border-color: rgba(239, 68, 68, 0.3);
    }

    .img-tag.high {
      color: #86efac;
      border-color: rgba(16, 185, 129, 0.3);
    }

    .controls {
      display: grid;
      grid-template-columns: 1fr 1fr auto;
      gap: 12px;
      align-items: end;
    }

    @media (max-width: 900px) {
      .controls {
        grid-template-columns: 1fr;
      }
    }

    .controls label {
      display: flex;
      flex-direction: column;
      gap: 6px;
      font-size: 10.5px;
      font-weight: 600;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    .controls select {
      background: var(--input-bg);
      border: 1px solid var(--input-border);
      color: #fff;
      border-radius: 8px;
      padding: 10px 12px;
      font-size: 13px;
      outline: none;
      font-family: inherit;
      transition: all 0.2s;
      cursor: pointer;
    }

    .controls select:focus {
      border-color: var(--accent-cyan);
    }

    .controls select option {
      background: #0e1626;
      color: #fff;
    }

    .direct-note {
      min-height: 40px;
      display: flex;
      align-items: center;
      grid-column: span 2;
      color: var(--text-secondary);
      font-size: 12.5px;
      line-height: 1.4;
      background: rgba(6, 182, 212, 0.06);
      border: 1px solid rgba(6, 182, 212, 0.16);
      border-radius: 8px;
      padding: 10px 12px;
    }

    @media (max-width: 900px) {
      .direct-note {
        grid-column: auto;
      }
    }

    .reject-btn {
      background: rgba(239, 68, 68, 0.08);
      border: 1px solid rgba(239, 68, 68, 0.25);
      color: #fca5a5;
      border-radius: 8px;
      padding: 10px 16px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s;
      height: 40px;
    }

    .reject-btn:hover {
      background: #ef4444;
      border-color: #ef4444;
      color: #fff;
    }

    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      margin-top: 4px;
    }

    .meta span {
      font-size: 11px;
      background: rgba(255, 255, 255, 0.03);
      border: 1px solid rgba(255, 255, 255, 0.06);
      border-radius: 6px;
      padding: 4px 8px;
      color: var(--text-secondary);
    }

    .meta span.score-value {
      color: var(--accent-cyan);
      border-color: rgba(6, 182, 212, 0.2);
      background: rgba(6, 182, 212, 0.04);
    }

    .meta span.gap-value {
      color: var(--accent-teal);
      border-color: rgba(20, 184, 166, 0.2);
      background: rgba(20, 184, 166, 0.04);
    }

    .meta .warn.hog-value {
      color: #f59e0b;
      border-color: rgba(245, 158, 11, 0.3);
      background: rgba(245, 158, 11, 0.06);
      animation: pulse-warn 2s infinite;
    }

    @keyframes pulse-warn {
      0% { box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.2); }
      70% { box-shadow: 0 0 0 6px rgba(245, 158, 11, 0); }
      100% { box-shadow: 0 0 0 0 rgba(245, 158, 11, 0); }
    }

    .empty-state {
      text-align: center;
      padding: 60px 20px;
      border: 2px dashed rgba(255, 255, 255, 0.08);
      border-radius: 12px;
      color: var(--text-muted);
      background: rgba(0, 0, 0, 0.15);
    }

    .empty-state svg {
      width: 44px;
      height: 44px;
      fill: rgba(255, 255, 255, 0.15);
      margin-bottom: 14px;
    }

    .empty-state p {
      color: var(--text-secondary);
      font-size: 14px;
      font-weight: 600;
      margin-bottom: 6px;
    }

    .empty-state span {
      font-size: 12px;
      max-width: 420px;
      display: inline-block;
      line-height: 1.45;
    }

    details.guide {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: 10px;
      overflow: hidden;
      margin-bottom: 24px;
    }

    details.guide summary {
      padding: 12px 16px;
      font-weight: 600;
      color: var(--accent-cyan);
      background: rgba(255, 255, 255, 0.02);
      cursor: pointer;
      outline: none;
      user-select: none;
    }

    .guide-body {
      border-top: 1px solid var(--border-color);
      padding: 16px;
      display: grid;
      gap: 16px;
    }

    .usage-flow {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 10px;
    }

    @media (max-width: 600px) {
      .usage-flow {
        grid-template-columns: 1fr;
      }
    }

    .usage-flow span {
      background: rgba(20, 184, 166, 0.08);
      border: 1px solid rgba(20, 184, 166, 0.2);
      color: var(--accent-teal);
      border-radius: 8px;
      padding: 8px 12px;
      font-size: 12px;
      font-weight: 600;
      text-align: center;
    }

    .guide-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
    }

    @media (max-width: 900px) {
      .guide-grid {
        grid-template-columns: 1fr;
      }
    }

    .guide-card {
      background: rgba(0, 0, 0, 0.15);
      border: 1px solid rgba(255, 255, 255, 0.04);
      border-left: 3px solid var(--accent-cyan);
      border-radius: 8px;
      padding: 12px;
      font-size: 12px;
      color: var(--text-secondary);
      line-height: 1.45;
      display: flex;
      flex-direction: column;
      gap: 4px;
    }

    .guide-card strong {
      color: var(--text-primary);
      font-size: 12.5px;
    }

    .guide-card:nth-child(1) { border-left-color: var(--accent-cyan); }
    .guide-card:nth-child(2) { border-left-color: var(--accent-blue); }
    .guide-card:nth-child(3) { border-left-color: #8b5cf6; }
    .guide-card:nth-child(4) { border-left-color: #f59e0b; }
    .guide-card:nth-child(5) { border-left-color: #06b6d4; }
    .guide-card:nth-child(6) { border-left-color: #10b981; }
  </style>
</head>
<body>
  <header class="dashboard-header">
    <div class="logo-area">
      <div class="logo-icon"></div>
      <div class="logo-text">
        <h1>LLE-PairBuilder AI</h1>
        <p>Trình Tạo Dataset Phục Hồi Ảnh Thiếu Sáng</p>
      </div>
    </div>
    <div class="header-actions">
      <button id="addPairBtn" class="btn btn-secondary" disabled>
        <svg class="icon" viewBox="0 0 24 24"><path d="M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z"/></svg>
        Thêm cặp thủ công
      </button>
      <button id="saveBtn" class="btn btn-primary" disabled>
        <svg class="icon" viewBox="0 0 24 24"><path d="M17 3H5c-1.11 0-2 .9-2 2v14c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V7l-4-4zm-5 16c-1.66 0-3-1.34-3-3s1.34-3 3-3 3 1.34 3 3-1.34 3-3 3zm3-10H5V5h10v4z"/></svg>
        Lưu cặp đã duyệt
      </button>
    </div>
  </header>
  <main class="dashboard-container">
    <details class="guide" open>
      <summary>Hướng Dẫn & Mẹo Chỉnh</summary>
      <div class="guide-body">
        <div class="usage-flow">
          <span>1. Tải video lên</span>
          <span>2. Chạy mặc định trước</span>
          <span>3. Review, chỉnh thủ công</span>
          <span>4. Lưu cặp đã duyệt</span>
        </div>
        <div class="guide-grid">
          <div class="guide-card">
            <strong>Bỏ sót khoảnh khắc tốt</strong>
            Giảm Frame step, tăng High before/after, giảm High min hoặc giảm Min gap.
          </div>
          <div class="guide-card">
            <strong>Ảnh trùng lặp nhiều</strong>
            Tăng Low dedup diff/bright và giữ Top per low bằng 1.
          </div>
          <div class="guide-card">
            <strong>Sai vật thể hoặc dính người</strong>
            Tăng Edge diff weight hoặc HOG penalty, sau đó đổi HIGH thủ công nếu cần.
          </div>
          <div class="guide-card">
            <strong>Khung LOW chưa tối ưu</strong>
            Dùng dropdown thay đổi Frame LOW hoặc click Thêm cặp thủ công rồi chọn HIGH tương ứng.
          </div>
          <div class="guide-card">
            <strong>Ảnh HIGH chưa khớp hẳn</strong>
            Dùng dropdown thay đổi Frame HIGH. Danh sách gợi ý hiển thị kèm Điểm số, Độ sáng và Cảnh báo người.
          </div>
          <div class="guide-card">
            <strong>Lưu trữ Dataset</strong>
            Các cặp được chọn sẽ copy vào web_platform/web_data/selected_dataset kèm metadata và nhật ký chỉnh sửa.
          </div>
        </div>
      </div>
    </details>

    <div class="dashboard-grid">
      <!-- SIDEBAR QUẢN LÝ -->
      <aside class="sidebar">
        <form id="uploadForm">
          <div class="card" style="margin-bottom: 20px;">
            <h2 class="card-title">1. Tải Video Đầu Vào</h2>
            <div class="upload-zone">
              <svg class="upload-icon" viewBox="0 0 24 24"><path d="M19.35 10.04C18.67 6.59 15.64 4 12 4 9.11 4 6.6 5.64 5.35 8.04 2.34 8.36 0 10.91 0 14c0 3.31 2.69 6 6 6h13c2.76 0 5-2.24 5-5 0-2.64-2.05-4.78-4.65-4.96zM14 13v4h-4v-4H7l5-5 5 5h-3z"/></svg>
              <p class="upload-prompt">Kéo thả hoặc Click chọn Video</p>
              <input id="videoInput" name="video" type="file" accept="video/*" required>
              <div class="file-info" id="fileInfo">Chưa chọn video nào</div>
            </div>
            <div class="job-fields">
              <div class="job-field">
                <label for="submittedByInput">Người nộp</label>
                <input id="submittedByInput" name="submitted_by" type="text" maxlength="120" placeholder="VD: Nguyễn Văn A" required>
              </div>
              <div class="job-field">
                <label for="objectivePairsInput">Mục tiêu team</label>
                <input id="objectivePairsInput" name="objective_pairs" type="number" min="1" step="1" value="500" readonly>
              </div>
            </div>
            <button id="uploadBtn" type="submit" class="btn btn-submit">Xử lý video</button>
          </div>
        </form>

        <form id="imagePairForm">
          <div class="card" style="margin-bottom: 20px;">
            <h2 class="card-title">Upload nhanh nhiều ảnh</h2>
            <div class="job-fields">
              <div class="job-field">
                <label for="directSubmittedByInput">Người nộp</label>
                <input id="directSubmittedByInput" name="submitted_by" type="text" maxlength="120" placeholder="VD: Nguyễn Văn A" required>
              </div>
              <div class="job-field">
                <label for="directLowInput">Ảnh LOW</label>
                <input id="directLowInput" name="low_images" type="file" accept="image/*" multiple required>
              </div>
              <div class="job-field">
                <label for="directHighInput">Ảnh HIGH</label>
                <input id="directHighInput" name="high_images" type="file" accept="image/*" multiple required>
              </div>
            </div>
            <button id="imagePairBtn" type="submit" class="btn btn-submit">Xem trước batch ảnh</button>
          </div>
        </form>

          <div class="card">
            <div class="card-header-flex">
              <h2 class="card-title">2. Cấu Hình Tham Số</h2>
              <button type="button" class="btn-reset" id="resetParamsBtn" title="Khôi phục mặc định">
                <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><path d="M17.65 6.35C16.2 4.9 14.21 4 12 4c-4.42 0-7.99 3.58-7.99 8s3.57 8 7.99 8c3.73 0 6.84-2.55 7.73-6h-2.08c-.82 2.33-3.04 4-5.65 4-3.31 0-6-2.69-6-6s2.69-6 6-6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z"/></svg>
              </button>
            </div>

            <!-- Param Group 1 -->
            <div class="param-section">
              <h3 class="param-section-title">Quét Video & Dự Phòng</h3>
              <div class="param-grid">
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Tần suất trích xuất (Frame step)</span>
                    <input name="frame_step" type="number" min="1" value="5">
                  </div>
                  <span class="param-desc">Số frame bỏ qua khi quét video. Số nhỏ (3-5) quét chi tiết hơn nhưng chậm; số lớn (8-15) chạy nhanh hơn nhưng dễ sót.</span>
                </div>
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Ảnh sáng dự phòng (Alternatives)</span>
                    <input name="alternatives_per_low" type="number" min="1" value="8">
                  </div>
                  <span class="param-desc">Số lượng ảnh sáng thay thế tối đa hiển thị trong dropdown lựa chọn thủ công.</span>
                </div>
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Số cặp tự động (Top per low)</span>
                    <input name="top_high_per_low" type="number" min="1" value="1">
                  </div>
                  <span class="param-desc">Số cặp ghép tự động cho mỗi ảnh tối. Để 1 khi tạo dataset; chỉ tăng khi muốn debug/khảo sát.</span>
                </div>
              </div>
            </div>

            <!-- Param Group 2 -->
            <div class="param-section">
              <h3 class="param-section-title">Độ Sáng & Chênh Lệch</h3>
              <div class="param-grid">
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Độ sáng tối đa LOW (Low max)</span>
                    <input name="low_brightness_max" type="number" step="1" value="60">
                  </div>
                  <span class="param-desc">Độ sáng tối đa (0-255) để một ảnh được coi là tối. Tăng lên nếu bị trích thiếu ảnh tối cần lấy.</span>
                </div>
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Độ sáng tối thiểu HIGH (High min)</span>
                    <input name="high_brightness_min" type="number" step="1" value="100">
                  </div>
                  <span class="param-desc">Độ sáng tối thiểu (0-255) để một ảnh được coi là sáng. Giảm xuống nếu video không có cảnh cực sáng.</span>
                </div>
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Chênh lệch sáng tối (Min gap)</span>
                    <input name="min_brightness_gap" type="number" step="1" value="40">
                  </div>
                  <span class="param-desc">Chênh lệch độ sáng tối thiểu bắt buộc giữa HIGH và LOW để đảm bảo độ tương phản cao cho dataset.</span>
                </div>
              </div>
            </div>

            <!-- Param Group 3 -->
            <div class="param-section">
              <h3 class="param-section-title">Phạm Vi & Thuật Toán</h3>
              <div class="param-grid">
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Quét trước tối (High before)</span>
                    <input name="high_context_before" type="number" min="1" value="8">
                  </div>
                  <span class="param-desc">Phạm vi tìm kiếm ảnh sáng trước lúc tắt đèn. Tăng nếu cảnh sáng rõ nằm xa về phía trước.</span>
                </div>
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Quét sau tối (High after)</span>
                    <input name="high_context_after" type="number" min="1" value="8">
                  </div>
                  <span class="param-desc">Phạm vi tìm kiếm ảnh sáng sau lúc bật đèn. Tăng nếu cảnh sáng rõ nằm xa về phía sau.</span>
                </div>
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Trọng số lệch vật thể (Edge diff)</span>
                    <input name="edge_diff_score_weight" type="number" step="0.5" value="8">
                  </div>
                  <span class="param-desc">Tăng (8-12) giúp tránh ghép nhầm cảnh khác nhau. Giảm (4-6) nếu ảnh tối quá mờ không nhận rõ cấu trúc cạnh.</span>
                </div>
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Phạt dính người (HOG penalty)</span>
                    <input name="hog_person_score_penalty" type="number" step="1" value="25">
                  </div>
                  <span class="param-desc">Điểm trừ khi phát hiện có người xuất hiện trong ảnh sáng (tránh vật thể di động che cảnh).</span>
                </div>
              </div>
            </div>

            <!-- Param Group 4 -->
            <div class="param-section">
              <h3 class="param-section-title">Lọc Trùng Lặp (Deduplication)</h3>
              <div class="param-grid">
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Lọc chi tiết trùng (Low dedup diff)</span>
                    <input name="pair_low_pixel_diff_threshold" type="number" step="1" value="8">
                  </div>
                  <span class="param-desc">Ngưỡng lọc ảnh tối trùng lặp (theo chi tiết). Tăng để loại các ảnh tối giống hệt nhau, đa dạng hoá dataset.</span>
                </div>
                <div class="param-item">
                  <div class="param-label-row">
                    <span class="param-name">Lọc độ sáng trùng (Low dedup bright)</span>
                    <input name="pair_low_brightness_diff_threshold" type="number" step="1" value="8">
                  </div>
                  <span class="param-desc">Ngưỡng lọc ảnh tối trùng lặp (theo độ sáng). Tránh lưu quá nhiều ảnh tối có cùng độ sáng liên tiếp.</span>
                </div>
              </div>
            </div>
          </div>
      </aside>

      <!-- KHÔNG GIAN LÀM VIỆC CHÍNH -->
      <section class="workspace">
        <div class="card status-card">
          <div class="status-indicator" id="status">Vui lòng tải video lên để bắt đầu tạo cặp ảnh.</div>
          <div class="objective-progress" id="objectiveProgress">
            <div class="objective-chart-card">
              <div class="pie-chart" style="--pie-deg: 0deg;" data-percent="0%"></div>
              <div class="objective-chart-copy">
                <span class="objective-label">Mục tiêu team</span>
                <span class="objective-value">0/500 cặp</span>
                <span class="objective-note">Còn thiếu 500 cặp ảnh</span>
              </div>
            </div>
            <div class="objective-pill">
              <span class="objective-label">Người nộp</span>
              <span class="objective-value">Chưa nhập</span>
              <span class="objective-note">Nhập ở form upload</span>
            </div>
            <div class="objective-pill">
              <span class="objective-label">Auto detect</span>
              <span class="objective-value">0 cặp</span>
              <span class="objective-note">Sau xử lý sẽ cập nhật</span>
            </div>
            <div class="objective-pill">
              <span class="objective-label">Đã lưu</span>
              <span class="objective-value">0 cặp</span>
              <span class="objective-note">Sau khi bấm lưu</span>
            </div>
          </div>
          <div class="summary-grid" id="summary"></div>
        </div>

        <div class="pairs-wrapper">
          <div class="section-title-flex">
            <h2>Danh Sách Cặp Ảnh LOW - HIGH Đã Chọn</h2>
            <span class="pairs-count" id="pairsCount">0 cặp</span>
          </div>
          <section class="pairs" id="pairs">
            <div class="empty-state">
              <svg viewBox="0 0 24 24"><path d="M19 5v14H5V5h14m0-2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-4.86 8.86l-3 3.87L9 13.14 6 17h12l-3.86-5.14z"/></svg>
              <p>Chưa có dữ liệu cặp ảnh được tạo</p>
              <span>Vui lòng tải lên video và bấm "Xử lý video". Hệ thống sẽ tự động phân tích và chọn ra các cặp ảnh tối ưu nhất theo cấu hình của bạn.</span>
            </div>
          </section>
        </div>
      </section>
    </div>
  </main>

  <script>
    let currentJobId = null;
    let currentPairs = [];
    let lowOptions = [];
    let highOptions = [];
    let currentJobMeta = { submitted_by: "", objective_pairs: null, saved_count: 0 };
    let teamStats = { saved_count: 0, objective_pairs: 500, remaining_pairs: 500 };
    let pendingDirectPair = null;
    let directLowFiles = [];
    let directHighFiles = [];

    const uploadForm = document.getElementById("uploadForm");
    const imagePairForm = document.getElementById("imagePairForm");
    const uploadBtn = document.getElementById("uploadBtn");
    const imagePairBtn = document.getElementById("imagePairBtn");
    const addPairBtn = document.getElementById("addPairBtn");
    const saveBtn = document.getElementById("saveBtn");
    const statusEl = document.getElementById("status");
    const pairsEl = document.getElementById("pairs");
    const summaryEl = document.getElementById("summary");
    const pairsCountEl = document.getElementById("pairsCount");
    const objectiveProgressEl = document.getElementById("objectiveProgress");

    // File input change event for better UX
    const videoInput = document.getElementById("videoInput");
    const fileInfo = document.getElementById("fileInfo");
    const submittedByInput = document.getElementById("submittedByInput");
    const directSubmittedByInput = document.getElementById("directSubmittedByInput");
    const objectivePairsInput = document.getElementById("objectivePairsInput");
    if (videoInput && fileInfo) {
      videoInput.addEventListener("change", (e) => {
        const file = e.target.files[0];
        if (file) {
          fileInfo.innerHTML = `<strong>Đã chọn:</strong> ${file.name} (${(file.size / (1024 * 1024)).toFixed(2)} MB)`;
          fileInfo.style.color = "var(--accent-cyan)";
        } else {
          fileInfo.textContent = "Chưa chọn video nào";
          fileInfo.style.color = "";
        }
      });
    }

    function syncJobMetaFromInputs() {
      currentJobMeta.submitted_by = submittedByInput ? submittedByInput.value.trim() : "";
      currentJobMeta.objective_pairs = objectivePairsInput && objectivePairsInput.value ? Number(objectivePairsInput.value) : null;
      renderObjectiveProgress();
    }

    function syncDirectJobMetaFromInputs() {
      currentJobMeta.submitted_by = directSubmittedByInput ? directSubmittedByInput.value.trim() : "";
      currentJobMeta.objective_pairs = objectivePairsInput && objectivePairsInput.value ? Number(objectivePairsInput.value) : null;
      renderObjectiveProgress();
    }

    function getVideoSubmitter() {
      return submittedByInput ? submittedByInput.value.trim() : "";
    }

    function getDirectSubmitter() {
      return directSubmittedByInput ? directSubmittedByInput.value.trim() : "";
    }

    if (submittedByInput) {
      submittedByInput.addEventListener("input", syncJobMetaFromInputs);
    }
    if (directSubmittedByInput) {
      directSubmittedByInput.addEventListener("input", syncDirectJobMetaFromInputs);
    }
    if (objectivePairsInput) {
      objectivePairsInput.addEventListener("input", syncJobMetaFromInputs);
    }

    function clearPendingDirectBatch() {
      if (pendingDirectPair) {
        [...(pendingDirectPair.lowOptions || []), ...(pendingDirectPair.highOptions || [])].forEach(item => {
          if (item.previewUrl) URL.revokeObjectURL(item.previewUrl);
        });
      }
      pendingDirectPair = null;
      directLowFiles = [];
      directHighFiles = [];
    }

    // Default params reset handler
    const defaultParams = {
      frame_step: 5,
      low_brightness_max: 60,
      high_brightness_min: 100,
      min_brightness_gap: 40,
      high_context_before: 8,
      high_context_after: 8,
      alternatives_per_low: 8,
      edge_diff_score_weight: 8,
      hog_person_score_penalty: 25,
      pair_low_pixel_diff_threshold: 8,
      pair_low_brightness_diff_threshold: 8,
      top_high_per_low: 1
    };
    document.getElementById("resetParamsBtn").addEventListener("click", () => {
      for (const [key, val] of Object.entries(defaultParams)) {
        const input = document.querySelector(`[name="${key}"]`);
        if (input) input.value = val;
      }
    });

    uploadForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const file = videoInput.files[0];
      if (!file) return;

      clearPendingDirectBatch();

      currentJobId = null;
      currentPairs = [];
      lowOptions = [];
      highOptions = [];
      currentJobMeta = {
        submitted_by: getVideoSubmitter(),
        objective_pairs: objectivePairsInput && objectivePairsInput.value ? Number(objectivePairsInput.value) : null,
        saved_count: 0
      };
      pairsEl.innerHTML = `
        <div class="empty-state">
          <div class="spinner" style="width: 32px; height: 32px; margin: 0 auto 16px auto;"></div>
          <p>Đang xử lý dữ liệu video...</p>
          <span>Quá trình trích xuất khung hình và tính toán độ chênh lệch ánh sáng có thể mất một vài phút tùy thuộc vào độ dài video.</span>
        </div>
      `;
      summaryEl.innerHTML = "";
      renderObjectiveProgress();
      saveBtn.disabled = true;
      addPairBtn.disabled = true;
      uploadBtn.disabled = true;
      statusEl.innerHTML = `
        <div class="progress-container">
          <div class="spinner"></div>
          <span class="progress-text">Đang tải video lên server...</span>
        </div>
      `;

      const body = new FormData(uploadForm);
      body.set("video", file);
      for (const [key] of Object.entries(defaultParams)) {
        const input = document.querySelector(`[name="${key}"]`);
        if (input) body.set(key, input.value);
      }

      try {
        const response = await fetch("/api/jobs", { method: "POST", body });
        const payload = await response.json();
        currentJobId = payload.job_id;
        statusEl.innerHTML = `
          <div class="progress-container">
            <div class="spinner"></div>
            <span class="progress-text">Video tải lên thành công. Bắt đầu phân tích...</span>
          </div>
        `;
        pollJob();
      } catch (err) {
        uploadBtn.disabled = false;
        statusEl.innerHTML = `<span class="error-message"><svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg> Gửi yêu cầu thất bại: ${err.message}</span>`;
      }
    });

    imagePairForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      directLowFiles = Array.from(document.getElementById("directLowInput").files || []);
      directHighFiles = Array.from(document.getElementById("directHighInput").files || []);
      if (!directLowFiles.length || !directHighFiles.length) return;

      clearPendingDirectBatch();
      directLowFiles = Array.from(document.getElementById("directLowInput").files || []);
      directHighFiles = Array.from(document.getElementById("directHighInput").files || []);
      const directLowOptions = directLowFiles.map((file, index) => ({
        idx: index,
        file_index: index,
        name: file.name,
        path: URL.createObjectURL(file),
        previewUrl: null,
        brightness: null,
        direct_upload: true
      }));
      const directHighOptions = directHighFiles.map((file, index) => ({
        idx: index,
        file_index: index,
        name: file.name,
        path: URL.createObjectURL(file),
        previewUrl: null,
        brightness: null,
        direct_upload: true
      }));
      directLowOptions.forEach(item => item.previewUrl = item.path);
      directHighOptions.forEach(item => item.previewUrl = item.path);

      pendingDirectPair = {
        lowOptions: directLowOptions,
        highOptions: directHighOptions
      };
      currentJobId = null;
      lowOptions = directLowOptions;
      highOptions = directHighOptions;
      currentJobMeta = {
        submitted_by: getDirectSubmitter(),
        objective_pairs: objectivePairsInput && objectivePairsInput.value ? Number(objectivePairsInput.value) : null,
        saved_count: 0
      };
      const initialPairCount = Math.min(directLowOptions.length, directHighOptions.length);
      currentPairs = Array.from({ length: initialPairCount }, (_, index) => {
        const low = directLowOptions[index];
        const high = directHighOptions[index];
        return {
          pair_id: index,
          segment_id: "direct",
          mode: "direct_upload",
          low_idx: low.idx,
          high_idx: high.idx,
          selected_high_idx: high.idx,
          low_file_index: low.file_index,
          high_file_index: high.file_index,
          low_name: low.name,
          high_name: high.name,
          low_path: low.path,
          high_path: high.path,
          low_brightness: null,
          high_brightness: null,
          brightness_gap: null,
          score: null,
          good_matches: null,
          inlier_ratio: null,
          hog_hits: null,
          accepted: true,
          alternatives: []
        };
      });

      summaryEl.innerHTML = "";
      renderPairs(currentPairs);
      renderObjectiveProgress();
      saveBtn.disabled = false;
      addPairBtn.disabled = lowOptions.length === 0 || highOptions.length === 0;
      statusEl.innerHTML = `
        <span class="success-message">
          <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>
          Đã tạo preview ${currentPairs.length} cặp từ ${directLowFiles.length} ảnh LOW và ${directHighFiles.length} ảnh HIGH. Các ảnh này sẽ được lưu chung một batch/video group khi bấm "Lưu cặp đã duyệt".
        </span>
      `;
    });

    async function pollJob() {
      if (!currentJobId) return;
      try {
        const response = await fetch(`/api/jobs/${currentJobId}`);
        const job = await response.json();

        if (job.status === "done") {
          uploadBtn.disabled = false;
          currentPairs = job.pairs || [];
          lowOptions = job.low_options || [];
          highOptions = job.high_options || [];
          currentJobMeta = {
            submitted_by: job.submitted_by || currentJobMeta.submitted_by || "",
            objective_pairs: job.objective_pairs || currentJobMeta.objective_pairs || null,
            saved_count: job.saved_count || 0
          };
          renderSummary(job.summary || {});
          renderObjectiveProgress();
          statusEl.innerHTML = `
            <span class="success-message">
              <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/></svg>
              Hoàn thành! Đã tự động phát hiện ${currentPairs.length} cặp ảnh LOW-HIGH tối ưu${currentJobMeta.submitted_by ? ` cho ${currentJobMeta.submitted_by}` : ""}.
            </span>
          `;
          if (pairsCountEl) pairsCountEl.textContent = `${currentPairs.length} cặp`;
          renderPairs(currentPairs);
          saveBtn.disabled = currentPairs.length === 0;
          addPairBtn.disabled = lowOptions.length === 0 || highOptions.length === 0;
          return;
        }

        if (job.status === "failed") {
          uploadBtn.disabled = false;
          statusEl.innerHTML = `
            <span class="error-message">
              <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg>
              Lỗi xử lý: ${job.error}
            </span>
          `;
          return;
        }

        statusEl.innerHTML = `
          <div class="progress-container">
            <div class="spinner"></div>
            <span class="progress-text">${job.message || "Đang xử lý video..."}</span>
          </div>
        `;
        setTimeout(pollJob, 1500);
      } catch (err) {
        setTimeout(pollJob, 2000);
      }
    }

    function renderSummary(summary) {
      summaryEl.innerHTML = Object.entries(summary).map(([key, value]) => `<span>${key}: ${value}</span>`).join("");
    }

    function renderObjectiveProgress(savedOverride = null) {
      if (!objectiveProgressEl) return;
      const submitter = currentJobMeta.submitted_by || "Chưa nhập";
      const teamTarget = Number(teamStats.objective_pairs || 500);
      const teamSaved = Number(teamStats.saved_count || 0);
      const teamRemaining = Math.max(teamTarget - teamSaved, 0);
      const percent = teamTarget > 0 ? Math.min(Math.round((teamSaved / teamTarget) * 100), 100) : 0;
      const pieDeg = Math.min((teamSaved / Math.max(teamTarget, 1)) * 360, 360).toFixed(1);
      const saved = savedOverride === null ? Number(currentJobMeta.saved_count || 0) : Number(savedOverride || 0);
      const detected = pendingDirectPair ? 0 : (currentPairs.length || 0);

      const doneClass = teamTarget > 0 && teamSaved >= teamTarget ? "done" : "";
      objectiveProgressEl.innerHTML = `
        <div class="objective-chart-card ${doneClass}">
          <div class="pie-chart" style="--pie-deg: ${pieDeg}deg;" data-percent="${percent}%"></div>
          <div class="objective-chart-copy">
            <span class="objective-label">Mục tiêu team</span>
            <span class="objective-value">${teamSaved}/${teamTarget} cặp</span>
            <span class="objective-note">${teamRemaining === 0 ? "Đã đạt mục tiêu 500 cặp" : `Còn thiếu ${teamRemaining} cặp ảnh`}</span>
          </div>
        </div>
        <div class="objective-pill">
          <span class="objective-label">Người nộp</span>
          <span class="objective-value">${submitter}</span>
          <span class="objective-note">${currentJobId ? `Job ${currentJobId}` : "Nhập trước khi xử lý"}</span>
        </div>
        <div class="objective-pill">
          <span class="objective-label">Auto detect</span>
          <span class="objective-value">${detected} cặp</span>
          <span class="objective-note">Cặp pipeline đề xuất</span>
        </div>
        <div class="objective-pill ${doneClass}">
          <span class="objective-label">Đã lưu</span>
          <span class="objective-value">${saved} cặp</span>
          <span class="objective-note">Cặp của job hiện tại</span>
        </div>
      `;
    }

    renderObjectiveProgress();

    async function fetchTeamStats() {
      try {
        const response = await fetch("/api/stats");
        if (!response.ok) return;
        const stats = await response.json();
        teamStats = {
          saved_count: Number(stats.saved_count || 0),
          objective_pairs: Number(stats.objective_pairs || 500),
          remaining_pairs: Number(stats.remaining_pairs || 0)
        };
        renderObjectiveProgress();
      } catch (err) {
        // Keep the local default if stats cannot be fetched.
      }
    }

    fetchTeamStats();

    async function parseJsonResponse(response) {
      const text = await response.text();
      let payload = {};
      if (text) {
        try {
          payload = JSON.parse(text);
        } catch (err) {
          payload = { detail: text };
        }
      }
      if (!response.ok) {
        const detail = payload.detail || payload.error || response.statusText || "Request failed";
        throw new Error(`${response.status}: ${detail}`);
      }
      return payload;
    }

    function optionLabel(alt) {
      const hog = Number(alt.hog_hits || 0);
      const warn = hog > 0 ? "⚠️ có người" : "";
      return `idx ${alt.high_idx} | điểm ${Number(alt.score).toFixed(2)} | sáng ${Number(alt.high_brightness).toFixed(1)} | match ${alt.good_matches} | inlier ${Number(alt.inlier_ratio).toFixed(2)} ${warn}`;
    }

    function frameOptionLabel(frame) {
      if (frame.direct_upload) {
        return `${frame.idx + 1}. ${frame.name}`;
      }
      return `idx ${frame.idx} | sáng ${Number(frame.brightness).toFixed(1)}`;
    }

    // Helper functions
    function findLowOption(idx) {
      return lowOptions.find(item => String(item.idx) === String(idx));
    }

    function findHighOption(idx) {
      return highOptions.find(item => String(item.idx) === String(idx));
    }

    function imageSrc(path) {
      if (!path) return "";
      if (path.startsWith("blob:") || path.startsWith("data:") || path.startsWith("http://") || path.startsWith("https://")) {
        return path;
      }
      return `/api/jobs/${currentJobId}/frame?path=${encodeURIComponent(path)}`;
    }

    function fmtNumber(value, digits = 2, fallback = "Upload trực tiếp") {
      if (value === null || value === undefined || value === "") return fallback;
      const number = Number(value);
      return Number.isFinite(number) ? number.toFixed(digits) : fallback;
    }

    function updatePairMeta(article, pair) {
      article.querySelector(".gap-value").textContent = `Chênh sáng: ${fmtNumber(pair.brightness_gap)}`;
      article.querySelector(".score-value").textContent = `Điểm: ${pair.score === null ? "Tự chọn" : Number(pair.score).toFixed(2)}`;
      article.querySelector(".matches-value").textContent = `Matches: ${pair.good_matches === null ? "Thủ công" : pair.good_matches}`;
      article.querySelector(".inlier-value").textContent = `Inlier: ${pair.inlier_ratio === null ? "Thủ công" : Number(pair.inlier_ratio).toFixed(2)}`;
      const hog = article.querySelector(".hog-value");
      if (hog) {
        hog.textContent = `Cảnh báo: Phát hiện người (${pair.hog_hits || 0})`;
        hog.style.display = Number(pair.hog_hits || 0) > 0 ? "inline-block" : "none";
      }
    }

    function renderPairs(pairs) {
      if (!pairs.length) {
        pairsEl.innerHTML = `
          <div class="empty-state">
            <svg viewBox="0 0 24 24"><path d="M19 5v14H5V5h14m0-2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-4.86 8.86l-3 3.87L9 13.14 6 17h12l-3.86-5.14z"/></svg>
            <p>Không có cặp nào được chọn</p>
            <span>Thử giảm High min, tăng High before/after hoặc giảm các trọng số phạt. Bạn cũng có thể click "Thêm cặp thủ công" để tự phối cảnh.</span>
          </div>
        `;
        if (pairsCountEl) pairsCountEl.textContent = "0 cặp";
        return;
      }
      if (pairsCountEl) pairsCountEl.textContent = `${pairs.length} cặp`;
      
      pairsEl.innerHTML = pairs.map(pair => {
        const isDirect = pair.mode === "direct_upload";
        const lowSelectOptions = lowOptions.map(frame => `
          <option value="${frame.idx}" ${frame.idx === pair.low_idx ? "selected" : ""}>${frameOptionLabel(frame)}</option>
        `).join("");
        const alternatives = pair.alternatives || [];
        const alternativeMap = new Map(alternatives.map(alt => [alt.high_idx, alt]));
        const highSelectOptions = highOptions.map(frame => {
          const alt = alternativeMap.get(frame.idx);
          const label = alt ? optionLabel(alt) : frameOptionLabel(frame);
          return `<option value="${frame.idx}" ${frame.idx === pair.selected_high_idx ? "selected" : ""}>${label}</option>`;
        }).join("");
        const controlsHtml = isDirect ? `
                <label>Chọn ảnh LOW
                  <select class="low-select">${lowSelectOptions}</select>
                </label>
                <label>Chọn ảnh HIGH
                  <select class="alt-select">${highSelectOptions}</select>
                </label>
                <button type="button" class="reject-btn">Loại Cặp này</button>
                <div class="direct-note">Batch upload trực tiếp: các cặp lưu trong lần này dùng chung một job/video group để tránh leakage khi chia train/val/test.</div>
        ` : `
                <label>Thay đổi Frame LOW
                  <select class="low-select">${lowSelectOptions}</select>
                </label>
                <label>Thay đổi Frame HIGH
                  <select class="alt-select">${highSelectOptions}</select>
                </label>
                <button type="button" class="reject-btn">Loại Cặp này</button>
        `;
        return `
          <article class="pair" data-pair-id="${pair.pair_id}">
            <div class="pickbox">
              <input type="checkbox" class="pick" ${pair.accepted !== false ? "checked" : ""} title="Chấp nhận cặp này">
            </div>
            <div class="pair-body">
              <div class="images">
                <figure>
                  <div class="img-tag low">LOW (THIẾU SÁNG)</div>
                  <img class="low-img" src="${imageSrc(pair.low_path)}" alt="Frame LOW ${pair.low_idx}">
                  <figcaption class="low-caption">LOW: ${isDirect ? pair.low_name || "ảnh upload trực tiếp" : `#${pair.low_idx} - Sáng: ${fmtNumber(pair.low_brightness)}`}</figcaption>
                </figure>
                <figure>
                  <div class="img-tag high">HIGH (ĐỦ SÁNG)</div>
                  <img class="high-img" src="${imageSrc(pair.high_path)}" alt="Frame HIGH ${pair.selected_high_idx}">
                  <figcaption class="high-caption">HIGH: ${isDirect ? pair.high_name || "ảnh upload trực tiếp" : `#${pair.selected_high_idx} - Sáng: ${fmtNumber(pair.high_brightness)}`}</figcaption>
                </figure>
              </div>
              <div class="controls">
                ${controlsHtml}
              </div>
              <div class="meta">
                <span>Cặp #${pair.pair_id}</span>
                <span>Đoạn tối: ${isDirect ? "Upload trực tiếp" : pair.segment_id === "manual" ? "Tự chọn thủ công" : `Phân đoạn ${pair.segment_id}`}</span>
                <span class="score-value">Điểm: ${pair.score === null ? "Tự chọn" : Number(pair.score).toFixed(2)}</span>
                <span class="gap-value">Chênh sáng: ${fmtNumber(pair.brightness_gap)}</span>
                <span class="matches-value">Matches: ${pair.good_matches === null ? "Thủ công" : pair.good_matches}</span>
                <span class="inlier-value">Inlier: ${pair.inlier_ratio === null ? "Thủ công" : Number(pair.inlier_ratio).toFixed(2)}</span>
                <span class="warn hog-value" style="${Number(pair.hog_hits || 0) > 0 ? "" : "display:none"}">Cảnh báo: Phát hiện người (${pair.hog_hits || 0})</span>
              </div>
            </div>
          </article>
        `;
      }).join("");

      document.querySelectorAll(".low-select").forEach(select => {
        select.addEventListener("change", event => {
          const article = event.target.closest(".pair");
          const pair = currentPairs.find(item => item.pair_id === Number(article.dataset.pairId));
          const low = findLowOption(event.target.value);
          if (!low) return;
          pair.low_idx = low.idx;
          pair.low_path = low.path;
          pair.low_brightness = low.brightness;
          pair.low_file_index = low.file_index;
          pair.low_name = low.name;
          pair.brightness_gap = Number(pair.high_brightness || 0) - Number(pair.low_brightness || 0);
          article.querySelector(".low-img").src = imageSrc(low.path);
          article.querySelector(".low-caption").textContent = pair.mode === "direct_upload" ? `LOW: ${low.name}` : `LOW: #${low.idx} - Sáng: ${fmtNumber(low.brightness)}`;
          updatePairMeta(article, pair);
        });
      });

      document.querySelectorAll(".alt-select").forEach(select => {
        select.addEventListener("change", event => {
          const article = event.target.closest(".pair");
          const pair = currentPairs.find(item => item.pair_id === Number(article.dataset.pairId));
          const highIdx = Number(event.target.value);
          const alt = (pair.alternatives || []).find(item => item.high_idx === highIdx);
          const frame = findHighOption(highIdx);
          if (!frame) return;
          pair.selected_high_idx = frame.idx;
          pair.high_idx = frame.idx;
          pair.high_path = frame.path;
          pair.high_brightness = frame.brightness;
          pair.high_file_index = frame.file_index;
          pair.high_name = frame.name;
          pair.brightness_gap = Number(pair.high_brightness || 0) - Number(pair.low_brightness || 0);
          pair.score = alt ? alt.score : null;
          pair.good_matches = alt ? alt.good_matches : null;
          pair.inlier_ratio = alt ? alt.inlier_ratio : null;
          pair.hog_hits = alt ? alt.hog_hits : null;
          article.querySelector(".high-img").src = imageSrc(frame.path);
          article.querySelector(".high-caption").textContent = pair.mode === "direct_upload" ? `HIGH: ${frame.name}` : `HIGH: #${frame.idx} - Sáng: ${fmtNumber(frame.brightness)}`;
          updatePairMeta(article, pair);
        });
      });

      document.querySelectorAll(".reject-btn").forEach(button => {
        button.addEventListener("click", event => {
          const article = event.target.closest(".pair");
          const checkbox = article.querySelector(".pick");
          checkbox.checked = false;
        });
      });
    }

    addPairBtn.addEventListener("click", () => {
      if (!lowOptions.length || !highOptions.length) return;
      const low = lowOptions[0];
      const high = highOptions[0];
      const nextId = currentPairs.length ? Math.max(...currentPairs.map(item => item.pair_id)) + 1 : 0;
      currentPairs.push({
        pair_id: nextId,
        segment_id: pendingDirectPair ? "direct" : "manual",
        mode: pendingDirectPair ? "direct_upload" : "manual",
        low_idx: low.idx,
        high_idx: high.idx,
        selected_high_idx: high.idx,
        low_file_index: low.file_index,
        high_file_index: high.file_index,
        low_name: low.name,
        high_name: high.name,
        low_path: low.path,
        high_path: high.path,
        low_brightness: low.brightness,
        high_brightness: high.brightness,
        brightness_gap: high.brightness - low.brightness,
        score: null,
        good_matches: null,
        inlier_ratio: null,
        hog_hits: null,
        accepted: true,
        alternatives: []
      });
      renderPairs(currentPairs);
      saveBtn.disabled = false;
    });

    async function savePendingDirectPair() {
      if (!pendingDirectPair) return;
      const reviewed = Array.from(document.querySelectorAll(".pair")).map(article => {
        const pair = currentPairs.find(item => item.pair_id === Number(article.dataset.pairId));
        const checked = article.querySelector(".pick").checked;
        return {
          ...pair,
          accepted: checked,
          human_decision: checked ? "accepted_direct" : "rejected"
        };
      });
      const acceptedCount = reviewed.filter(item => item.accepted).length;
      if (!acceptedCount) {
        clearPendingDirectBatch();
        currentPairs = [];
        renderPairs(currentPairs);
        saveBtn.disabled = true;
        statusEl.innerHTML = `<span class="error-message">Tất cả cặp ảnh đang bị bỏ chọn nên chưa lưu. Chọn lại ảnh nếu muốn upload.</span>`;
        return;
      }

      saveBtn.disabled = true;
      imagePairBtn.disabled = true;
      statusEl.innerHTML = `
        <div class="progress-container">
          <div class="spinner"></div>
          <span class="progress-text">Đang upload ${acceptedCount} cặp ảnh trực tiếp lên Cloudinary và lưu metadata...</span>
        </div>
      `;

      const body = new FormData();
      directLowFiles.forEach(file => body.append("low_images", file));
      directHighFiles.forEach(file => body.append("high_images", file));
      body.set("pairs_json", JSON.stringify(reviewed));
      body.set("submitted_by", getDirectSubmitter());
      body.set("objective_pairs", objectivePairsInput && objectivePairsInput.value ? objectivePairsInput.value : 500);

      try {
        const response = await fetch("/api/image-pairs", { method: "POST", body });
        const payload = await parseJsonResponse(response);

        currentJobId = payload.job_id;
        currentJobMeta = {
          submitted_by: payload.submitted_by || getDirectSubmitter(),
          objective_pairs: payload.objective_pairs || (objectivePairsInput && objectivePairsInput.value ? Number(objectivePairsInput.value) : 500),
          saved_count: payload.saved_count || payload.copied || 1
        };
        teamStats = {
          saved_count: Number(payload.team_saved_count || 0),
          objective_pairs: Number(payload.team_objective_pairs || 500),
          remaining_pairs: Number(payload.team_remaining_pairs || 0)
        };
        clearPendingDirectBatch();
        currentPairs = [];
        renderPairs(currentPairs);
        renderObjectiveProgress(currentJobMeta.saved_count);
        imagePairForm.reset();
        saveBtn.disabled = true;
        statusEl.innerHTML = `
          <span class="success-message">
            <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>
            Đã lưu thành công ${payload.copied} cặp ảnh trực tiếp trong cùng batch ${payload.job_id}. Tiến độ team: ${payload.team_saved_count}/${payload.team_objective_pairs}.
          </span>
        `;
      } catch (err) {
        saveBtn.disabled = false;
        statusEl.innerHTML = `<span class="error-message">Lỗi khi lưu cặp ảnh trực tiếp: ${err.message}</span>`;
      } finally {
        imagePairBtn.disabled = false;
      }
    }

    saveBtn.addEventListener("click", async () => {
      if (pendingDirectPair) {
        await savePendingDirectPair();
        return;
      }
      if (!currentJobId) return;
      const reviewed = Array.from(document.querySelectorAll(".pair")).map(article => {
        const pair = currentPairs.find(item => item.pair_id === Number(article.dataset.pairId));
        const checked = article.querySelector(".pick").checked;
        return {
          ...pair,
          accepted: checked,
          human_decision: checked ? "accepted" : "rejected"
        };
      });

      saveBtn.disabled = true;
      statusEl.innerHTML = `
        <div class="progress-container">
          <div class="spinner"></div>
          <span class="progress-text">Đang lưu cặp ảnh và sao chép tệp...</span>
        </div>
      `;

      try {
        const response = await fetch(`/api/jobs/${currentJobId}/save`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ pairs: reviewed })
        });
        const payload = await parseJsonResponse(response);
        currentJobMeta.saved_count = payload.saved_count || payload.copied || 0;
        currentJobMeta.objective_pairs = payload.objective_pairs || currentJobMeta.objective_pairs;
        currentJobMeta.submitted_by = payload.submitted_by || currentJobMeta.submitted_by;
        renderObjectiveProgress(currentJobMeta.saved_count);
        if (payload.team_objective_pairs) {
          teamStats = {
            saved_count: Number(payload.team_saved_count || 0),
            objective_pairs: Number(payload.team_objective_pairs || 500),
            remaining_pairs: Number(payload.team_remaining_pairs || 0)
          };
        }
        const remainingText = payload.team_objective_pairs ? ` Tiến độ team: ${payload.team_saved_count}/${payload.team_objective_pairs}, còn thiếu ${payload.team_remaining_pairs} cặp.` : "";
        statusEl.innerHTML = `
          <span class="success-message">
            <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>
            Đã lưu thành công ${payload.copied} cặp đã duyệt vào thư mục kết quả.${remainingText}
          </span>
        `;
      } catch (err) {
        statusEl.innerHTML = `<span class="error-message">Lỗi khi lưu cặp ảnh: ${err.message}</span>`;
      }
      saveBtn.disabled = false;
    });
  </script>
</body>
</html>"""


def process_job(job_id: str, video_path: Path, config: PipelineConfig) -> None:
    jobs[job_id].update({"status": "running", "message": "Đang tách frame, chấm điểm ứng viên và chọn cặp mặc định..."})
    
    # Update DB status to running
    db = SessionLocal()
    try:
        db_job = db.query(DBJob).filter(DBJob.job_id == job_id).first()
        if db_job:
            db_job.status = "running"
            db_job.message = "Đang tách frame, chấm điểm ứng viên và chọn cặp mặc định..."
            db.commit()
    except Exception as e:
        print(f"Error updating job running state in DB: {e}")
    finally:
        db.close()

    try:
        manifest = run_pipeline(video_path, JOB_DIR / job_id, config)
        
        # Update memory state
        jobs[job_id].update(
            {
                "status": "done",
                "message": "Hoàn tất",
                "pairs": manifest["pairs"],
                "low_options": manifest["low_options"],
                "high_options": manifest["high_options"],
                "summary": manifest["summary"],
                "config": manifest["config"],
                "submitted_by": jobs[job_id].get("submitted_by"),
                "objective_pairs": jobs[job_id].get("objective_pairs"),
                "saved_count": jobs[job_id].get("saved_count", 0),
            }
        )
        
        # Save to database
        db = SessionLocal()
        try:
            db_job = db.query(DBJob).filter(DBJob.job_id == job_id).first()
            if db_job:
                db_job.status = "done"
                db_job.message = "Hoàn tất"
                db_job.config_json = json.dumps(manifest["config"])
                db_job.low_options_json = json.dumps(manifest["low_options"])
                db_job.high_options_json = json.dumps(manifest["high_options"])
                db_job.summary_json = json.dumps(manifest["summary"])
                
                # Insert pair candidates
                for pair in manifest["pairs"]:
                    db_pair = DBPair(
                        job_id=job_id,
                        pair_id=pair["pair_id"],
                        segment_id=str(pair["segment_id"]),
                        low_idx=pair["low_idx"],
                        high_idx=pair["high_idx"],
                        selected_high_idx=pair["selected_high_idx"],
                        low_path=pair["low_path"],
                        high_path=pair["high_path"],
                        low_brightness=pair["low_brightness"],
                        high_brightness=pair["high_brightness"],
                        brightness_gap=pair["brightness_gap"],
                        score=pair["score"],
                        good_matches=pair["good_matches"],
                        inlier_ratio=pair["inlier_ratio"],
                        hog_hits=pair["hog_hits"],
                        accepted=pair["accepted"],
                        alternatives_json=json.dumps(pair["alternatives"])
                    )
                    db.add(db_pair)
                db.commit()
        except Exception as db_exc:
            print(f"Error saving job output to DB: {db_exc}")
            db.rollback()
        finally:
            db.close()

    except Exception as exc:
        jobs[job_id].update({"status": "failed", "error": str(exc)})
        
        # Update DB status to failed
        db = SessionLocal()
        try:
            db_job = db.query(DBJob).filter(DBJob.job_id == job_id).first()
            if db_job:
                db_job.status = "failed"
                db_job.message = "Thất bại"
                db_job.error = str(exc)
                db.commit()
        except Exception as db_exc:
            print(f"Error saving job failed state to DB: {db_exc}")
        finally:
            db.close()


@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML


@app.get("/api/stats")
def get_stats():
    db = SessionLocal()
    try:
        saved_count = db.query(DBReviewedPair).count()
    finally:
        db.close()
    return {
        "objective_pairs": TEAM_OBJECTIVE_PAIRS,
        "saved_count": saved_count,
        "remaining_pairs": max(TEAM_OBJECTIVE_PAIRS - saved_count, 0),
    }


@app.get("/api/health")
def health_check():
    db = SessionLocal()
    try:
        db.execute(text("SELECT 1"))
        db_ok = True
        db_error = None
    except Exception as exc:
        db_ok = False
        db_error = str(exc)
    finally:
        db.close()

    return {
        "app": "ok",
        "database": {
            "ok": db_ok,
            "error": db_error,
            "init_error": DB_INIT_ERROR,
        },
        "cloudinary": cloudinary_health(),
        "team_objective_pairs": TEAM_OBJECTIVE_PAIRS,
    }


def save_reviewed_outputs(job_id: str, reviewed_pairs: list[dict], start_idx: int) -> tuple[int, list[dict], str]:
    rows: list[dict] = []
    copied = 0
    use_cloudinary = cloudinary_enabled()

    if use_cloudinary:
        prefix = f"{storage_prefix()}/{job_id}"
        storage_mode = "cloudinary"
    else:
        low_dir = DATASET_DIR / "low"
        high_dir = DATASET_DIR / "high"
        for folder in [low_dir, high_dir]:
            folder.mkdir(parents=True, exist_ok=True)
        prefix = ""
        storage_mode = "local"

    for item in reviewed_pairs:
        if not item.get("accepted", True):
            continue
        low_path = Path(item["low_path"])
        high_path = Path(item["high_path"])
        if not low_path.exists() or not high_path.exists():
            continue

        dst_idx = start_idx + copied
        low_name = f"pair_{dst_idx:06d}_low.png"
        high_name = f"pair_{dst_idx:06d}_high.png"

        if use_cloudinary:
            low_key = f"{prefix}/low/{low_name}"
            high_key = f"{prefix}/high/{high_name}"
            saved_low = upload_file(low_path, low_key, "image/png")
            saved_high = upload_file(high_path, high_key, "image/png")
        else:
            dst_low = low_dir / low_name
            dst_high = high_dir / high_name
            shutil.copy2(low_path, dst_low)
            shutil.copy2(high_path, dst_high)
            saved_low = str(dst_low)
            saved_high = str(dst_high)

        rows.append(
            {
                "pair_id": dst_idx,
                "source_pair_id": item.get("pair_id"),
                "job_id": job_id,
                "submitted_by": item.get("submitted_by"),
                "objective_pairs": item.get("objective_pairs"),
                "low_path": str(low_path),
                "high_path": str(high_path),
                "saved_low": saved_low,
                "saved_high": saved_high,
                "score": item.get("score"),
                "human_decision": item.get("human_decision", "accepted"),
                "storage": storage_mode,
            }
        )
        copied += 1

    return copied, rows, storage_mode


def cleanup_job_files(job_id: str) -> None:
    shutil.rmtree(JOB_DIR / job_id, ignore_errors=True)
    for path in UPLOAD_DIR.glob(f"{job_id}*"):
        if path.is_file():
            path.unlink(missing_ok=True)


def parse_objective(value) -> int | None:
    try:
        objective = int(value) if value not in ("", None) else None
    except (TypeError, ValueError):
        return None
    return objective if objective and objective > 0 else None


def safe_int(value, fallback: int | None = None) -> int | None:
    if value in ("", None):
        return fallback
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def safe_float(value, fallback: float | None = None) -> float | None:
    if value in ("", None):
        return fallback
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


@app.post("/api/image-pairs")
async def create_image_pair(request: Request):
    form = await request.form()
    submitted_by = str(form.get("submitted_by") or "").strip() or "Không rõ"
    objective_pairs = parse_objective(form.get("objective_pairs"))
    job_id = uuid.uuid4().hex[:12]
    direct_dir = JOB_DIR / job_id / "direct_pair"
    direct_dir.mkdir(parents=True, exist_ok=True)
    low_dir = direct_dir / "low"
    high_dir = direct_dir / "high"
    low_dir.mkdir(parents=True, exist_ok=True)
    high_dir.mkdir(parents=True, exist_ok=True)

    def image_suffix(filename: str | None) -> str:
        suffix = Path(filename or "").suffix.lower()
        return suffix if suffix in {".png", ".jpg", ".jpeg", ".webp", ".bmp"} else ".png"

    low_images = list(form.getlist("low_images"))
    high_images = list(form.getlist("high_images"))
    # Backward compatibility with the older one-low/one-high form names.
    if not low_images and form.get("low_image") is not None:
        low_images = [form.get("low_image")]
    if not high_images and form.get("high_image") is not None:
        high_images = [form.get("high_image")]
    low_images = [item for item in low_images if hasattr(item, "file")]
    high_images = [item for item in high_images if hasattr(item, "file")]
    if not low_images or not high_images:
        raise HTTPException(status_code=400, detail="Cần upload ít nhất 1 ảnh LOW và 1 ảnh HIGH.")

    low_paths: list[Path] = []
    high_paths: list[Path] = []
    for idx, image in enumerate(low_images):
        path = low_dir / f"low_{idx:04d}{image_suffix(image.filename)}"
        with path.open("wb") as handle:
            shutil.copyfileobj(image.file, handle)
        low_paths.append(path)
    for idx, image in enumerate(high_images):
        path = high_dir / f"high_{idx:04d}{image_suffix(image.filename)}"
        with path.open("wb") as handle:
            shutil.copyfileobj(image.file, handle)
        high_paths.append(path)

    pairs_raw = str(form.get("pairs_json") or "").strip()
    if pairs_raw:
        try:
            submitted_pairs = json.loads(pairs_raw)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="pairs_json không hợp lệ.") from exc
    else:
        submitted_pairs = [
            {
                "pair_id": idx,
                "low_file_index": idx,
                "high_file_index": idx,
                "accepted": True,
                "human_decision": "accepted_direct",
            }
            for idx in range(min(len(low_paths), len(high_paths)))
        ]

    reviewed_pairs: list[dict] = []
    db_pair_rows: list[dict] = []
    for fallback_id, item in enumerate(submitted_pairs):
        try:
            low_index = int(item.get("low_file_index", item.get("low_idx", 0)))
            high_index = int(item.get("high_file_index", item.get("selected_high_idx", item.get("high_idx", 0))))
        except (TypeError, ValueError):
            continue
        if low_index < 0 or low_index >= len(low_paths) or high_index < 0 or high_index >= len(high_paths):
            continue
        pair_id = int(item.get("pair_id", fallback_id))
        accepted = bool(item.get("accepted", True))
        low_path = low_paths[low_index]
        high_path = high_paths[high_index]
        decision = str(item.get("human_decision") or ("accepted_direct" if accepted else "rejected"))[:20]
        reviewed_pairs.append(
            {
                "pair_id": pair_id,
                "job_id": job_id,
                "submitted_by": submitted_by,
                "objective_pairs": objective_pairs,
                "accepted": accepted,
                "human_decision": decision,
                "low_path": str(low_path),
                "high_path": str(high_path),
                "score": None,
            }
        )
        db_pair_rows.append(
            {
                "pair_id": pair_id,
                "low_index": low_index,
                "high_index": high_index,
                "low_path": str(low_path),
                "high_path": str(high_path),
                "accepted": accepted,
            }
        )

    if not reviewed_pairs:
        raise HTTPException(status_code=400, detail="Không có cặp ảnh hợp lệ để lưu.")

    jobs[job_id] = {
        "status": "done",
        "message": "Đã lưu batch ảnh trực tiếp",
        "pairs": [],
        "summary": {
            "mode": "direct_image_batch",
            "low_images": len(low_paths),
            "high_images": len(high_paths),
            "submitted_pairs": len(reviewed_pairs),
        },
        "submitted_by": submitted_by,
        "objective_pairs": objective_pairs,
        "saved_count": 0,
    }

    db = SessionLocal()
    try:
        start_idx = db.query(DBReviewedPair).count()
    finally:
        db.close()

    try:
        copied, saved_records, storage_mode = save_reviewed_outputs(job_id, reviewed_pairs, start_idx)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi upload/lưu file ảnh trực tiếp: {exc}") from exc
    if not saved_records:
        raise HTTPException(status_code=400, detail="Không lưu được cặp ảnh. Kiểm tra định dạng file ảnh.")
    saved_by_source_pair = {record["source_pair_id"]: record for record in saved_records}

    db = SessionLocal()
    try:
        db_job = DBJob(
            job_id=job_id,
            status="done",
            message="Đã lưu batch ảnh trực tiếp",
            video_name=f"direct_image_batch_{job_id}",
            submitted_by=submitted_by,
            objective_pairs=objective_pairs,
            summary_json=json.dumps({
                "mode": "direct_image_batch",
                "low_images": len(low_paths),
                "high_images": len(high_paths),
                "submitted_pairs": len(reviewed_pairs),
            }),
        )
        db.add(db_job)
        for row in db_pair_rows:
            db.add(
                DBPair(
                    job_id=job_id,
                    pair_id=row["pair_id"],
                    segment_id="direct",
                    low_idx=row["low_index"],
                    high_idx=row["high_index"],
                    selected_high_idx=row["high_index"],
                    low_path=row["low_path"],
                    high_path=row["high_path"],
                    accepted=row["accepted"],
                    alternatives_json="[]",
                )
            )
        for item in reviewed_pairs:
            saved_record = saved_by_source_pair.get(item["pair_id"])
            if not saved_record:
                continue
            db.add(
                DBReviewedPair(
                    dataset_pair_id=saved_record["pair_id"],
                    job_id=job_id,
                    submitted_by=submitted_by,
                    source_pair_id=item["pair_id"],
                    low_path=item["low_path"],
                    high_path=item["high_path"],
                    saved_low=saved_record["saved_low"],
                    saved_high=saved_record["saved_high"],
                    score=None,
                    human_decision=item.get("human_decision", "accepted_direct")[:20],
                )
            )
        db.commit()
        saved_count = db.query(DBReviewedPair).filter(DBReviewedPair.job_id == job_id).count()
        team_saved_count = db.query(DBReviewedPair).count()
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Lỗi lưu metadata cặp ảnh: {exc}") from exc
    finally:
        db.close()

    jobs[job_id]["saved_count"] = saved_count
    if storage_mode == "cloudinary" and cleanup_after_save():
        cleanup_job_files(job_id)

    return {
        "job_id": job_id,
        "copied": copied,
        "storage": storage_mode,
        "dataset_dir": "Cloudinary" if storage_mode == "cloudinary" else str(DATASET_DIR),
        "submitted_by": submitted_by,
        "objective_pairs": objective_pairs,
        "saved_count": saved_count,
        "team_objective_pairs": TEAM_OBJECTIVE_PAIRS,
        "team_saved_count": team_saved_count,
        "team_remaining_pairs": max(TEAM_OBJECTIVE_PAIRS - team_saved_count, 0),
        "source_group": f"direct_image_batch_{job_id}",
        "saved_pairs": saved_records,
    }


@app.post("/api/jobs")
async def create_job(request: Request, video: UploadFile = File(...)):
    form = await request.form()
    config = config_from_form(dict(form))
    submitted_by = str(form.get("submitted_by") or "").strip() or "Không rõ"
    objective_pairs = parse_objective(form.get("objective_pairs"))
    suffix = Path(video.filename or "upload.mp4").suffix or ".mp4"
    job_id = uuid.uuid4().hex[:12]
    video_path = UPLOAD_DIR / f"{job_id}{suffix}"

    with video_path.open("wb") as handle:
        shutil.copyfileobj(video.file, handle)

    # Memory state
    jobs[job_id] = {
        "status": "queued",
        "message": "Đang chờ xử lý",
        "pairs": [],
        "summary": {},
        "submitted_by": submitted_by,
        "objective_pairs": objective_pairs,
        "saved_count": 0,
    }
    
    # Save queued job state to Database
    db = SessionLocal()
    try:
        db_job = DBJob(
            job_id=job_id,
            status="queued",
            message="Đang chờ xử lý",
            video_name=video.filename,
            submitted_by=submitted_by,
            objective_pairs=objective_pairs,
        )
        db.add(db_job)
        db.commit()
    except Exception as e:
        print(f"Error creating job record in DB: {e}")
        db.rollback()
    finally:
        db.close()

    executor.submit(process_job, job_id, video_path, config)
    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    job = jobs.get(job_id)
    if job is not None:
        return job

    # If not in active memory, query DB
    db = SessionLocal()
    try:
        db_job = db.query(DBJob).filter(DBJob.job_id == job_id).first()
        if db_job:
            # If job failed
            if db_job.status == "failed":
                return {"status": "failed", "error": db_job.error or "Lỗi không xác định"}
            # If job is still running/queued (fallback/recovery)
            if db_job.status in ("queued", "running"):
                return {
                    "status": db_job.status,
                    "message": db_job.message,
                    "pairs": [],
                    "summary": {},
                    "submitted_by": db_job.submitted_by,
                    "objective_pairs": db_job.objective_pairs,
                    "saved_count": db.query(DBReviewedPair).filter(DBReviewedPair.job_id == job_id).count(),
                }
            
            # If job is done, rebuild the response from the DB tables
            db_pairs = db.query(DBPair).filter(DBPair.job_id == job_id).order_by(DBPair.pair_id).all()
            pairs = []
            for p in db_pairs:
                pairs.append({
                    "pair_id": p.pair_id,
                    "segment_id": p.segment_id,
                    "low_idx": p.low_idx,
                    "high_idx": p.high_idx,
                    "selected_high_idx": p.selected_high_idx,
                    "low_path": p.low_path,
                    "high_path": p.high_path,
                    "low_brightness": p.low_brightness,
                    "high_brightness": p.high_brightness,
                    "brightness_gap": p.brightness_gap,
                    "score": p.score,
                    "good_matches": p.good_matches,
                    "inlier_ratio": p.inlier_ratio,
                    "hog_hits": p.hog_hits,
                    "accepted": p.accepted,
                    "alternatives": json.loads(p.alternatives_json or "[]")
                })
            
            return {
                "status": "done",
                "pairs": pairs,
                "low_options": json.loads(db_job.low_options_json or "[]"),
                "high_options": json.loads(db_job.high_options_json or "[]"),
                "summary": json.loads(db_job.summary_json or "{}"),
                "config": json.loads(db_job.config_json or "{}"),
                "submitted_by": db_job.submitted_by,
                "objective_pairs": db_job.objective_pairs,
                "saved_count": db.query(DBReviewedPair).filter(DBReviewedPair.job_id == job_id).count(),
            }
    except Exception as e:
        print(f"Error querying job from DB: {e}")
    finally:
        db.close()

    # Fallback to local manifest file
    manifest_path = JOB_DIR / job_id / "manifest.json"
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail="Không tìm thấy job")
    manifest = load_manifest(JOB_DIR / job_id)
    return {
        "status": "done",
        "pairs": manifest["pairs"],
        "low_options": manifest["low_options"],
        "high_options": manifest["high_options"],
        "summary": manifest["summary"],
        "config": manifest["config"],
        "submitted_by": jobs.get(job_id, {}).get("submitted_by"),
        "objective_pairs": jobs.get(job_id, {}).get("objective_pairs"),
        "saved_count": jobs.get(job_id, {}).get("saved_count", 0),
    }


@app.get("/api/jobs/{job_id}/frame")
def get_frame(job_id: str, path: str):
    job_dir = (JOB_DIR / job_id).resolve()
    requested = Path(path).resolve()
    if job_dir not in requested.parents and requested != job_dir:
        raise HTTPException(status_code=403, detail="Đường dẫn nằm ngoài job")
    if not requested.exists():
        raise HTTPException(status_code=404, detail="Không tìm thấy ảnh")
    return FileResponse(requested)


@app.post("/api/jobs/{job_id}/save")
async def save_pairs(job_id: str, request: Request):
    # Check job folder or DB record existence
    db = SessionLocal()
    job_exists = False
    try:
        db_job = db.query(DBJob).filter(DBJob.job_id == job_id).first()
        if db_job:
            job_exists = True
    except Exception as e:
        print(f"Error checking job existence in DB: {e}")
    finally:
        db.close()

    if not job_exists and not (JOB_DIR / job_id).exists():
        raise HTTPException(status_code=404, detail="Không tìm thấy job")

    payload = await request.json()
    reviewed_pairs = payload.get("pairs", [])
    submitted_by = jobs.get(job_id, {}).get("submitted_by") or "Không rõ"
    objective_pairs = jobs.get(job_id, {}).get("objective_pairs")

    db = SessionLocal()
    try:
        db_job = db.query(DBJob).filter(DBJob.job_id == job_id).first()
        if db_job:
            submitted_by = db_job.submitted_by or submitted_by
            objective_pairs = db_job.objective_pairs
    except Exception as e:
        print(f"Error reading job metadata before saving files: {e}")
    finally:
        db.close()

    for item in reviewed_pairs:
        item["job_id"] = job_id
        item["submitted_by"] = submitted_by
        item["objective_pairs"] = objective_pairs

    db = SessionLocal()
    try:
        start_idx = db.query(DBReviewedPair).count()
    finally:
        db.close()

    try:
        copied, saved_records, storage_mode = save_reviewed_outputs(job_id, reviewed_pairs, start_idx)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi upload/lưu file cặp ảnh: {exc}") from exc
    saved_by_source_pair = {record["source_pair_id"]: record for record in saved_records}

    # Save metadata to DB
    db = SessionLocal()
    try:
        db_job = db.query(DBJob).filter(DBJob.job_id == job_id).first()
        if db_job:
            submitted_by = db_job.submitted_by or submitted_by
            objective_pairs = db_job.objective_pairs

        for item in reviewed_pairs:
            # Update pair candidate accepted/selected_high_idx in DB
            db_pair = db.query(DBPair).filter(DBPair.job_id == job_id, DBPair.pair_id == item["pair_id"]).first()
            if db_pair:
                db_pair.accepted = bool(item.get("accepted", True))
                db_pair.selected_high_idx = safe_int(item.get("selected_high_idx"), db_pair.selected_high_idx)
                # If they rematched LOW/HIGH, update these fields as well
                db_pair.low_idx = safe_int(item.get("low_idx"), db_pair.low_idx)
                db_pair.high_idx = safe_int(item.get("high_idx"), db_pair.high_idx)
                db_pair.low_path = str(item.get("low_path", db_pair.low_path))
                db_pair.high_path = str(item.get("high_path", db_pair.high_path))
                db_pair.low_brightness = safe_float(item.get("low_brightness"), db_pair.low_brightness)
                db_pair.high_brightness = safe_float(item.get("high_brightness"), db_pair.high_brightness)
                db_pair.brightness_gap = safe_float(item.get("brightness_gap"), db_pair.brightness_gap)
                db_pair.score = item.get("score", db_pair.score)
                db_pair.good_matches = item.get("good_matches", db_pair.good_matches)
                db_pair.inlier_ratio = item.get("inlier_ratio", db_pair.inlier_ratio)
                db_pair.hog_hits = item.get("hog_hits", db_pair.hog_hits)
            
            saved_record = saved_by_source_pair.get(item.get("pair_id"))
            if saved_record:
                db_reviewed = DBReviewedPair(
                    dataset_pair_id=saved_record["pair_id"],
                    job_id=job_id,
                    submitted_by=submitted_by,
                    source_pair_id=item.get("pair_id"),
                    low_path=item["low_path"],
                    high_path=item["high_path"],
                    saved_low=saved_record["saved_low"],
                    saved_high=saved_record["saved_high"],
                    score=item.get("score"),
                    human_decision=str(item.get("human_decision", "accepted"))[:20]
                )
                db.add(db_reviewed)
                
        db.commit()
        saved_count = db.query(DBReviewedPair).filter(DBReviewedPair.job_id == job_id).count()
    except Exception as db_exc:
        print(f"Error saving reviewed pairs to DB: {db_exc}")
        db.rollback()
        saved_count = copied
    finally:
        db.close()

    if job_id in jobs:
        jobs[job_id]["saved_count"] = saved_count
        jobs[job_id]["submitted_by"] = submitted_by
        jobs[job_id]["objective_pairs"] = objective_pairs
    remaining_pairs = max(int(objective_pairs or 0) - int(saved_count or 0), 0) if objective_pairs else None
    db = SessionLocal()
    try:
        team_saved_count = db.query(DBReviewedPair).count()
    finally:
        db.close()
    if storage_mode == "cloudinary" and cleanup_after_save():
        cleanup_job_files(job_id)
    return {
        "copied": copied,
        "dataset_dir": "Cloudinary" if storage_mode == "cloudinary" else str(DATASET_DIR),
        "storage": storage_mode,
        "submitted_by": submitted_by,
        "objective_pairs": objective_pairs,
        "saved_count": saved_count,
        "remaining_pairs": remaining_pairs,
        "team_objective_pairs": TEAM_OBJECTIVE_PAIRS,
        "team_saved_count": team_saved_count,
        "team_remaining_pairs": max(TEAM_OBJECTIVE_PAIRS - team_saved_count, 0),
    }
