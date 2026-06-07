# Lowlight Collector

Web platform để upload video, tự tách các cặp ảnh LOW/HIGH, cho user review lại LOW/HIGH thủ công, rồi lưu dataset đã duyệt.

App có tracking cho từng video:

- **Người nộp**: lưu tên member upload video.
- **Mục tiêu số cặp ảnh**: hiển thị đã lưu bao nhiêu cặp và còn thiếu bao nhiêu cặp.
- Metadata được lưu vào Postgres và CSV trong `web_platform/web_data/selected_dataset/metadata`.

## Chạy Local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn web_platform.app:app --host 0.0.0.0 --port 8000
```

Mở:

```text
http://localhost:8000
```

## Deploy Render

Repo đã có `render.yaml`, gồm:

- Web service FastAPI
- PostgreSQL để lưu metadata job/pair
- Persistent disk mount vào `web_platform/web_data` để lưu upload, frame, pair output

Các bước:

1. Push repo này lên GitHub.
2. Vào Render Dashboard.
3. Chọn **New > Blueprint**.
4. Connect GitHub repo này.
5. Render đọc `render.yaml` và tạo service/database/disk.
6. Deploy xong mở URL Render cấp.

Lưu ý: app cần persistent disk nếu muốn giữ ảnh/video/output sau restart hoặc redeploy. Nếu không gắn disk, dữ liệu file trong `web_platform/web_data` có thể mất.

## File Chính

```text
web_platform/app.py          FastAPI app + UI
web_platform/pipeline.py     Pipeline tách frame, bắt cặp LOW/HIGH
web_platform/database.py     SQLAlchemy models cho Postgres/SQLite
web_platform/README.md       Hướng dẫn chạy app chi tiết hơn
web_platform/WEB_USAGE_GUIDE.md  Hướng dẫn tham số cho user
render.yaml                  Cấu hình Render Blueprint
requirements.txt             Python dependencies
runtime.txt                  Python version cho Render
```
