# Lowlight Collector

Web platform để upload video, tự tách các cặp ảnh LOW/HIGH, cho user review lại LOW/HIGH thủ công, rồi lưu dataset đã duyệt.

App có tracking cho team và từng video:

- **Người nộp**: lưu tên member upload video.
- **Mục tiêu số cặp ảnh**: hiển thị đã lưu bao nhiêu cặp và còn thiếu bao nhiêu cặp.
- Mục tiêu team mặc định: **500 cặp ảnh**.
- Ảnh final có thể lưu lên Cloudinary nếu cấu hình biến môi trường Cloudinary.
- Metadata được lưu vào Supabase Postgres hoặc Postgres tương thích.

## Chạy Local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn web_platform.app:app --host 0.0.0.0 --port 8000
```

Điền `DATABASE_URL` và Cloudinary credentials vào `.env` nếu muốn test Supabase/Cloudinary local. File `.env` đã được ignore, không commit.

Mở:

```text
http://localhost:8000
```

## Deploy Render

Repo đã có `render.yaml`, gồm:

- Web service FastAPI
- Supabase Postgres để lưu metadata job/pair
- Cloudinary để lưu ảnh LOW/HIGH final lâu dài
- Python pinned ở `3.11.15` qua `PYTHON_VERSION` và `.python-version`

Render free không hỗ trợ persistent disk, nên app dùng `web_platform/web_data` làm nơi xử lý tạm. Sau khi lưu cặp đã duyệt, ảnh final được upload lên Cloudinary và file tạm có thể được cleanup.

## Supabase Env Var

Tạo Supabase project, lấy Postgres connection string rồi điền vào Render:

```text
DATABASE_URL=<supabase_postgres_connection_string>
```

Nếu Supabase đưa URL dạng `postgres://...`, app sẽ tự đổi sang `postgresql://...` cho SQLAlchemy.

## Cloudinary Env Vars

Tạo Cloudinary account, lấy API credentials rồi điền các biến này trong Render:

```text
CLOUDINARY_CLOUD_NAME=<cloud_name>
CLOUDINARY_API_KEY=<api_key>
CLOUDINARY_API_SECRET=<api_secret>
CLOUDINARY_FOLDER=lowlight_datasets
CLEANUP_AFTER_SAVE=true
```

Nếu muốn dùng `CLOUDINARY_URL` thay 3 biến riêng cũng được, nhưng `render.yaml` hiện khai báo 3 biến riêng cho rõ.

Nếu chưa cấu hình Cloudinary, app fallback lưu local vào `web_platform/web_data/selected_dataset`, nhưng dữ liệu local trên Render free có thể mất sau restart/redeploy.

Các bước:

1. Push repo này lên GitHub.
2. Vào Render Dashboard.
3. Chọn **New > Blueprint**.
4. Connect GitHub repo này.
5. Render đọc `render.yaml` và tạo web service.
6. Điền `DATABASE_URL` từ Supabase và credentials Cloudinary trong Environment.
7. Deploy xong mở URL Render cấp.

## Kiểm Tra Sau Deploy

Mở:

```text
https://<render-service-url>/api/health
```

Kết quả tốt sẽ có:

```json
{
  "app": "ok",
  "database": {"ok": true},
  "cloudinary": {"configured": true, "ok": true},
  "team_objective_pairs": 500
}
```

Kiểm tra progress dataset:

```text
https://<render-service-url>/api/stats
```

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
