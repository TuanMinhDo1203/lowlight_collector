# Lowlight Collector

Web platform để upload video, tự tách các cặp ảnh LOW/HIGH, cho user review lại LOW/HIGH thủ công, rồi lưu dataset đã duyệt.

Ngoài video, form upload ảnh nhận riêng hai nhóm LOW/reference, chạy matching để đề xuất cặp trước khi review, rồi cho chọn dùng chung `job_id` hoặc mỗi cặp một `job_id` riêng.
Ảnh HEIC/HEIF được tự động chuyển thành PNG trên backend trước khi matching và upload lên Drive.

App có tracking cho team và từng video:

- **Người nộp**: lưu tên member upload video.
- **Mục tiêu số cặp ảnh**: hiển thị đã lưu bao nhiêu cặp và còn thiếu bao nhiêu cặp.
- Mục tiêu team mặc định: **500 cặp ảnh**.
- Ảnh final được lưu lên Google Drive thông qua rclone; nếu rclone chưa sẵn sàng, thao tác lưu sẽ báo lỗi.
- Metadata được lưu vào Supabase Postgres hoặc Postgres tương thích.

## Chạy Local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn web_platform.app:app --host 0.0.0.0 --port 8000
```

Điền `DATABASE_URL` và các biến `RCLONE_*` vào `.env`. File `.env` đã được ignore, không commit.

Mở:

```text
http://localhost:8000
```

## Deploy Render

Repo đã có `render.yaml`, gồm:

- Web service FastAPI
- Supabase Postgres để lưu metadata job/pair
- Google Drive qua rclone để lưu ảnh LOW/reference final lâu dài
- Python pinned ở `3.11.15` qua `PYTHON_VERSION` và `.python-version`

App dùng `web_platform/web_data` làm nơi xử lý tạm. Sau khi lưu cặp đã duyệt thành công, ảnh final được upload lên Google Drive và file tạm được cleanup.

## Supabase Env Var

Tạo Supabase project, lấy Postgres connection string rồi điền vào Render:

```text
DATABASE_URL=<supabase_postgres_connection_string>
```

Nếu Supabase đưa URL dạng `postgres://...`, app sẽ tự đổi sang `postgresql://...` cho SQLAlchemy.

## Rclone Env Vars

Rclone phải được cài trên host và remote `drive` phải được cấu hình trước. Điền các biến sau:

```text
RCLONE_REMOTE=drive
RCLONE_CONFIG=/home/<username>/.config/rclone/rclone.conf
RCLONE_DATASET_ROOT=LLIE_Dataset
```

Ảnh được lưu theo cấu trúc:

```text
drive:LLIE_Dataset/raw/low/<filename>
drive:LLIE_Dataset/raw/reference/<filename>
```

Không đặt `rclone.conf` trong repository. Khi deploy, provision rclone binary và config bằng cơ chế Secret File của nền tảng, rồi đặt `RCLONE_CONFIG` trỏ tới file đó. Nếu rclone, remote hoặc config lỗi, thao tác lưu sẽ báo lỗi thay vì fallback local.

Các bước:

1. Push repo này lên GitHub.
2. Vào Render Dashboard.
3. Chọn **New > Blueprint**.
4. Connect GitHub repo này.
5. Render đọc `render.yaml` và tạo web service.
6. Điền `DATABASE_URL`, các biến `RCLONE_*`, và provision `rclone.conf` dưới dạng secret ngoài repository.
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
  "rclone": {"configured": true, "ok": true},
  "team_objective_pairs": 500
}
```

Kiểm tra progress dataset:

```text
https://<render-service-url>/api/stats
```

Progress được đếm trực tiếp từ các cặp filename khớp nhau trong `raw/low` và `raw/reference`, không lấy từ số row trong database.

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
