# Trình Tạo Cặp Ảnh Low/High

Web app local để tạo dataset cặp ảnh thiếu sáng/đủ sáng từ video upload.

Luồng sử dụng:

1. Tải video lên.
2. Chỉnh tham số pipeline nếu cần.
3. Review các cặp LOW/HIGH được chọn tự động.
4. Nếu cặp chưa đúng, đổi **Frame LOW** hoặc **Frame HIGH** bằng dropdown.
5. Dùng **Thêm cặp thủ công** nếu pipeline bỏ sót một cặp tốt.
6. Loại cặp xấu hoặc lưu các cặp đã duyệt.

Hướng dẫn tham số chi tiết nằm ở `WEB_USAGE_GUIDE.md`.

## Chạy App

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn web_platform.app:app --host 0.0.0.0 --port 8000
```

Mở `http://localhost:8000`.

Nếu member dùng chung Wi-Fi/LAN, lấy IP máy host bằng:

```bash
ip route get 1.1.1.1 | awk '{for(i=1;i<=NF;i++) if ($i=="src") print $(i+1)}'
```

Sau đó member mở:

```text
http://YOUR_LOCAL_IP:8000
```

Nếu không cùng mạng, dùng private tunnel như Tailscale hoặc Cloudflare Tunnel và trỏ vào port `8000`.

Nếu chạy local hoặc chưa cấu hình Cloudinary, các cặp đã chọn được lưu vào:

```text
web_data/selected_dataset/low
web_data/selected_dataset/high
```

Khi deploy Render free với Cloudinary, ảnh LOW/HIGH final được upload lên Cloudinary theo folder:

```text
lowlight_datasets/<job_id>/low
lowlight_datasets/<job_id>/high
```

Supabase Postgres lưu metadata gồm `job_id`, `submitted_by`, đường dẫn ảnh gốc tạm thời và URL Cloudinary đã lưu.
