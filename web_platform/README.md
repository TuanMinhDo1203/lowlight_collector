# Trình Tạo Cặp Ảnh Low/High

Web app local để tạo dataset cặp ảnh thiếu sáng/đủ sáng từ video upload.

Luồng sử dụng:

1. Tải video lên để pipeline tự tìm cặp, hoặc upload riêng hai nhóm ảnh LOW và reference có sẵn.
   File HEIC/HEIF được đọc thời gian chụp trước khi backend tự động chuyển thành PNG.
2. Chỉnh tham số pipeline nếu cần.
3. Review các cặp LOW/HIGH được chọn tự động.
4. Nếu cặp chưa đúng, đổi **Frame LOW** hoặc **Frame HIGH** bằng dropdown.
5. Dùng **Thêm cặp thủ công** nếu pipeline bỏ sót một cặp tốt.
6. Với hai nhóm ảnh có sẵn, app đọc EXIF/HEIC capture time, sắp xếp LOW và reference riêng rồi ghép tuần tự; ảnh thiếu time metadata giữ thứ tự upload và được báo trong summary. Có thể dùng dropdown để chỉnh lại LOW/reference, thêm cặp hoặc loại cặp trước khi lưu. Dropdown **Cách gán job_id / source group** cho phép chọn cả batch dùng chung một `job_id` hoặc mỗi cặp accepted dùng một `job_id` riêng.
   Thanh tiến độ hiển thị pha đọc/chuyển định dạng và số cặp thời gian đã ghép, không còn quét toàn bộ ma trận LOW x reference.
7. Dashboard thống kê số cặp theo người chụp từ inventory Google Drive hiện tại, sau đó mới đối chiếu `saved_low`/`saved_high` với database để loại các row lịch sử không còn file.
8. Khi đang lưu lên Drive, preview được khóa để tab/request khác không thể xóa file tạm giữa chừng. Nếu upload lỗi, preview được giữ lại để người dùng bấm lưu lại.

Khi lưu, thanh progress hiển thị phần trăm, số file và số cặp đã upload xong lên Google Drive. Tiến độ được cập nhật sau từng lệnh `rclone copyto`.

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

Khi bấm lưu, ảnh LOW/reference final được upload qua rclone theo folder:

```text
drive:LLIE_Dataset/raw/low
drive:LLIE_Dataset/raw/reference
```

`job_id` vẫn được lưu trong metadata để phân biệt source group. Chế độ **Mỗi cặp một job_id/source group riêng** chỉ thay đổi metadata grouping; file ảnh vẫn nằm trong hai thư mục dataset chuẩn ở trên.

Supabase Postgres lưu metadata gồm `job_id`, `submitted_by`, đường dẫn ảnh gốc tạm thời và remote path trên Google Drive. Backend cung cấp `/api/storage/file` để đọc ảnh từ Drive mà không đưa `rclone.conf` hoặc token ra frontend.
