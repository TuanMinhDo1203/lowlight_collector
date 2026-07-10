# Hướng Dẫn Sử Dụng Web Platform

## Chạy App

```bash
source .venv/bin/activate
uvicorn web_platform.app:app --host 0.0.0.0 --port 8000
```

Mở trên máy hiện tại:

```text
http://localhost:8000
```

Nếu các member dùng chung Wi-Fi/LAN, họ có thể mở:

```text
http://YOUR_LOCAL_IP:8000
```

## Quy Trình Cơ Bản

1. Tải video lên.
2. Giữ tham số mặc định và bấm **Xử lý video** để test trước.
3. Review các cặp LOW/HIGH hệ thống tự chọn.
4. Nếu cặp chưa ổn:
   - đổi **Frame LOW**
   - đổi **Frame HIGH**
   - hoặc bấm **Loại cặp**
5. Nếu pipeline bỏ sót cặp tốt, bấm **Thêm cặp thủ công** rồi tự chọn LOW/HIGH.
6. Tick các cặp muốn giữ.
7. Bấm **Lưu cặp đã duyệt**.

Output sẽ nằm ở:

```text
drive:LLIE_Dataset/raw/low
drive:LLIE_Dataset/raw/reference
```

Metadata và remote path được lưu trong database.

## Ý Nghĩa Tham Số

### Frame step

Số frame bị bỏ qua khi trích frame từ video.

```text
Giảm xuống = lấy nhiều frame hơn, dễ bắt đúng khoảnh khắc mở cửa/chuyển cảnh hơn, nhưng chạy chậm hơn.
Tăng lên = chạy nhanh hơn, ít frame hơn, nhưng dễ bỏ sót khoảnh khắc quan trọng.
```

Gợi ý:

```text
3-5 cho video ngắn
5-10 cho video dài
```

### Low max

Độ sáng tối đa để một frame được xem là LOW, tức frame thiếu sáng.

```text
Giảm xuống = chỉ lấy frame rất tối.
Tăng lên = lấy thêm các frame hơi tối hoặc trung bình tối.
```

Gợi ý:

```text
50-70
```

### High min

Độ sáng tối thiểu để một frame được xem là HIGH, tức frame đủ sáng.

```text
Giảm xuống = có nhiều ứng viên HIGH hơn, gồm cả frame hơi tối.
Tăng lên = chỉ lấy frame sáng rõ hơn.
```

Gợi ý:

```text
95-120
```

### Min gap

Độ chênh sáng tối thiểu giữa HIGH và LOW.

```text
Tăng lên = cặp low/high tương phản rõ hơn.
Giảm xuống = ra nhiều cặp hơn, nhưng có thể có cặp chênh sáng yếu.
```

Gợi ý:

```text
35-60
```

### High before / High after

Số frame đủ sáng cần tìm trước và sau một đoạn tối.

```text
Tăng lên = có nhiều lựa chọn HIGH hơn, dễ tìm lại cảnh đúng nếu nằm xa đoạn tối.
Giảm xuống = ít bị bắt nhầm cảnh xa, nhưng có thể bỏ sót HIGH đúng.
```

Gợi ý:

```text
6-12
```

### Alternatives

Số lựa chọn HIGH được ưu tiên hiển thị theo điểm matching.

```text
Tăng lên = user có nhiều lựa chọn thủ công hơn.
Giảm xuống = UI gọn hơn.
```

Gợi ý:

```text
6-12
```

### Edge diff weight

Mức phạt khi cấu trúc cạnh/object giữa LOW và HIGH khác nhau.

```text
Tăng lên = tránh bắt nhầm cảnh/object tốt hơn.
Tăng quá cao = có thể loại nhầm cặp đúng nhưng LOW quá tối.
```

Gợi ý:

```text
6-10
```

### HOG penalty

Mức phạt nếu OpenCV HOG phát hiện người trong frame HIGH.

```text
Tăng lên = hạn chế chọn frame có người.
Tăng quá cao = có thể bỏ qua frame đúng nếu detector báo nhầm.
```

Gợi ý:

```text
20-35
```

### Low dedup diff / Low dedup bright

Điều khiển việc loại các LOW frame gần trùng nhau trong output cuối.

```text
Tăng lên = loại trùng lặp mạnh hơn.
Giảm xuống = giữ lại nhiều biến thể LOW hơn.
```

Gợi ý:

```text
Low dedup diff: 8-15
Low dedup bright: 6-12
```

### Top per low

Số HIGH match được tự động giữ cho mỗi LOW.

```text
Để 1 khi tạo dataset chính.
Tăng lên 2+ khi muốn debug hoặc khảo sát nhiều lựa chọn.
```

## Gợi Ý Chỉnh Khi Có Vấn Đề

Nếu bị mất cặp tốt:

```text
Giảm Frame step
Tăng High before / High after
Giảm High min
Giảm Min gap
Tăng Alternatives
```

Nếu có quá nhiều cặp sai:

```text
Tăng Min gap
Tăng Edge diff weight
Tăng HOG penalty
Giảm High before / High after
```

Nếu bị trùng lặp nhiều:

```text
Tăng Low dedup diff
Tăng Low dedup bright
Giữ Top per low = 1
```

Nếu frame rất tối bị mất:

```text
Giữ LOW_BRIGHTNESS_MIN = 0.0 trong pipeline.
Không hard reject person detection nếu chưa kiểm tra thủ công.
```
