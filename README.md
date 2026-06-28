# Vietnam Stock OHLCV Data Extractor Pipeline

Hệ thống ETL tối ưu hóa thu thập dữ liệu giao dịch chứng khoán Việt Nam (Giá Thô & Giá Điều chỉnh - OHLCV) tự động chạy định kỳ, tự động kiểm định đơn vị giá và xuất Feature Store phục vụ mô hình Machine Learning. Hệ thống hỗ trợ lưu trữ đa môi trường: Local (PostgreSQL & TimescaleDB) hoặc Cloud (Google Cloud Storage & BigQuery).

Hệ thống bao gồm hai luồng pipeline chính:
1.  **Pipeline CafeF (Khởi tạo lịch sử)**: Tải và nạp toàn bộ dữ liệu lịch sử thô và điều chỉnh của toàn bộ thị trường (HOSE, HNX, UPCOM) từ năm 2000 đến nay.
2.  **Pipeline Vnstock (Chạy hàng ngày - T0)**: Tự động chạy lúc 20:00 tối các ngày từ Thứ 2 đến Thứ 6 để cập nhật phiên giao dịch mới nhất, tự động chạy bù dữ liệu ngày thiếu (Backfill), quét sự kiện doanh nghiệp (cổ tức, phát hành thêm) để reload dữ liệu lịch sử giá điều chỉnh.

---

## 🚀 Tính năng nổi bật nâng cao

*   **Idempotency (Tính khả trùng)**: Đảm bảo dữ liệu không bị nhân bản khi chạy lại nhờ cơ chế xóa/dọn dẹp phân vùng BigQuery (Staging Mode) hoặc PostgreSQL partition trước khi nạp.
*   **Tối ưu bộ nhớ RAM**: Xử lý dữ liệu lớn (hơn 3.2 triệu dòng lịch sử) bằng cơ chế đọc ghi theo phân đoạn (Chunking) và nén Parquet Snappy.
*   **Điều phối API thông minh**: Tích hợp `SmartRateLimiter` tự động kiểm soát tần suất request và micro-sleep thông minh để phòng tránh khóa IP/tài khoản từ nhà cung cấp API.
*   **Khôi phục danh sách mã tự động (Fallback Cache)**: Nếu API lấy danh sách mã của Vnstock gặp sự cố, hệ thống tự động tải lại danh sách gần nhất được lưu trong Checkpoint Snapshot (`active_symbols_cache`) để đảm bảo pipeline chính tiếp tục vận hành.
*   **Quét sự kiện doanh nghiệp an toàn (Soft Fault-Tolerance)**: Tự động quét sự kiện chia cổ tức/cổ phiếu thưởng T0 qua API Vietcap. Khối quét được bọc an toàn; nếu API Vietcap sập, hệ thống gửi cảnh báo qua Telegram nhưng vẫn tiếp tục cập nhật bảng giá T0 bình thường thay vì crash đột ngột.
*   **Đồng bộ đơn vị giá tự động (Price Unit Check)**: Tự động đối chiếu đơn vị giá bảng điện tử T0 và dữ liệu lịch sử OHLCV qua cổ phiếu benchmark (mặc định: FPT). Hệ thống tự động nhân hệ số 1000.0 nếu phát hiện lệch đơn vị (giá bảng điện tử tính theo nghìn đồng, OHLCV tính theo đồng).
*   **Cảnh báo Telegram & Đồng bộ Logger**:
    *   Hệ thống logger được chuẩn hóa cấu trúc tiền tố rõ ràng như `[Vnstock]`, `[Events]`, `[Backfill]`, `[Unit Check]`, `[GCS]`, `[BigQuery]`, `📲 [Telegram]`.
    *   Báo cáo tổng hợp tiến độ chạy daily hoặc gửi cảnh báo lỗi khẩn cấp trực quan qua Telegram Bot API.

---

## 🛠️ Yêu cầu Hệ thống

*   Python 3.12 trở lên.
*   Tài khoản **Google Cloud Platform (GCP)** (nếu chạy Cloud Mode) đã kích hoạt Cloud Storage và BigQuery.
*   Cơ sở dữ liệu **PostgreSQL + TimescaleDB** (nếu chạy Local Mode).

---

## 📂 Cấu trúc Dự án

```text
├── configs/
│   └── blacklist.txt           # Danh sách các mã rác/mã ảo cần bỏ qua
├── extractors/
│   ├── __init__.py
│   ├── extractor_cafef.py      # Bộ trích xuất lịch sử CafeF
│   └── extractor_vnstock.py    # Bộ cập nhật hàng ngày & bù ngày thiếu Vnstock
├── storages/
│   ├── __init__.py             # Factory Method khởi tạo bộ lưu trữ
│   ├── base.py                 # Định nghĩa lớp cơ sở trừu tượng
│   ├── cloud.py                # Bộ lưu trữ lên GCS & BigQuery
│   └── local.py                # Bộ lưu trữ lên PostgreSQL & TimescaleDB
├── .env.example                # File mẫu cấu hình môi trường
├── config.py                   # Quản lý tập trung các cấu hình và hằng số
├── Dockerfile                  # Cấu hình đóng gói Container Docker
├── main.py                     # Entrypoint chạy daily pipeline
├── main_cafef.ipynb            # Jupyter Notebook chạy khởi tạo lịch sử
├── notifier.py                 # Module cảnh báo qua Telegram Bot
├── pyproject.toml              # Khai báo dependencies và metadata dự án (thay cho requirements.txt)
└── utils.py                    # Tiện ích chung & bộ giới hạn tốc độ API
```

---

## ⚙️ Hướng dẫn Cấu hình

### Bước 1: Google Cloud Credentials (Nếu dùng Cloud Mode)
1.  Tạo một **Service Account** trên Google Cloud Console với quyền:
    *   `Storage Object Creator` và `Storage Object Viewer` (cho GCS Bucket).
    *   `BigQuery Data Editor` và `BigQuery Job User` (cho BigQuery).
2.  Tải khóa tài khoản dịch vụ dưới dạng tệp **JSON** và lưu vào thư mục dự án tại đường dẫn: `secrets/credentials.json`.

### Bước 2: Tạo File cấu hình môi trường `.env`
Sao chép tệp cấu hình mẫu và chỉnh sửa các tham số phù hợp:
```bash
cp .env.example .env
```
Thiết lập các biến quan trọng trong tệp `.env`:
*   `DEPLOYMENT_ENV`: Thiết lập `cloud` (GCP) hoặc `local` (PostgreSQL).
*   `GCS_BUCKET_NAME`: Tên bucket lưu trữ trên GCS.
*   `BQ_DATASET`: Tên dataset trên BigQuery.
*   `DATABASE_URL`: URL kết nối PostgreSQL nếu chạy local (Ví dụ: `postgresql://postgres:postgres@localhost:5432/vn_stock`).
*   `TELEGRAM_BOT_TOKEN` & `TELEGRAM_CHAT_ID`: (Tùy chọn) Cấu hình bot để nhận thông báo hàng ngày.
*   `GOOGLE_APPLICATION_CREDENTIALS`: Chỉ định đường dẫn xác thực nếu chạy local (Ví dụ: `secrets/credentials.json`).

---

## 💻 Cài đặt & Chạy trên máy vật lý (Local)

### 1. Khởi tạo môi trường ảo và cài đặt thư viện
```bash
# Tạo môi trường ảo
python3 -m venv .venv

# Kích hoạt môi trường ảo (MacOS/Linux)
source .venv/bin/activate

# Kích hoạt môi trường ảo (Windows)
# .venv\Scripts\activate.bat

# Cài đặt dependencies (Cloud mode)
pip install -e .

# Hoặc cài đặt bao gồm cả dependencies cho Local mode (PostgreSQL)
pip install -e ".[local]"
```

### 2. Khởi tạo dữ liệu lịch sử (Chạy một lần duy nhất)
Mở file [main_cafef.ipynb](main_cafef.ipynb) bằng Jupyter Notebook hoặc VS Code, chạy tuần tự các cell để:
*   Tải toàn bộ dữ liệu lịch sử giá thô và giá điều chỉnh của HOSE, HNX, UPCOM từ CafeF.
*   Ghi các tệp nén Parquet lên GCS.
*   Khởi tạo cấu trúc bảng trên BigQuery với thiết lập Partition/Cluster và nạp toàn bộ lịch sử.

### 3. Chạy kiểm thử pipeline hàng ngày
```bash
python main.py
```
*Lưu ý: Mặc định, chương trình sẽ tự động bỏ qua nếu hôm nay là cuối tuần hoặc ngày lễ nghỉ giao dịch của Việt Nam. Để ép buộc chạy thử nghiệm, bạn có thể chỉnh cấu hình trong file `.env` thành `FORCE_RUN=true`.*

---

## 🐳 Cài đặt & Chạy bằng Docker

Dự án đã được container hóa hoàn chỉnh giúp chạy an toàn, cô lập và không phụ thuộc vào hệ điều hành của host.

### 1. Build Docker Image
```bash
docker-compose build
```

### 2. Thực thi Container
Chạy lệnh sau để kích hoạt chạy ETL daily một lần bằng Docker Compose:
```bash
docker-compose up
```
*Docker Compose đã được cấu hình tự động ánh xạ (mount) file xác thực GCP tại `secrets/credentials.json` và nạp các biến môi trường từ tệp `.env` vào bên trong container.*

---

## ⏰ Thiết lập Chạy Tự động Hàng ngày (Cronjob - Chạy Local)

Mở công cụ chỉnh sửa cronjob:
```bash
crontab -e
```
Thêm dòng lệnh sau để chạy tự động lúc **20:00 tối** từ Thứ 2 đến Thứ 6 hằng tuần:
```text
0 20 * * 1-5 cd "/path/to/your/extractor-data-ohlcv" && .venv/bin/python main.py >> cron.log 2>&1
```
*(Hãy thay thế `/path/to/your/Extractor-Data-OHCLV` bằng đường dẫn tuyệt đối đến thư mục chứa dự án trên máy của bạn).*

---

## ☁️ Triển khai Serverless trên Google Cloud Run (Khuyên dùng trên Production)

Để vận hành trên môi trường GCP thực tế, phương án tối ưu và tiết kiệm chi phí nhất là triển khai dưới dạng **Cloud Run Job** và kích hoạt tự động bằng **Cloud Scheduler**. Do dự án chỉ chạy định kỳ rồi thoát, Cloud Run Job sẽ giúp bạn không phải trả chi phí duy trì server 24/7.

### Bước 1: Tạo kho lưu trữ ảnh Docker (Artifact Registry)
```bash
gcloud artifacts repositories create vn-stock-repo \
  --repository-format=docker \
  --location=asia-southeast1 \
  --description="Kho lưu trữ ảnh Docker của dự án VN Stock ETL"
```

### Bước 2: Build và Đẩy Docker Image lên Cloud
Sử dụng Cloud Build để biên dịch và đẩy ảnh lên Artifact Registry trực tiếp từ thư mục mã nguồn:
```bash
gcloud builds submit --tag asia-southeast1-docker.pkg.dev/<YOUR_PROJECT_ID>/vn-stock-repo/vn-stock-etl:latest
```
*(Thay thế `<YOUR_PROJECT_ID>` bằng ID dự án GCP thực tế của bạn).*

### Bước 3: Tạo Cloud Run Job
1. **Lưu trữ Secrets trên Google Cloud Secret Manager** để bảo mật:
   Tạo hai secret trên Secret Manager để lưu thông tin Telegram:
   ```bash
   echo -n "<YOUR_TELEGRAM_BOT_TOKEN>" | gcloud secrets create TELEGRAM_BOT_TOKEN --data-file=-
   echo -n "<YOUR_TELEGRAM_CHAT_ID>" | gcloud secrets create TELEGRAM_CHAT_ID --data-file=-
   ```
   *Lưu ý: Hãy cấp quyền `Secret Manager Secret Accessor` cho Service Account chạy Job để nó có thể giải mã bí mật khi container khởi chạy.*

2. **Khởi tạo Cloud Run Job**:
   ```bash
   gcloud run jobs create vn-stock-daily-job \
     --image asia-southeast1-docker.pkg.dev/<YOUR_PROJECT_ID>/vn-stock-repo/vn-stock-etl:latest \
     --region asia-southeast1 \
     --service-account=<YOUR_SERVICE_ACCOUNT_EMAIL> \
     --cpu=1 \
     --memory=2Gi \
     --timeout=30m \
     --set-env-vars="GCS_BUCKET_NAME=vn-stock,BQ_DATASET=vn_stock_dataset" \
     --set-secrets="TELEGRAM_BOT_TOKEN=TELEGRAM_BOT_TOKEN:latest,TELEGRAM_CHAT_ID=TELEGRAM_CHAT_ID:latest"
   ```

### Bước 4: Lập lịch chạy hàng ngày bằng Cloud Scheduler
Tạo một cronjob trên Cloud Scheduler để tự động gọi chạy Cloud Run Job vào lúc **20:00 tối** từ thứ Hai đến thứ Sáu (múi giờ Việt Nam):
```bash
gcloud scheduler jobs create http vn-stock-daily-scheduler \
  --schedule="0 20 * * 1-5" \
  --time-zone="Asia/Ho_Chi_Minh" \
  --uri="https://asia-southeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/<YOUR_PROJECT_ID>/jobs/vn-stock-daily-job:run" \
  --http-method=POST \
  --oauth-service-account-email=<YOUR_SERVICE_ACCOUNT_EMAIL>
```

### ⚠️ Cấu hình Vận hành Khuyên dùng (Best Practices)

Để hệ thống hoạt động ổn định và tối ưu chi phí trên GCP, hãy đảm bảo cấu hình các thông số sau cho Cloud Run Job:

*   **Timeout (Thời gian chờ)**: Mặc định Cloud Run Job có timeout là 10 phút. Bạn cần tăng cấu hình timeout của Cloud Run Job lên tối thiểu **30 phút** hoặc **60 phút** để đảm bảo job không bị kill giữa chừng khi phải reload toàn bộ lịch sử giá điều chỉnh của nhiều mã cổ phiếu (phát sinh trong các ngày có sự kiện chia tách/cổ tức).
*   **Memory/CPU (Tài nguyên máy chủ)**: Khuyến nghị cấp phát tối thiểu **2 GiB Memory** và **1 vCPU** cho Cloud Run Job. Cấu hình này giúp công cụ Polars hoạt động tối đa hiệu năng, tận dụng cơ chế đa luồng khi xử lý và gộp dữ liệu lớn.
*   **Kiểm soát Chi phí BigQuery (Cost Control)**:
    *   Bảng `raw_price` và `adj_price` đã được cấu hình phân vùng theo tháng (`MONTH` partition) trên trường `trading_date`.
    *   Khi viết các truy vấn phân tích hoặc sử dụng dữ liệu sau này, **luôn luôn** bổ sung điều kiện lọc theo `trading_date` (ví dụ: `WHERE trading_date >= '2026-01-01'`) để BigQuery chỉ quét các phân vùng cần thiết (Partition Pruning), giúp giảm thiểu tối đa dung lượng quét và tiết kiệm chi phí.
*   **Chạy bù Thủ công Cuối tuần (Manual Rerun)**: Hệ thống được lập lịch chạy tự động từ thứ Hai đến thứ Sáu và tự động bỏ qua các ngày cuối tuần hoặc ngày lễ. Nếu xảy ra sự cố vào thứ Sáu và bạn muốn trigger chạy lại thủ công (manual execution) vào thứ Bảy hoặc Chủ Nhật, bạn **bắt buộc** phải thiết lập biến môi trường **`FORCE_RUN=true`** trong cấu hình của Cloud Run Job trước khi chạy, nếu không tiến trình sẽ tự động thoát lập tức.
