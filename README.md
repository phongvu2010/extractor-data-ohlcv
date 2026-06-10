# Vietnam Stock OHLCV Data Extractor Pipeline

Hệ thống ETL tối ưu hóa thu thập dữ liệu giao dịch chứng khoán Việt Nam (OHLCV - Open, High, Low, Close, Volume) tự động lưu trữ lên Google Cloud Storage (GCS) và Google Cloud BigQuery.

Hệ thống bao gồm hai pipeline chính:
1.  **Pipeline CafeF (Một lần duy nhất)**: Tải và nạp toàn bộ dữ liệu lịch sử của sàn chứng khoán Việt Nam (HOSE, HNX, UPCOM) từ năm 2000 đến nay.
2.  **Pipeline Vnstock (Chạy hàng ngày - T0)**: Tự động chạy thông qua Cronjob, cập nhật dữ liệu ngày hiện hành, tự động chạy bù dữ liệu ngày thiếu (Backfill), quét sự kiện điều chỉnh giá (Cổ tức, phát hành thêm) để reload lịch sử giá điều chỉnh.

---

## 🚀 Tính năng nổi bật

*   **Idempotency (Tính khả trùng)**: Đảm bảo dữ liệu không bị nhân bản khi chạy lại nhiều lần nhờ cơ chế dọn dẹp phân vùng trước khi nạp.
*   **Tối ưu bộ nhớ RAM**: Xử lý dữ liệu lớn (hơn 3.2 triệu dòng lịch sử) bằng cơ chế đọc ghi theo phân đoạn (Chunking) và nén Parquet Snappy.
*   **Điều phối API thông minh**: Tự động giới hạn tần suất yêu cầu API để phòng tránh khóa IP/tài khoản nhà cung cấp dữ liệu.
*   **Phát hiện sự kiện doanh nghiệp tự động**: So sánh giá tham chiếu để phát hiện sự kiện ex-rights (cổ tức, chia tách) và tự động đồng bộ lại toàn bộ lịch sử giá điều chỉnh của cổ phiếu liên quan.
*   **Cấu trúc bảng BigQuery tối ưu**: Bảng dữ liệu được cấu hình Phân vùng (`Partition`) theo tháng cho cột `trading_date` và Phân cụm (`Cluster`) theo cột `symbol, exchange`.
*   **Cảnh báo Telegram**: Báo cáo tổng hợp tiến độ chạy hoặc gửi cảnh báo lỗi khẩn cấp trực quan qua Telegram Bot API.

---

## 🛠️ Yêu cầu Hệ thống

*   Python 3.12 trở lên
*   Tài khoản **Google Cloud Platform (GCP)** đã bật dịch vụ Storage và BigQuery.
*   Quyền truy cập mạng (Internet) ổn định để tải dữ liệu.

---

## 📂 Cấu trúc Dự án

```text
├── extractors/
│   ├── __init__.py
│   ├── extractor_cafef.py      # Bộ trích xuất lịch sử CafeF
│   └── extractor_vnstock.py    # Bộ cập nhật hàng ngày & bù ngày thiếu Vnstock
├── secrets/
│   └── credentials.json        # Google Service Account Credentials (chỉ dùng nội bộ/local)
├── scratch/                    # Thư mục chứa các tệp nháp thử nghiệm
├── .env                        # Chứa cấu hình môi trường thực tế (không đẩy lên git)
├── .env.example                # File mẫu cấu hình môi trường
├── blacklist.txt               # Danh sách các mã rác/mã ảo cần bỏ qua
├── config.py                   # Quản lý tập trung các cấu hình và hằng số
├── Dockerfile                  # Cấu hình đóng gói Container Docker
├── docker-compose.yml          # Quản lý chạy container
├── main.py                     # Entrypoint chạy daily pipeline
├── main_cafef.ipynb            # Jupyter Notebook chạy khởi tạo lịch sử
├── notifier.py                 # Module cảnh báo qua Telegram Bot
├── requirements.txt            # Danh sách thư viện Python cần thiết
├── storages.py                 # Module tương tác chính với GCP (GCS & BigQuery)
└── utils.py                    # Tiện ích chung & bộ giới hạn tốc độ API
```

---

## ⚙️ Hướng dẫn Cấu hình

### Bước 1: Google Cloud Credentials
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
*   `GCS_BUCKET_NAME`: Tên bucket lưu trữ trên GCS (Ví dụ: `vn-stock`).
*   `BQ_DATASET`: Tên dataset trên BigQuery (Ví dụ: `vn_stock_dataset`).
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

# Cập nhật pip và cài đặt dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Bước khởi tạo dữ liệu lịch sử (Chạy một lần duy nhất)
Mở file [main_cafef.ipynb](file:///Users/hunterdo/Documents/Python Project/Extractor-Data-OHCLV/main_cafef.ipynb) bằng Jupyter Notebook hoặc VS Code, chạy tuần tự các cell để:
*   Tải toàn bộ dữ liệu lịch sử giá thô và giá điều chỉnh của HOSE, HNX, UPCOM từ máy chủ CafeF.
*   Ghi các tệp nén Parquet lên GCS.
*   Khởi tạo cấu trúc bảng trên BigQuery với thiết lập Partition/Cluster và nạp toàn bộ lịch sử vào bảng.

### 3. Chạy kiểm thử pipeline hàng ngày
```bash
python main.py
```
*Lưu ý: Mặc định, chương trình sẽ tự động bỏ qua nếu hôm nay là cuối tuần hoặc ngày lễ nghỉ giao dịch của Việt Nam. Để ép buộc chạy thử nghiệm, bạn có thể chỉnh cấu hình trong file `.env` thành `FORCE_RUN_WEEKEND=true`.*

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

## ⏰ Thiết lập Chạy Tự động Hàng ngày (Cronjob)

Để thiết lập hệ thống tự động cập nhật dữ liệu sau khi kết thúc phiên giao dịch hàng ngày (sau 15:30 chiều), bạn có thể cấu hình cronjob trên Linux/macOS.

Mở công cụ chỉnh sửa cronjob:
```bash
crontab -e
```
Thêm dòng lệnh sau để chạy tự động lúc **15:30 chiều** từ Thứ 2 đến Thứ 6 hằng tuần:
```text
30 15 * * 1-5 cd "/path/to/your/Extractor-Data-OHCLV" && .venv/bin/python main.py >> cron.log 2>&1
```
*(Hãy thay thế `/path/to/your/Extractor-Data-OHCLV` bằng đường dẫn tuyệt đối đến thư mục chứa dự án trên máy của bạn).*

---

## ☁️ Triển khai Serverless trên Google Cloud Run (Khuyên dùng trên Production)

Để vận hành trên môi trường GCP thực tế, phương án tối ưu và tiết kiệm chi phí nhất là triển khai dưới dạng **Cloud Run Job** và kích hoạt tự động bằng **Cloud Scheduler**. Do dự án chỉ chạy định kỳ rồi thoát, Cloud Run Job sẽ giúp bạn không phải trả chi phí duy trì server 24/7 (chỉ tính tiền theo giây thực tế khi script chạy).

### Bước 1: Tạo kho lưu trữ ảnh Docker (Artifact Registry)
Tạo kho lưu trữ Docker trên Google Cloud (nếu chưa có):
```bash
gcloud artifacts repositories create vn-stock-repo \
  --repository-format=docker \
  --location=asia-southeast1 \
  --description="Kho lưu trữ ảnh Docker của dự án VN Stock ETL"
```

### Bước 2: Build và Đẩy Docker Image lên Cloud
Sử dụng Cloud Build để biên dịch và đẩy ảnh lên Artifact Registry trực tiếp từ thư mục mã nguồn (không cần cài đặt Docker ở máy cục bộ):
```bash
gcloud builds submit --tag asia-southeast1-docker.pkg.dev/<YOUR_PROJECT_ID>/vn-stock-repo/vn-stock-etl:latest
```
*(Thay thế `<YOUR_PROJECT_ID>` bằng ID dự án GCP thực tế của bạn).*

### Bước 3: Tạo Cloud Run Job
Tạo một Job trên Cloud Run dựa vào Docker Image vừa đẩy lên:
```bash
gcloud run jobs create vn-stock-daily-job \
  --image asia-southeast1-docker.pkg.dev/<YOUR_PROJECT_ID>/vn-stock-repo/vn-stock-etl:latest \
  --region asia-southeast1 \
  --service-account=<YOUR_SERVICE_ACCOUNT_EMAIL> \
  --set-env-vars="GCS_BUCKET_NAME=vn-stock,BQ_DATASET=vn_stock_dataset,TELEGRAM_BOT_TOKEN=<YOUR_TOKEN>,TELEGRAM_CHAT_ID=<YOUR_CHAT_ID>"
```
> [!TIP]
> Hãy cấp quyền cho Service Account chạy Job này các vai trò cần thiết tương ứng với GCS và BigQuery (như `Storage Admin`, `BigQuery Admin` hoặc các vai trò chi tiết như ở Bước 1 của mục cấu hình).

### Bước 4: Chạy thử nghiệm Job thủ công
Kiểm tra xem Job có hoạt động thành công hay không bằng cách kích hoạt chạy ngay lập tức:
```bash
gcloud run jobs execute vn-stock-daily-job --region asia-southeast1
```

### Bước 5: Lập lịch chạy hàng ngày bằng Cloud Scheduler
Tạo một cronjob trên Cloud Scheduler để tự động gọi chạy Cloud Run Job vào lúc **15:30 chiều** từ thứ Hai đến thứ Sáu (múi giờ Việt Nam):
```bash
gcloud scheduler jobs create http vn-stock-daily-scheduler \
  --schedule="30 15 * * 1-5" \
  --time-zone="Asia/Ho_Chi_Minh" \
  --uri="https://asia-southeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/<YOUR_PROJECT_ID>/jobs/vn-stock-daily-job:run" \
  --http-method=POST \
  --oauth-service-account-email=<YOUR_SERVICE_ACCOUNT_EMAIL>
```

---

## 💡 Thiết lập Tự động Dọn dẹp Bảng tạm (Default Table Expiration)

Trong quá trình reload lịch sử giá điều chỉnh, hệ thống sẽ tạo các bảng tạm staging dạng `adjusted_price_staging_<SYMBOL>`. Để đảm bảo các bảng tạm này luôn được dọn dẹp sạch sẽ (tránh tích tụ làm rác BigQuery dataset nếu có lỗi kết nối mạng xảy ra), bạn nên cấu hình thuộc tính **Default Table Expiration** (Thời hạn tự động hủy bảng) của dataset `vn_stock_dataset` thành **1 ngày (24 giờ)**. 

Có 3 cách để thiết lập chốt chặn an toàn này:

### Cách 1: Thiết lập qua Giao diện Web Google Cloud Console
1. Truy cập vào **GCP Console** -> **BigQuery**.
2. Chọn dự án của bạn và tìm tới dataset `vn_stock_dataset`.
3. Nhấp chọn biểu tượng ba chấm cạnh tên dataset, chọn **Edit** (hoặc **Edit details**).
4. Tìm trường cấu hình **Default table expiration** (Thời hạn bảng mặc định).
5. Tích chọn kích hoạt và điền thời gian mong muốn (ví dụ: nhập `24 hours` hoặc chọn `1 day`).
6. Nhấn **Save** để áp dụng.

### Cách 2: Thiết lập bằng câu lệnh SQL DDL (Chạy trực tiếp trên BigQuery Console)
Chạy câu truy vấn sau trong khung soạn thảo SQL của BigQuery:
```sql
ALTER SCHEMA `vn_stock_dataset` SET OPTIONS(default_table_expiration_days=1);
```
*(Thay `vn_stock_dataset` bằng tên dataset của bạn nếu cấu hình tên khác ở file `.env`)*.

### Cách 3: Thiết lập bằng gcloud CLI (Chạy từ Terminal)
Nếu bạn sử dụng Cloud SDK, hãy chạy lệnh sau để cập nhật trực tiếp:
```bash
bq update --default_table_expiration 86400 vn_stock_dataset
```
*(Tham số `86400` tương ứng với số giây trong 1 ngày).*
