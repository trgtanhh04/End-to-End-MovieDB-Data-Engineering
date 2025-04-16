# Pipeline Dữ liệu cho Hệ thống Gợi ý Phim

## Tổng quan
- Từ khi xuất hiện đến nay, phim được xem như là môn nghệ thuật thứ 7 và được rất nhiều người ưa thích. Trong suốt quá trình phát triển của ngành công nghiệp phim ảnh, nó đã trải qua rất nhiều thăng trầm và có rất nhiều bộ phim chất lượng được sản xuất. Tuy nhiên, thị trường phim hiện nay đang xuất hiện các tranh cãi về đề tài phim kém chất lượng. Vì vậy, tôi đã quyết định chọn đề tài phim điện ảnh để thực hiện đồ án.

- Dự án tập trung xây dựng một hệ thống kỹ thuật dữ liệu hiện đại, hỗ trợ thu thập, lưu trữ, xử lý và phân tích dữ liệu phim qua các năm, nhằm cung cấp giải pháp tư vấn phim cá nhân hóa theo sở thích người dùng.

- Pipeline dữ liệu được tự động hóa toàn diện, ứng dụng các công nghệ Big Data tiên tiến như Apache Hadoop, Spark, Kafka và cơ sở dữ liệu phân tán, đảm bảo khả năng xử lý dữ liệu lớn hiệu quả, linh hoạt và theo thời gian thực.
## Cấu trúc thư mục

## Kiến trúc pipeline
<p align="center">
  <img src="https://github.com/trgtanhh04/End-to-End-MovieDB-Data-Engineering/blob/main/imge/Data%20engineering%20architecture.png" width="100%" alt="Mô hình MVC">
</p>

## Task Dependencies
Sơ đồ này minh họa rõ ràng hành trình của dữ liệu từ thu thập đến xử lý và phân phối kết quả trong hệ thống.
<p align="center">
  <img src=https://github.com/trgtanhh04/End-to-End-MovieDB-Data-Engineering/blob/main/imge/Pipeline_Airflow.png width="100%" alt="Mô hình MVC">
</p>

Mô tả các tác vụ:
- **craw_data_task**: Trích xuất dữ liệu từ nguồn bên ngoài như API, website, file CSV,...

- **convert_to_json_task**: Chuyển đổi dữ liệu đã thu thập sang định dạng JSON để chuẩn hóa và dễ xử lý.

- **load_to_datalake_task**: Tải dữ liệu JSON vào hệ thống lưu trữ tạm thời (Data Lake), ví dụ: Hadoop HDFS, Amazon S3,...

- **trigger_kafka_etl_task**: Gửi tín hiệu kích hoạt quá trình ETL trên hệ thống Kafka (hoặc công cụ stream khác như Spark, Flink).

- **send_email_task**: Gửi email thông báo kết quả chạy pipeline (thành công/thất bại), giúp theo dõi và giám sát.

## 🔄 Quy Trình Xử Lý Dữ Liệu (ETL Pipeline)

Dự án thực hiện toàn bộ quy trình xử lý dữ liệu phim theo mô hình ETL (Extract - Transform - Load), bao gồm các bước:

---

### 🧪 1. Extract – Trích Xuất Dữ Liệu

- **Mục tiêu:** Lấy dữ liệu phim từ một website công khai.
- **Công cụ:** `requests`, `BeautifulSoup` hoặc `Selenium` (nếu web động).
- **Nội dung thu thập:** Tên phim, thể loại, thời lượng, quốc gia, điểm IMDb, mô tả,...
- **Kết quả:** Dữ liệu thô (raw data) được lưu dưới dạng JSON hoặc CSV tại local.

---

### 🔄 2. Load vào Data Lake (HDFS)

- **Mục tiêu:** Lưu trữ dữ liệu thô vào hệ thống lưu trữ phân tán HDFS để dễ dàng chia sẻ và xử lý song song.
- **Công cụ:** `Hadoop HDFS` (có thể thông qua `PyArrow`, `hdfsCLI`, hoặc command line).
- **Cấu trúc lưu trữ:** Dữ liệu được lưu theo ngày hoặc theo nguồn crawl.

---

### ⚙️ 3. Kafka Stream (Real-time Trigger)

- **Mục tiêu:** Kích hoạt xử lý dữ liệu khi có dữ liệu mới trong HDFS.
- **Công cụ:** `Apache Kafka` (Producer-Consumer).
  - **Producer** gửi thông điệp chứa thông tin về file mới trong HDFS.
  - **Consumer** (Spark hoặc Flink) nhận thông tin và bắt đầu ETL.

---

### 🧹 4. Transform – Làm Sạch và Chuyển Đổi Dữ Liệu

- **Mục tiêu:** Chuẩn hóa dữ liệu, xử lý lỗi, chuyển đổi kiểu dữ liệu, loại bỏ trùng lặp,...
- **Công cụ:** `Apache Spark` hoặc `Pandas` (nếu dữ liệu nhỏ).
- **Ví dụ xử lý:**
  - Chuẩn hóa định dạng thời gian
  - Bóc tách thể loại thành list
  - Loại bỏ phim trùng lặp hoặc thiếu thông tin quan trọng

---

### 🛢 5. Load – Tải Dữ Liệu vào PostgreSQL

- **Mục tiêu:** Lưu trữ dữ liệu đã xử lý vào hệ quản trị cơ sở dữ liệu quan hệ để dễ dàng truy vấn và phân tích.
- **Công cụ:** `psycopg2`, `SQLAlchemy`, hoặc `Spark JDBC connector`.
- **Thiết kế CSDL:** Thiết kế bảng `movies` chứa các thông tin phim.

---

### ☁️ 6. Đồng Bộ lên PostgreSQL Cloud (Neon)

- **Mục tiêu:** Đẩy dữ liệu từ PostgreSQL local lên PostgreSQL cloud (Neon) để sử dụng ở môi trường production hoặc chia sẻ công khai.
- **Công cụ:** `pg_dump` + `pg_restore` hoặc đồng bộ qua `DBeaver`, `pgAdmin`, `Airbyte`, hoặc trực tiếp qua `psql`.
- **Lưu ý:** Cần thiết lập kết nối SSL nếu Neon yêu cầu.

---

## ✅ Tổng Quan Pipeline

```mermaid
graph TD
  A[Crawl dữ liệu phim từ web] --> B[Lưu vào Data Lake (HDFS)]
  B --> C[Kích hoạt Kafka]
  C --> D[Xử lý bằng Spark/Flink]
  D --> E[Lưu vào PostgreSQL local]
  E --> F[Đồng bộ lên PostgreSQL Cloud (Neon)]

