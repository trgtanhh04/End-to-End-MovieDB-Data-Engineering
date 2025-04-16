# Pipeline Dữ liệu cho Hệ thống Gợi ý Phim

## Tổng quan
- Từ khi xuất hiện đến nay, phim được xem như là môn nghệ thuật thứ 7 và được rất nhiều người ưa thích. Trong suốt quá trình phát triển của ngành công nghiệp phim ảnh, nó đã trải qua rất nhiều thăng trầm và có rất nhiều bộ phim chất lượng được sản xuất. Tuy nhiên, thị trường phim hiện nay đang xuất hiện các tranh cãi về đề tài phim kém chất lượng. Vì vậy, tôi đã quyết định chọn đề tài phim điện ảnh để thực hiện đồ án.

- Dự án tập trung xây dựng một hệ thống kỹ thuật dữ liệu hiện đại, hỗ trợ thu thập, lưu trữ, xử lý và phân tích dữ liệu phim qua các năm, nhằm cung cấp giải pháp tư vấn phim cá nhân hóa theo sở thích người dùng.

- Pipeline dữ liệu được tự động hóa toàn diện, ứng dụng các công nghệ Big Data tiên tiến như Apache Hadoop, Spark, Kafka và cơ sở dữ liệu phân tán, đảm bảo khả năng xử lý dữ liệu lớn hiệu quả, linh hoạt và theo thời gian thực.

---

## Cấu trúc thư mục
---
## Kiến trúc pipeline
<p align="center">
  <img src="https://github.com/trgtanhh04/End-to-End-MovieDB-Data-Engineering/blob/main/imge/Data%20engineering%20architecture.png" width="100%" alt="Mô hình MVC">
</p>
---
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
---
## Quy Trình Xử Lý Dữ Liệu

1. Thu Thập Dữ Liệu

Hệ thống crawler được lên lịch chạy định kỳ để thu thập dữ liệu phim từ nhiều nguồn website khác nhau. Quá trình này đảm bảo cập nhật đầy đủ thông tin phim mới như: tên phim, thể loại, quốc gia, thời lượng, điểm đánh giá,...

2. Lưu Trữ Dữ Liệu Thô

Dữ liệu sau khi crawl được lưu dưới dạng tệp JSON trong hệ thống tệp cục bộ. Đây là nguồn dữ liệu thô ban đầu phục vụ cho các bước xử lý tiếp theo.

3. Nạp Dữ Liệu Vào Data Lake (HDFS)

Các tệp JSON sẽ được chuyển vào hệ thống Data Lake dựa trên nền tảng HDFS. Điều này cho phép lưu trữ dữ liệu khối lượng lớn, hỗ trợ khả năng truy xuất và xử lý phân tán hiệu quả.

4. ETL Cơ Bản (Kafka Triggered)

Sau khi lưu trữ vào HDFS, hệ thống sử dụng Kafka để kích hoạt chuỗi xử lý ETL. Bao gồm:

- **Extract:** Đọc dữ liệu từ HDFS.
- **Transform:** Làm sạch, chuẩn hóa, xử lý định dạng dữ liệu (chuyển đổi kiểu dữ liệu, tách thể loại, chuẩn hóa thời gian...).
- **Load:** Lưu lại dữ liệu đã xử lý vào một thư mục HDFS mới (phục vụ bước xử lý nâng cao sau này).

Kafka đảm nhiệm vai trò điều phối, truyền tin, đảm bảo các bước ETL được tự động kích hoạt khi có dữ liệu mới.


5. Phân Vùng Dữ Liệu

Dữ liệu trong HDFS được phân vùng theo ngày crawl hoặc theo thể loại phim nhằm tối ưu cho các truy vấn phân tích và tìm kiếm về sau.

6. Xử Lý Nâng Cao (Apache Spark)

Apache Spark được tích hợp để xử lý nâng cao dữ liệu, ví dụ:

- Lọc và phân loại phim theo điểm đánh giá
- Phân tích xu hướng thể loại phổ biến
- Chuẩn hóa dữ liệu từ nhiều nguồn
- Tạo các bảng tổng hợp phục vụ phân tích

### 7. Tải Vào PostgreSQL

Dữ liệu đã xử lý sẽ được nạp vào hệ quản trị cơ sở dữ liệu PostgreSQL, phục vụ cho:

- Các truy vấn nhanh, chính xác
- Trích xuất dữ liệu phục vụ frontend hoặc API

8. Đồng Bộ Lên PostgreSQL Cloud (Neon)

Dữ liệu sau khi lưu vào PostgreSQL cục bộ sẽ được đẩy lên nền tảng PostgreSQL cloud Neon để:

- Dễ dàng triển khai ứng dụng từ xa
- Chia sẻ dữ liệu với frontend hoặc các team khác
- Triển khai phân tích real-time trên cloud

#### Ứng Dụng Thực Tế

- **API:** Xây dựng API cho hệ thống quản lý phim, cho phép người dùng truy vấn thông tin phim, lọc theo thể loại, điểm IMDb,...
- **Dashboard phân tích:** Triển khai bảng điều khiển giúp quản trị viên nắm được xu hướng phim, lượt đánh giá cao/thấp,...
- **Tích hợp gợi ý phim:** Dựa trên lịch sử hoặc xu hướng phổ biến từ phân tích Spark.


#### Phân Phối Quy Trình Làm Việc

Toàn bộ pipeline từ crawl → HDFS → Kafka ETL → Spark → PostgreSQL được điều phối và tự động hóa thông qua **Apache Airflow**, đảm bảo:

- Quản lý lịch trình chạy task dễ dàng
- Xử lý lỗi và retry linh hoạt
- Theo dõi trực quan luồng dữ liệu


