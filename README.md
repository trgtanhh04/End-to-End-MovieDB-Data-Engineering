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
- `craw_data_task`: Trích xuất dữ liệu từ nguồn bên ngoài như API, website, file CSV,...

- `convert_to_json_task`: Chuyển đổi dữ liệu đã thu thập sang định dạng JSON để chuẩn hóa và dễ xử lý.

- `load_to_datalake_task`: Tải dữ liệu JSON vào hệ thống lưu trữ tạm thời (Data Lake), ví dụ: Hadoop HDFS, Amazon S3,...

- `trigger_kafka_etl_task`: Gửi tín hiệu kích hoạt quá trình ETL trên hệ thống Kafka (hoặc công cụ stream khác như Spark, Flink).

- `send_email_task` Gửi email thông báo kết quả chạy pipeline (thành công/thất bại), giúp theo dõi và giám sát.
