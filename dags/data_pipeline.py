from datetime import datetime, timedelta
from airflow import DAG
import time
from airflow.operators.python import PythonOperator
import json
import logging
import os
from dotenv import load_dotenv
import runpy
import subprocess

# Load biến môi trường
load_dotenv(dotenv_path='/home/tienanh/End-to-End Movie Recommendation/.env')

SENDER_EMAIL = os.getenv("SENDER_EMAIL") # your email
APP_PASSWORD = os.getenv("APP_PASSWORD") # your app password
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


# dag_folder = os.path.dirname(os.path.abspath(__file__))
# project_folder = os.path.dirname(dag_folder)
# scripts_folder = os.path.join(project_folder, "scripts")
dag_folder = os.path.dirname(os.path.abspath(__file__))
project_folder = os.path.abspath(os.path.join(dag_folder, "../../End-to-End Movie Recommendation"))
scripts_folder = os.path.join(project_folder, "scripts")

if scripts_folder not in os.sys.path:
    os.sys.path.append(scripts_folder)
    print(f"Đã thêm {scripts_folder} vào sys.path")
if project_folder not in os.sys.path:
    os.sys.path.append(project_folder)
    print(f"Đã thêm {project_folder} vào sys.path")

from sendEmail import GmailSender, generate_pipeline_html_report
from scripts.kafka_listeniner import kafka_send_message, trigger_new_data

pipeline_status = {
    'start_time': datetime.now(),
    'tasks': {},
    'overall_status': 'running'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': "truongtienanh16@gmail.com",
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'movies_data_pipeline',
    default_args=default_args,
    description='Data pipeline for movies data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)



# =========== pipeline status ============
def update_pipeline_status(task_id, status, **kwargs):
    # Kiểm tra nếu task_id không có trong pipeline_status['tasks'], thì khởi tạo task mới
    if task_id not in pipeline_status['tasks']:
        pipeline_status['tasks'][task_id] = {}

    # Sau khi đảm bảo có dictionary cho task_id, cập nhật thông tin status và các thông tin khác
    pipeline_status['tasks'][task_id] = {
        'status': status,
        'start_time': kwargs.get('start_time', datetime.now()),
    }

    if status in ['success', 'failed']:
        pipeline_status['tasks'][task_id]['end_time'] = kwargs.get('end_time', datetime.now())
        if 'start_time' not in pipeline_status['tasks'][task_id]:
            start = pipeline_status['tasks'][task_id]['start_time']
            end = pipeline_status['tasks'][task_id]['end_time']
            execution_time = (end - start).total_seconds()
            pipeline_status['tasks'][task_id]['execution_time'] = execution_time

    if 'error' in kwargs:
        pipeline_status['tasks'][task_id]['error'] = kwargs['error']
    
    for key, value in kwargs.items():
        if key not in ['start_time', 'end_time', 'error']:
            pipeline_status['tasks'][task_id][key] = value
    
    if status == 'running':
        pipeline_status['overall_status'] = 'running'
    if status == 'failed':
        pipeline_status['overall_status'] = 'failed'

# ============ callback ============
# Kiểm tra xem pipeline đã hoàn thành chưa
def is_pipeline_completed():
    for task in pipeline_status['tasks'].values():
        if task.get('status') not in ['success', 'failed']:
            return False
    return True

# Callback khi thành công
def task_success_callback(context):
    task_id = context['task'].task_id
    update_pipeline_status(
        task_id, 
        'success',
        end_time=datetime.now(),
    )
    logging.info(f"Task {task_id} thành công")

    if is_pipeline_completed():
        pipeline_status['end_time'] = datetime.now()
        pipeline_status['overall_status'] = (
            'failed' if any(t['status'] == 'failed' for t in pipeline_status['tasks'].values()) else 'success'
        )
        html = generate_pipeline_html_report(pipeline_status)
        GmailSender(SENDER_EMAIL, APP_PASSWORD).send_email(
            recipients=["truongtienanh16@gmail.com"],
            subject="Báo cáo pipeline Movies",
            body=html,
            is_html=True
        )

# Callback khi thất bại
def task_failure_callback(context):
    task_id = context['task'].task_id
    error = str(context.get('exception', 'Unknown error'))
    update_pipeline_status(
        task_id, 
        'failed',
        end_time=datetime.now(),
        error=error,
    )
    logging.error(f"Task {task_id} thất bại: {error}")

    if is_pipeline_completed():
        pipeline_status['end_time'] = datetime.now()
        pipeline_status['overall_status'] = 'failed'
        html = generate_pipeline_html_report(pipeline_status)
        GmailSender(SENDER_EMAIL, APP_PASSWORD).send_email(
            recipients=["truongtienanh16@gmail.com"],
            subject="Pipeline Movies gặp lỗi",
            body=html,
            is_html=True
        )

# ============ task ============
# Task 1: Chạy script craw_data.py
def run_craw_data(**kwargs):
    task_id = kwargs['task_instance'].task_id
    logging.info(f"running {task_id}")
    update_pipeline_status(task_id, 'running', start_time=datetime.now())

    try:
        craw_path = os.path.join(scripts_folder, "craw_data.py")
        print(craw_path)
        if not os.path.exists(craw_path):
            logging.error(f"File {craw_path} không tồn tại")
            raise FileNotFoundError(f"File {craw_path} không tồn tại")
        
        logging.info(f"Đang chạy file {craw_path}")
        start_time = datetime.now()

        try:
            # Chạy script như một chương trình riêng biệt
            runpy.run_path(craw_path, run_name="__main__")
        except Exception as e:
            logging.error(f"Lỗi khi chạy script craw_data: {e}", exc_info=True)
            raise

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logging.info(f"Thời gian thực thi: {execution_time} giây")

        update_pipeline_status(
            task_id, 
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=execution_time,
        )

        return {
            'status': 'success',
            'message': 'Đã chạy thành công craw_data',
            'execution_time': execution_time,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "user": "tienanh",
        }
    except FileNotFoundError as e:
        logging.error(f"Lỗi: {e}", exc_info=True)
        update_pipeline_status(
            task_id, 
            'failed',
            error=str(e),
        )
        raise
    except Exception as e:
        logging.error(f"Lỗi khi chạy task {task_id}: {e}", exc_info=True)
        update_pipeline_status(
            task_id, 
            'failed',
            error=str(e),
        )
        raise

# Task 2: Chạy script convert_to_json.py
def run_convert_to_json(**kwargs):

    task_id = kwargs['task_instance'].task_id
    logging.info(f"running {task_id}")
    update_pipeline_status(task_id, 'running', start_time=datetime.now())

    try:

        convert_path = os.path.join(scripts_folder, "convert_to_json.py")
        print(convert_path)
        if not os.path.exists(convert_path):
            logging.error(f"File {convert_path} không tồn tại")
            raise FileNotFoundError(f"File {convert_path} không tồn tại")
        
        logging.info(f"Đang chạy file {convert_path}")
        start_time = datetime.now()

        try:
            # Chạy script như một chương trình riêng biệt
            runpy.run_path(convert_path, run_name="__main__")
        except Exception as e:
            logging.error(f"Lỗi khi chạy script convert_to_json: {e}", exc_info=True)
            raise

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logging.info(f"Thời gian thực thi: {execution_time} giây")

        update_pipeline_status(
            task_id, 
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=execution_time,
        )

        return {
            'status': 'success',
            'message': 'Đã chạy thành công convert_to_json',
            'execution_time': execution_time,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "user": "tienanh",
        }
    except FileNotFoundError as e:
        logging.error(f"Lỗi: {e}", exc_info=True)
        update_pipeline_status(
            task_id, 
            'failed',
            error=str(e),
        )
        raise
    except Exception as e:
        logging.error(f"Lỗi khi chạy task {task_id}: {e}", exc_info=True)
        update_pipeline_status(
            task_id, 
            'failed',
            error=str(e),
        )
        raise

# Task 3: Chạy script load_to_dataLake.py
def run_load_to_datalake(**kwargs):
    task_id = kwargs['task_instance'].task_id
    logging.info(f"running {task_id}")
    update_pipeline_status(task_id, 'running', start_time=datetime.now())

    try:
        load_path = os.path.join(scripts_folder, "load_to_dataLake.py")
        print(load_path)
        if not os.path.exists(load_path):
            logging.error(f"File {load_path} không tồn tại")
            raise FileNotFoundError(f"File {load_path} không tồn tại")
        
        logging.info(f"Đang chạy file {load_path}")
        start_time = datetime.now()

        try:
            # Chạy script như một chương trình riêng biệt
            runpy.run_path(load_path, run_name="__main__")
        except Exception as e:
            logging.error(f"Lỗi khi chạy script load_to_datalake: {e}", exc_info=True)
            raise

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logging.info(f"Thời gian thực thi: {execution_time} giây")

        update_pipeline_status(
            task_id, 
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=execution_time,
        )

        return {
            'status': 'success',
            'message': 'Đã chạy thành công load_to_datalake',
            'execution_time': execution_time,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "user": "tienanh",
        }
    except FileNotFoundError as e:
        logging.error(f"Lỗi: {e}", exc_info=True)
        update_pipeline_status(
            task_id, 
            'failed',
            error=str(e),
        )
        raise
    except Exception as e:
        logging.error(f"Lỗi khi chạy task {task_id}: {e}", exc_info=True)
        update_pipeline_status(
            task_id, 
            'failed',
            error=str(e),
        )
        raise


# Task 4: Chay ETL
def trigger_kafka_etl(**kwargs):
    task_id = kwargs['task_instance'].task_id
    logging.info(f"running {task_id}")
    update_pipeline_status(task_id, 'running', start_time=datetime.now())

    try:
        # Gửi thông điệp đến Kafka để kích hoạt ETL
        message = {
            "status": "new_data",
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "source": "test_producer"
        }
        trigger_new_data(timestamp=None)
        logging.info(f"Đã gửi thông điệp đến Kafka: {message}")

        update_pipeline_status(
            task_id, 
            'success',
            start_time=datetime.now(),
        )

        return {
            'status': 'success',
            'message': 'Đã gửi thông điệp đến Kafka thành công',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "user": "tienanh",
        }
    except Exception as e:
        logging.error(f"Lỗi khi gửi thông điệp đến Kafka: {e}", exc_info=True)
        update_pipeline_status(
            task_id, 
            'failed',
            error=str(e),
        )
        raise

# Task 4: Chạy script ETL
def run_etl(**kwargs):
    task_id = kwargs['task_instance'].task_id
    logging.info(f"running {task_id}")
    update_pipeline_status(task_id, 'running', start_time=datetime.now())

    try:
        etl_path = os.path.join(scripts_folder, "ETL.py")
        print(etl_path)
        if not os.path.exists(etl_path):
            logging.error(f"File {etl_path} không tồn tại")
            raise FileNotFoundError(f"File {etl_path} không tồn tại")
        
        logging.info(f"Đang chạy file {etl_path}")
        start_time = datetime.now()

        try:
            # Chạy script như một chương trình riêng biệt
            runpy.run_path(etl_path, run_name="__main__")
        except Exception as e:
            logging.error(f"Lỗi khi chạy script ETL: {e}", exc_info=True)
            raise

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logging.info(f"Thời gian thực thi: {execution_time} giây")

        update_pipeline_status(
            task_id, 
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=execution_time,
        )

        return {
            'status': 'success',
            'message': 'Đã chạy thành công ETL',
            'execution_time': execution_time,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "user": "tienanh",
        }
    except FileNotFoundError as e:
        logging.error(f"Lỗi: {e}", exc_info=True)
        update_pipeline_status(
            task_id, 
            'failed',
            error=str(e),
        )
        raise
    except Exception as e:
        logging.error(f"Lỗi khi chạy task {task_id}: {e}", exc_info=True)
        update_pipeline_status(
            task_id, 
            'failed',
            error=str(e),
        )
        raise
    


# Task 5: chạy script gửi app
# def run_app(**kwargs):
#     task_id = kwargs['task_instance'].task_id
#     logging.info(f"running {task_id}")
#     update_pipeline_status(task_id, 'running', start_time=datetime.now())

#     try:
#         project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#         app_path = os.path.join(project_root, "app", "app.py")
#         # app_path = '/home/tienanh/End-to-End Movie Recommendation/app/app.py'
#         # print(app_path)
#         if not os.path.exists(app_path):
#             logging.error(f"File {app_path} không tồn tại")
#             raise FileNotFoundError(f"File {app_path} không tồn tại")
        
#         logging.info(f"Đang chạy file {app_path}")
#         start_time = datetime.now()

#         try:
#             # Chạy script như một chương trình riêng biệt
#             subprocess.Popen(
#                 ["streamlit", "run", app_path],
#                 stdout=subprocess.DEVNULL,
#                 stderr=subprocess.DEVNULL,
#                 preexec_fn=os.setsid  # tách group tiến trình để tránh bị kill theo
                
#             )
#         except Exception as e:
#             logging.error(f"Lỗi khi chạy script app: {e}", exc_info=True)
#             raise

#         end_time = datetime.now()
#         execution_time = (end_time - start_time).total_seconds()
#         logging.info(f"Thời gian thực thi: {execution_time} giây")

#         update_pipeline_status(
#             task_id, 
#             'success',
#             start_time=start_time,
#             end_time=end_time,
#             execution_time=execution_time,
#         )

#         return {
#             'status': 'success',
#             'message': 'Đã chạy thành công app',
#             'execution_time': execution_time,
#             'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
#             "user": "tienanh",
#         }
#     except FileNotFoundError as e:
#         logging.error(f"Lỗi: {e}", exc_info=True)
#         update_pipeline_status(
#             task_id, 
#             'failed',
#             error=str(e),
#         )
#         raise
#     except Exception as e:
#         logging.error(f"Lỗi khi chạy task {task_id}: {e}", exc_info=True)
#         update_pipeline_status(
#             task_id, 
#             'failed',
#             error=str(e),
#         )
#         raise


# Task 5: Gửi email báo cáo
def send_pipeline_report_email(**kwargs):
    """Generate and send a single email report summarizing all tasks in the pipeline."""
    task_id = kwargs['task_instance'].task_id
    update_pipeline_status(task_id, 'running', start_time=datetime.now())

    start_time = datetime.now()

    try:
        # Thu thập thông tin về tất cả các task từ DAG run hiện tại
        dag_run = kwargs['dag_run']
        task_instances = dag_run.get_task_instances()

        # Cập nhật trạng thái của tất cả các task trong pipeline_status
        for ti in task_instances:
            ti_id = ti.task_id
            ti_state = ti.state

            status = (
                'success' if ti_state == 'success'
                else 'failed' if ti_state == 'failed'
                else 'running'
            )

            if ti_id not in pipeline_status['tasks']:
                update_pipeline_status(
                    ti_id,
                    status,
                    start_time=ti.start_date,
                    end_time=ti.end_date,
                    execution_time=f"{(ti.end_date - ti.start_date).total_seconds():.2f}s" if ti.end_date and ti.start_date else "N/A"
                )

        # Hoàn tất trạng thái tổng thể của pipeline
        if all(t['status'] == 'success' for t in pipeline_status['tasks'].values()):
            pipeline_status['overall_status'] = 'success'
        else:
            pipeline_status['overall_status'] = 'failed'

        html_content = generate_pipeline_html_report(pipeline_status)

        recipients = kwargs['dag_run'].dag.default_args.get('email', ['truongtienanh16@gmail.com'])

        subject = f"Pipeline Movies Data Report - {datetime.now().strftime('%Y-%m-%d')}"

        GmailSender(SENDER_EMAIL, APP_PASSWORD).send_email(
            recipients=recipients,
            subject=subject,
            body=html_content,
            is_html=True
        )

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        logging.info(f"Email báo cáo đã được gửi thành công trong {execution_time:.2f} giây")
        update_pipeline_status(
            task_id,
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=f"{execution_time:.2f}s"
        )

    except Exception as e:
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logging.error(f"Lỗi khi gửi email báo cáo: {e}", exc_info=True)
        update_pipeline_status(
            task_id,
            'failed',
            start_time=start_time,
            end_time=end_time,
            execution_time=f"{execution_time:.2f}s",
            error=str(e)
        )
        raise

#=========== task instances ============
craw_data_task = PythonOperator(
    task_id='craw_data',
    python_callable=run_craw_data,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    provide_context=True,
    dag=dag,
)

convert_to_json_task = PythonOperator(
    task_id='convert_to_json',
    python_callable=run_convert_to_json,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    provide_context=True,
    dag=dag,
)

load_to_datalake_task = PythonOperator(
    task_id='load_to_datalake',
    python_callable=run_load_to_datalake,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    provide_context=True,
    dag=dag,
)

trigger_kafka_etl_task = PythonOperator(
    task_id='trigger_kafka_etl',
    python_callable=trigger_kafka_etl,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    provide_context=True,
    dag=dag,
)


# etl_task = PythonOperator(
#     task_id='ETL',
#     python_callable=run_etl,
#     on_success_callback=task_success_callback,
#     on_failure_callback=task_failure_callback,
#     provide_context=True,
#     dag=dag,
# )

send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_pipeline_report_email,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    provide_context=True,
    dag=dag,
)

#=========== task dependencies ============
craw_data_task >> convert_to_json_task >> load_to_datalake_task >> trigger_kafka_etl_task  >> send_email_task
