#!/usr/bin/env python
"""
Send pipeline report via Gmail using Python.
This script uses Gmail's SMTP server to send an email report.
"""
import os
import smtplib
import ssl
from datetime import datetime
from typing import Dict, List, Union
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv


# Load biến môi trường
load_dotenv()

SENDER_EMAIL = os.getenv("SENDER_EMAIL") # your email
APP_PASSWORD = os.getenv("APP_PASSWORD") # your app password
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

class GmailSender:
    def __init__(self, sender_email: str, app_password: str):
        self.sender_email = sender_email
        self.app_password = app_password

    def send_email(
        self,
        recipients: Union[str, List[str]],
        subject: str,
        body: str,
        is_html: bool = False
    ):
        if isinstance(recipients, str):
            recipients = [recipients]

        # Tạo message
        message = MIMEMultipart()
        message["From"] = self.sender_email
        message["To"] = ", ".join(recipients)
        message["Subject"] = subject

        mime_type = "html" if is_html else "plain"
        message.attach(MIMEText(body, mime_type))

        # Gửi mail
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(self.sender_email, self.app_password)
            server.sendmail(self.sender_email, recipients, message.as_string())

        print(f"Đã gửi email đến: {', '.join(recipients)}")

def generate_pipeline_html_report(pipeline_status: Dict) -> str:
    run_date = datetime.now().strftime('%Y-%m-%d')
    start_time = pipeline_status.get('start_time')
    end_time = pipeline_status.get('end_time')
    overall_status = pipeline_status.get('overall_status', 'unknown').upper()
    execution_time = (end_time - start_time).total_seconds() if end_time and start_time else 0

    task_rows = ""
    for task_name, info in pipeline_status.get("tasks", {}).items():
        status = info.get('status', 'unknown').upper()
        status_class = "running" if status == "RUNNING" else "success" if status == "SUCCESS" else "failed"
        task_rows += f"""
        <tr class="{status_class}">
            <td>{task_name}</td>
            <td><span class="badge {status_class}">{status}</span></td>
            <td>{info.get('start_time', datetime.now()).strftime('%H:%M:%S')}</td>
            <td>{info.get('end_time', '-').strftime('%H:%M:%S') if info.get('end_time') else '-'}</td>
            <td>{info.get('execution_time', 'N/A')}</td>
            <td>{info.get('error', '')}</td>
        </tr>
        """

    return f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                font-family: 'Segoe UI', sans-serif;
                background-color: #fdfdfd;
                color: #333;
                margin: 0;
                padding: 0;
            }}
            .header {{
                background: linear-gradient(90deg, #a18cd1, #fbc2eb);
                color: #222;
                padding: 24px 40px;
                font-size: 26px;
                font-weight: bold;
                text-align: center;
                box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
            }}
            .container {{
                padding: 40px;
                max-width: 1000px;
                margin: auto;
                background: white;
                border-radius: 12px;
                box-shadow: 0 0 15px rgba(0,0,0,0.05);
            }}
            h3 {{
                color: #6a5acd;
                margin-bottom: 10px;
            }}
            .info p {{
                margin: 6px 0;
                font-size: 16px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}
            th {{
                background-color: #eaeaff;
                color: #444;
                padding: 12px;
                font-size: 15px;
                text-align: left;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
            }}
            td {{
                padding: 10px;
                border-bottom: 1px solid #eee;
                font-size: 14px;
            }}
            tr:last-child td {{
                border-bottom: none;
            }}
            .badge {{
                padding: 4px 10px;
                border-radius: 12px;
                font-size: 12px;
                font-weight: 600;
                text-transform: uppercase;
                display: inline-block;
            }}
            .success {{
                background-color: #d4edda;
                color: #155724;
            }}
            .failed {{
                background-color: #f8d7da;
                color: #721c24;
            }}
            .running {{
                background-color: #fff3cd;
                color: #856404;
            }}
            .footer {{
                margin-top: 40px;
                text-align: center;
                font-size: 13px;
                color: #888;
            }}
        </style>
    </head>
    <body>
        <div class="header">🚀 Pipeline Execution Report</div>
        <div class="container">
            <h3>Summary</h3>
            <div class="info">
                <p><strong>Date:</strong> {run_date}</p>
                <p><strong>Start Time:</strong> {start_time.strftime('%Y-%m-%d %H:%M:%S') if start_time else 'N/A'}</p>
                <p><strong>End Time:</strong> {end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'N/A'}</p>
                <p><strong>Total Duration:</strong> {execution_time:.2f} seconds</p>
                <p><strong>Status:</strong> <span class="badge {overall_status.lower()}">{overall_status}</span></p>
            </div>

            <h3>Task Details</h3>
            <table>
                <tr>
                    <th>Task</th>
                    <th>Status</th>
                    <th>Start</th>
                    <th>End</th>
                    <th>Duration</th>
                    <th>Notes</th>
                </tr>
                {task_rows}
            </table>

            <div class="footer">
                Powered by trgtanhh2004 | Generated on {run_date}
            </div>
        </div>
    </body>
    </html>
    """


if __name__ == "__main__":
    test_status = {
        'start_time': datetime.now(),
        'end_time': datetime.now(),
        'overall_status': 'success',
        'tasks': {
            'Task A': {
                'status': 'success',
                'start_time': datetime.now(),
                'end_time': datetime.now(),
                'execution_time': '4.5s'
            },
            'Task B': {
                'status': 'failed',
                'start_time': datetime.now(),
                'end_time': datetime.now(),
                'execution_time': '2.3s',
                'error': 'API timeout'
            }
        }
    }

    html_body = generate_pipeline_html_report(test_status)
    gmail = GmailSender(SENDER_EMAIL, APP_PASSWORD)

    gmail.send_email(
        recipients="truongtienanh16@gmail.com",
        subject="Báo cáo Pipeline tự động",
        body=html_body,
        is_html=True
    )
