#!/usr/bin/env python3
import os
import psycopg2
import logging
import time
from dotenv import load_dotenv

# Load biến môi trường từ file .env
load_dotenv(dotenv_path='/home/tienanh/End-to-End Movie Recommendation/.env')

log_path = os.path.join(os.path.dirname(__file__), "migration.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# DB
local_db = {
    'dbname': os.getenv("PG_DATABASE"),
    'user': os.getenv("PG_USER"),
    'password': os.getenv("PG_PASSWORD"),
    'host': os.getenv("PG_HOST"),
    'port': os.getenv("PG_PORT"),
}

# Neon DB
neon_db = {
    "dbname": os.getenv("NEON_DB_NAME"),
    "user": os.getenv("NEON_DB_USER"),
    "password": os.getenv("NEON_DB_PASSWORD"),
    "host": os.getenv("NEON_DB_HOST"),
    "port": os.getenv("NEON_DB_PORT"),
    "sslmode": os.getenv("NEON_DB_SSLMODE"),
}

# Hàm chuyển đổi kiểu dữ liệu
def convert_data_type(data_type, max_len):
    if data_type == "character varying" and max_len:
        return f"VARCHAR({max_len})"
    elif data_type == "character" and max_len:
        return f"CHAR({max_len})"
    elif data_type == "timestamp without time zone":
        return "TIMESTAMP"
    else:
        return data_type.upper()

def upload_to_cloud():
    """Migrate data from local PostgreSQL to Neon cloud PostgreSQL."""
    try:
        with psycopg2.connect(**local_db) as local_conn, local_conn.cursor() as local_cursor, \
             psycopg2.connect(**neon_db) as neon_conn, neon_conn.cursor() as neon_cursor:

            # Lấy danh sách bảng
            local_cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
            """)
            tables = [table[0] for table in local_cursor.fetchall()]
            logger.info(f"Discovered {len(tables)} tables: {tables}")

            exclude_tables = ['temp_table', 'logs', 'etl_log']
            tables = [t for t in tables if t not in exclude_tables]

            for table_name in tables:
                logger.info(f"Processing table: {table_name}")
                start_time = time.time()

                # Lấy thông tin cột
                local_cursor.execute(f"""
                    SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}' AND table_schema = 'public'
                    ORDER BY ordinal_position
                """)
                columns = local_cursor.fetchall()

                # Tạo bảng trên Neon
                col_defs = []
                for col in columns:
                    col_name, data_type, max_len, is_nullable, default_val = col
                    data_type = convert_data_type(data_type, max_len)
                    nullable = "NOT NULL" if is_nullable == "NO" else ""
                    default = f"DEFAULT {default_val}" if default_val else ""
                    col_defs.append(f"{col_name} {data_type} {default} {nullable}".strip())
                create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)});"
                neon_cursor.execute(create_stmt)
                neon_conn.commit()
                logger.info(f"Created table {table_name} on Neon (if not exists)")

                neon_cursor.execute(f"TRUNCATE TABLE {table_name};")
                neon_conn.commit()

                local_cursor.execute(f"SELECT * FROM {table_name};")
                rows = local_cursor.fetchall()
                if not rows:
                    logger.info(f"No data in {table_name}, skipping insert.")
                    continue

                # Chèn dữ liệu theo batch
                batch_size = 1000
                placeholders = ','.join(['%s'] * len(columns))
                insert_stmt = f"INSERT INTO {table_name} VALUES ({placeholders})"

                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    neon_cursor.executemany(insert_stmt, batch)
                    neon_conn.commit()
                    logger.info(f"Inserted {min(i + batch_size, len(rows))}/{len(rows)} rows into {table_name}")

                logger.info(f"Migrated {table_name} in {time.time() - start_time:.2f} seconds")

    except Exception as e:
        logger.error(f"Error during migration: {e}")

if __name__ == "__main__":
    upload_to_cloud()
    logger.info("Data migration completed successfully.")

