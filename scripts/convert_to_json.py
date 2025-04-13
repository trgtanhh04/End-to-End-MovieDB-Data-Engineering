
import os
import json
import logging
import pandas as pd
from datetime import datetime

# Configure logging - using absolute paths instead of __file__
logs_dir = "/home/tienanh/End-to-End Movie Recommendation/data_json"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, "convert_to_json.log")),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Configuration - use absolute paths
CONFIG = {
    'input_csv': '/home/tienanh/End-to-End Movie Recommendation/data/movies.csv',
    'output_dir': '/home/tienanh/End-to-End Movie Recommendation/data_json'
}


def initialize_output_directory(output_dir):
    """Create output directory if it doesn't exist."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    return output_dir


def convert_csv_to_json(input_csv, output_dir):
    """Convert data from CSV to JSON and group by 20 files per directory."""
    df = pd.read_csv(input_csv)

    total_files = len(df)
    num_folders = (total_files // 20) + (1 if total_files % 20 else 0)

    logger.info(f"Total records: {total_files}, creating {num_folders} folders.")

    for folder_idx in range(num_folders):
        folder_name = os.path.join(output_dir, f"batch_{folder_idx+1}")
        os.makedirs(folder_name, exist_ok=True)

        start_idx = folder_idx * 20
        end_idx = min(start_idx + 20, total_files)

        for idx in range(start_idx, end_idx):
            record = df.iloc[idx].to_dict()

            metadata = {
                'page': int(df.iloc[idx]['page']),
                'index': int(df.iloc[idx]['index']),
                'time_craw': df.iloc[idx]['time_craw']
            }

            record.pop('page', None)
            record.pop('index', None)
            record.pop('time_craw', None)

            record["metadata"] = metadata

            json_path = os.path.join(folder_name, f"movie_{idx+1}.json")

            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=4, default=str)

            logger.info(f"Saved JSON: {json_path}")


def main():
    """Main function to convert CSV to JSON."""
    logger.info("Starting CSV to JSON conversion process")

    initialize_output_directory(CONFIG['output_dir'])
    convert_csv_to_json(CONFIG['input_csv'], CONFIG['output_dir'])

    logger.info("CSV to JSON conversion complete")


if __name__ == "__main__":
    main()