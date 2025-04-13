import os
import logging
import requests
from datetime import datetime
import shutil

# Configure logging
logs_dir = "/home/tienanh/End-to-End Movie Recommendation/archive"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, "load_to_hdfs.log")),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'json_data_dir': '/home/tienanh/End-to-End Movie Recommendation/data_json', # fix path
    'hdfs_url': 'http://127.0.0.1:9870/webhdfs/v1',
    'hdfs_user': 'tienanh',
    'hdfs_base_dir': f"/user/hadoop/datalake/{datetime.now().strftime('%Y-%m-%d')}",
    'archive_dir': '/home/tienanh/End-to-End Movie Recommendation/archive', # fix path
    'user': 'tienanh',
    'current_time': '2025-04-10 02:30:39'  # Current time
}

class WebHDFSClient:
    """Simple WebHDFS client using requests library."""

    def __init__(self, base_url, user):
        """Initialize WebHDFS client."""
        self.base_url = base_url.rstrip('/')
        self.user = user
        self.session = requests.Session()

    def _build_url(self, path, operation, **params):
        """Build WebHDFS URL."""
        path = path.lstrip('/')
        params['op'] = operation
        params['user.name'] = self.user

        # Convert params to query string
        query = '&'.join([f"{k}={v}" for k, v in params.items()])
        return f"{self.base_url}/{path}?{query}"

    def check_exists(self, path):
        """Check if file/directory exists in HDFS."""
        try:
            url = self._build_url(path, 'GETFILESTATUS')
            response = self.session.get(url)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Error checking existence of {path}: {str(e)}")
            return False

    def mkdir(self, path):
        """Create directory in HDFS if it doesn't exist."""
        if self.check_exists(path):
            logger.info(f"Directory already exists: {path}")
            return True

        try:
            url = self._build_url(path, 'MKDIRS')
            response = self.session.put(url)
            if response.status_code == 200:
                logger.info(f"Created directory: {path}")
                return True
            else:
                logger.error(f"Error creating directory {path}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Exception creating directory {path}: {str(e)}")
            return False

    def upload_file(self, local_path, hdfs_path):
        """Upload file to HDFS."""
        try:
            create_url = self._build_url(hdfs_path, 'CREATE', overwrite='true')
            response = self.session.put(create_url, allow_redirects=False)
            if response.status_code != 307:
                logger.error(f"Error creating file on HDFS: {response.text}")
                return False

            redirect_url = response.headers['Location']
            with open(local_path, 'rb') as f:
                upload_response = self.session.put(redirect_url, data=f)
            if upload_response.status_code == 201:
                logger.info(f"Uploaded {local_path} to {hdfs_path}")
                return True
            else:
                logger.error(f"Error uploading {local_path}: {upload_response.text}")
                return False
        except Exception as e:
            logger.error(f"Exception uploading {local_path}: {str(e)}")
            return False

def upload_json_folders_to_hdfs(local_root, hdfs_root, client):
    """Upload JSON files from local to HDFS."""
    for folder in os.scandir(local_root):
        if folder.is_dir():
            folder_name = folder.name
            hdfs_folder_path = f"{hdfs_root}/{folder_name}"
            if client.mkdir(hdfs_folder_path):
                logger.info(f"Processing folder: {folder_name}")
                for file in os.scandir(folder.path):
                    if file.is_file() and file.name.endswith('.json'):
                        local_file_path = file.path
                        hdfs_file_path = f"{hdfs_folder_path}/{file.name}"
                        client.upload_file(local_file_path, hdfs_file_path)


def archive_processed_data(json_data_dir):
    """Archive the processed data."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        archive_folder_name = f"json_data_{timestamp}"
        archive_path = os.path.join(CONFIG['archive_dir'], archive_folder_name)
        os.makedirs(archive_path, exist_ok=True)
        for item in os.listdir(json_data_dir):
            s = os.path.join(json_data_dir, item)
            d = os.path.join(archive_path, item)
            if os.path.isdir(s):
                shutil.copytree(s, d, False, None)
            else:
                shutil.copy2(s, d)
        logger.info(f"Archived data to {archive_path}")
        return True
    except Exception as e:
        logger.error(f"Error archiving data: {str(e)}")
        return False
    
def main():
    """Main function to load data to HDFS."""
    logger.info("Starting data upload process to HDFS")

    hdfs_client = WebHDFSClient(CONFIG['hdfs_url'], CONFIG['hdfs_user'])

    if hdfs_client.mkdir(CONFIG['hdfs_base_dir']):
        logger.info(f"HDFS base directory is ready: {CONFIG['hdfs_base_dir']}")

    upload_json_folders_to_hdfs(CONFIG['json_data_dir'], CONFIG['hdfs_base_dir'], hdfs_client)
    archive_processed_data(CONFIG['json_data_dir'])
    logger.info("Data upload process completed")

if __name__ == "__main__":
    main()