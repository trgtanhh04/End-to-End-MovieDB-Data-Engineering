import os
import random
import logging
import time
import requests
from bs4 import BeautifulSoup
import re
import csv
from datetime import datetime

# Configuration parameters
# Configuration parameters
CONFIG = {
    'url_template': 'https://www.themoviedb.org/movie?page={}',
    'output_dir': os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data')),  
    'max_pages': 1,  # Change this to the number of pages you want to crawl
    'user_agents': [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
    ],
    'retry_attempts': 3,
    'retry_delay': 5,
    'page_load_delay': (3, 6)
}

# Tạo thư mục nếu chưa tồn tại
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
os.makedirs(log_dir, exist_ok=True)  # Đảm bảo thư mục tồn tại

# Configure logging
log_file = os.path.join(log_dir, 'crawler.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, 'a'),  # Lưu log vào data/crawler.log
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
# Danh sách lưu dữ liệu
movies = []

def initialize_output_directory(output_dir):
    """Create output directory if it doesn't exist."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    return output_dir

def get_session():
    """Initialize and return a configured requests session."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(CONFIG['user_agents'])
    })
    return session

def extract_movie_info(movie_id, session):
    """Extract movie information from individual movie page."""
    movie_url = f"https://www.themoviedb.org/movie/{movie_id}"
    response = session.get(movie_url)

    if response.status_code != 200:
        logger.error(f"Cannot retrieve data from {movie_url}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extracting movie details
    name = soup.find('h2').get_text(strip=True) if soup.find('h2') else "N/A"

    year = soup.find('span', class_='release').get_text(strip=True).strip("()") if soup.find('span', class_='release') else "N/A"

    genres = [a.get_text(strip=True) for a in soup.find('span', class_='genres').find_all('a')] if soup.find('span', class_='genres') else ["N/A"]

    runtime = soup.find('span', class_='runtime').get_text(strip=True) if soup.find('span', class_='runtime') else "N/A"

    score = soup.find('div', class_='user_score_chart')['data-percent'] if soup.find('div', class_='user_score_chart') else "N/A"
    
    director = soup.find('li', class_='profile').find('a').get_text(strip=True) if soup.find('li', class_='profile') else "N/A"

    facts_section = soup.find('section', class_='facts left_column')
    status, language, budget, revenue = "N/A", "N/A", "N/A", "N/A"
    if facts_section:
        for ele in facts_section.find_all('p'):
            text = ele.get_text(strip=True)
            if "Status" in text:
                status = text.split("Status")[1].strip()
            elif "Original Language" in text:
                language = text.split("Original Language")[1].strip()
            elif "Budget" in text:
                budget = text.split("Budget")[1].strip()
            elif "Revenue" in text:
                revenue = text.split("Revenue")[1].strip()

    image = soup.find('img', class_='poster')['src'] if soup.find('img', class_='poster') else "N/A"
    overview = soup.find('div', class_='overview').get_text(strip=True) if soup.find('div', class_='overview') else "N/A"

    return {
        "id": movie_id,
        "name": name,
        "release_year": year,
        "genre": genres,
        "runtime": runtime,
        "score": score,
        "director": director,
        "status": status,
        "language": language,
        "budget": budget,
        "revenue": revenue,
        "image": image,
        "overview": overview
    }

def crawl_page(page_number, session):
    """Crawl a single page for movie listings."""
    base_url = CONFIG['url_template'].format(page_number)
    response = session.get(base_url)

    if response.status_code != 200:
        logger.error(f"Cannot access {base_url}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract movie IDs
    movie_ids = set(re.search(r'/movie/(\d+)', a_tag['href']).group(1) for a_tag in soup.find_all('a', href=re.compile(r'/movie/\d+')))
    logger.info(f"Page {page_number}: Found {len(movie_ids)} movies")

    return movie_ids

def save_movies(output_dir, movies):
    """Save movie data to CSV file."""
    file_path = os.path.join(output_dir, "movies.csv")
    with open(file_path, "w", encoding="utf-8", newline='') as file:
        writer = csv.DictWriter(file, fieldnames=[
            "id",
            "name", 
            "release_year", 
            "genre", 
            "runtime", 
            "score", 
            "director",
            "status", 
            "language", 
            "budget", 
            "revenue", 
            "image", 
            "overview", 
            'page', 
            'index', 
            'time_craw'
        ])
        writer.writeheader()
        for movie in movies:
            writer.writerow(movie)
    logger.info(f"Data saved to file {file_path}")

def main():
    """Main function to run the crawler."""
    logger.info("Starting the crawling process")
    start_time = time.time()

    # Initialize output directory
    initialize_output_directory(CONFIG['output_dir'])

    session = get_session()

    for page in range(1, CONFIG['max_pages'] + 1):
        movie_ids = crawl_page(page, session)

        index = 1
        for movie_id in movie_ids:
            movie_info = extract_movie_info(movie_id, session)
            if movie_info:
                movie_info["page"] = page
                movie_info["index"] = index
                movie_info["time_craw"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Format chuẩn
                movies.append(movie_info)
            time.sleep(random.uniform(*CONFIG['page_load_delay']))  # Random delay to avoid blocking
            index += 1
        index = 1
        logger.info(f"Page {page} completed!")

    save_movies(CONFIG['output_dir'], movies)

    elapsed_time = time.time() - start_time
    logger.info(f"Data collection complete. Elapsed time: {elapsed_time:.2f} seconds")

    # Return success status
    return True

if __name__ == "__main__":
    main()