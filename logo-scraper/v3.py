import os
import requests
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import time

# Configure API keys
GOOGLE_API_KEY = "AIzaSyB-uW6I0JO0Cgms8uYPM86b1dTOfjU4TgE"
SEARCH_ENGINE_ID = "d626f24be7e0045ed"

def search_images(query, start_index=1):
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        'key': GOOGLE_API_KEY,
        'cx': SEARCH_ENGINE_ID,
        'q': query,
        'searchType': 'image',
        'num': 10,  # Increased to 10 to get more results per request
        'start': start_index,
        'fileType': 'svg,png,jpg',
        'imgType': 'clipart',
        'safe': 'active'
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json()
        if 'items' not in results:
            return []
        return [item['link'] for item in results['items']]
    except requests.exceptions.RequestException:
        return []

def download_image(url, folder_path, filename, max_retries=3):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
            response.raise_for_status()
            
            content_type = response.headers.get('Content-Type', '').lower()
            
            if 'image' in content_type:
                file_extension = content_type.split('/')[-1]
                if file_extension == 'jpeg':
                    file_extension = 'jpg'
                elif file_extension == 'svg+xml':
                    file_extension = 'svg'
            else:
                parsed_url = urlparse(response.url)
                file_extension = os.path.splitext(parsed_url.path)[1]
                if file_extension.startswith('.'):
                    file_extension = file_extension[1:]
            
            if not file_extension:
                return False
            
            file_path = os.path.join(folder_path, f"{filename}.{file_extension}")
            with open(file_path, 'wb') as f:
                f.write(response.content)
            return True
        
        except requests.exceptions.RequestException:
            time.sleep(1)  # Wait a bit before retrying
    
    return False

def process_school(school, nickname, division_folder):
    school_folder = os.path.join(division_folder, school)
    os.makedirs(school_folder, exist_ok=True)
    
    query = f"{school} {nickname} Athletics Logo"
    start_index = 1
    downloaded_count = 0
    
    while downloaded_count < 5:
        image_urls = search_images(query, start_index)
        if not image_urls:
            break
        
        for url in image_urls:
            if downloaded_count >= 5:
                break
            filename = f"{school}_{nickname}_logo_{downloaded_count + 1}"
            if download_image(url, school_folder, filename):
                downloaded_count += 1
        
        start_index += 10
    
    print(f"Downloaded {downloaded_count} logos for {school} {nickname}")

def process_excel(input_file):
    logos_folder = "logos"
    os.makedirs(logos_folder, exist_ok=True)
    xls = pd.ExcelFile(input_file)
    
    for sheet_name in xls.sheet_names:
        if sheet_name != "NCAA D1" and sheet_name != "NCAA D2":
            continue
        
        df = pd.read_excel(xls, sheet_name)
        division_folder = os.path.join(logos_folder, sheet_name)
        os.makedirs(division_folder, exist_ok=True)
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for _, row in df.iterrows():
                school = row['School']
                nickname = row['Nickname']
                futures.append(executor.submit(process_school, school, nickname, division_folder))
            
            for future in futures:
                future.result()

if __name__ == "__main__":
    input_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    process_excel(input_file)