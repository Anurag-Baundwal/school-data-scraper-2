import os
import requests
import pandas as pd
import json

# Configure API keys
GOOGLE_API_KEY = "AIzaSyB-uW6I0JO0Cgms8uYPM86b1dTOfjU4TgE"
SEARCH_ENGINE_ID = "d626f24be7e0045ed"

def search_images(query, num_images=5):
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        'key': GOOGLE_API_KEY,
        'cx': SEARCH_ENGINE_ID,
        'q': query,
        'searchType': 'image',
        'num': num_images,
        'fileType': 'svg,png,jpg',  # Prioritize SVG
        'imgType': 'clipart',
        'safe': 'active'
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json()
        
        print("Google Custom Search API Response:")
        print(json.dumps(results, indent=2))
        
        if 'items' not in results:
            print("No 'items' found in the API response.")
            if 'error' in results:
                print(f"API Error: {results['error']['message']}")
            return []
        
        return [item['link'] for item in results['items']]
    except requests.exceptions.RequestException as e:
        print(f"Error in Google Custom Search API request: {str(e)}")
        return []

import requests
from urllib.parse import urlparse
import os

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
                # Try to get file extension from Content-Type
                file_extension = content_type.split('/')[-1]
                if file_extension == 'jpeg':
                    file_extension = 'jpg'
                elif file_extension == 'svg+xml':
                    file_extension = 'svg'
            else:
                # Try to get file extension from URL
                parsed_url = urlparse(response.url)
                file_extension = os.path.splitext(parsed_url.path)[1]
                if file_extension.startswith('.'):
                    file_extension = file_extension[1:]
            
            if not file_extension:
                print(f"Could not determine file extension for {url}")
                return
            
            file_path = os.path.join(folder_path, f"{filename}.{file_extension}")
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {file_path}")
            return  # Successful download, exit the function
        
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed. Error downloading image from {url}: {str(e)}")
    
    print(f"Failed to download image after {max_retries} attempts: {url}")

def process_excel(input_file):
    logos_folder = "logos"
    os.makedirs(logos_folder, exist_ok=True)

    xls = pd.ExcelFile(input_file)

    for sheet_name in xls.sheet_names:
        if sheet_name != "JUCO - USCAA":
            continue
        
        df = pd.read_excel(xls, sheet_name)
        
        division_folder = os.path.join(logos_folder, sheet_name)
        os.makedirs(division_folder, exist_ok=True)
        
        for _, row in df.iterrows():
            school = row['School']
            nickname = row['Nickname']
            
            query = f"{school} {nickname} Athletics Logo"
            
            image_urls = search_images(query)
            
            if image_urls:
                school_folder = os.path.join(division_folder, school)
                os.makedirs(school_folder, exist_ok=True)
                
                for i, url in enumerate(image_urls, 1):
                    filename = f"{school}_{nickname}_logo_{i}"
                    download_image(url, school_folder, filename)
            else:
                print(f"No logos found for {school} {nickname}")

if __name__ == "__main__":
    input_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    process_excel(input_file)