import os
import requests
import pandas as pd
from urllib.parse import quote
import json

# Oxylabs API credentials
USERNAME = 'logo_scraper_1234_datqZ'
PASSWORD = 'Password123456~'

def search_images(query, num_images=3):
    payload = {
        'source': 'google_search',
        'domain': 'com',
        'query': f"{query} filetype:svg",
        'parse': True,
        'context': [
            {'key': 'tbm', 'value': 'isch'},
        ],
    }
    
    try:
        response = requests.post(
            'https://realtime.oxylabs.io/v1/queries',
            auth=(USERNAME, PASSWORD),
            json=payload,
        )
        
        response.raise_for_status()
        
        results = response.json()
        
        # Debug: Print the structure of the results
        print(json.dumps(results, indent=2))
        
        image_results = results.get('results', [{}])[0].get('content', {}).get('results', {}).get('organic', [])
        svg_urls = []
        
        for item in image_results:
            if isinstance(item, dict) and 'link' in item:
                url = item['link']
                if url.lower().endswith('.svg'):
                    svg_urls.append(url)
            
            if len(svg_urls) >= num_images:
                break
        
        return svg_urls
    except requests.exceptions.RequestException as e:
        print(f"Error in Oxylabs API request: {str(e)}")
        return []
    except Exception as e:
        print(f"Unexpected error in search_images: {str(e)}")
        return []

def download_svg(url, folder_path, filename):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        content_type = response.headers.get('Content-Type', '').lower()
        if 'svg' in content_type or url.lower().endswith('.svg'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {file_path}")
        else:
            print(f"Skipped non-SVG file: {url}")
    except Exception as e:
        print(f"Error downloading SVG from {url}: {str(e)}")

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
                    filename = f"{school}_{nickname}_logo_{i}.svg"
                    download_svg(url, school_folder, filename)
            else:
                print(f"No SVG logos found for {school} {nickname}")

if __name__ == "__main__":
    input_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    process_excel(input_file)