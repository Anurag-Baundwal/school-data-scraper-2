# import os
import base64
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException
import time
import google.generativeai as genai
import json
from urllib.parse import urlparse
import pandas as pd
from datetime import datetime
import asyncio
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import aiohttp
from ssl import SSLError
import sys

#------------------- API KEYS ----------------------------------------
# Python code to load and parse the environment variables:
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Parse Gemini API keys as a list
GEMINI_API_KEYS = os.getenv('GEMINI_API_KEYS').split(',')

# Load other environment variables
OXYLABS_USERNAME = os.getenv('OXYLABS_USERNAME')
OXYLABS_PASSWORD = os.getenv('OXYLABS_PASSWORD')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
SEARCH_ENGINE_ID = os.getenv('SEARCH_ENGINE_ID')

#--------------------------------------------------------------------------

def get_random_api_key():
    return random.choice(GEMINI_API_KEYS)

async def take_full_screenshot(driver, url):
    """Takes a full-page screenshot with improved error handling.""" 
    max_retries = 3
    for attempt in range(max_retries):
        try:
            driver.get(url)
            await asyncio.sleep(5)  # Allow time for dynamic content
            
            # Scroll to the bottom of the page to ensure all content is loaded
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(2)  # Wait for any lazy-loaded content
            
            # Scroll back to the top
            driver.execute_script("window.scrollTo(0, 0);")
            
            # Set window size to capture full page
            total_height = driver.execute_script("return document.body.scrollHeight")
            driver.set_window_size(1920, total_height)
            
            # Take screenshot
            screenshot = driver.get_screenshot_as_base64()
            return screenshot
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"All attempts failed for {url}: {e}")
                return None

async def extract_roster_data(screenshot_base64, url, school_name, nickname):
    genai.configure(api_key=get_random_api_key())

    generation_config = {
        "temperature": 0.2,
        "top_p": 0.95,
        "top_k": 64,
        "max_output_tokens": 8192,
    }
    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash",
        generation_config=generation_config,
    )
    
    image_parts = [
        {
            "mime_type": "image/jpeg",
            "data": base64.b64decode(screenshot_base64)
        }
    ]
    
    current_year = datetime.now().year
    
    prompt = f"""
    Analyze the screenshot of a college softball roster webpage from {url}. The expected school name is "{school_name}" and the team nickname or name should be related to "{nickname}". Focus ONLY on player information, ignoring any coach or staff data that might be present. Extract the following information for each player:
    - Name
    - Position
    - Year (Fr, So, Jr, Sr, Grad, etc)
    - Hometown (before the slash in the Hometown/High School column)
    - High School (after the slash in the Hometown/High School column)
    - Graduation Year (calculate based on the player's year and the roster year)

    Determine the roster year. Look for an explicit mention of the roster year on the page (e.g., "2024 Softball Roster"). If not found, assume it's for the upcoming season ({current_year + 1}).

    For the Graduation Year calculation, use the determined roster year as the base:
    - Freshman (Fr) or First Year: Roster Year + 3
    - Sophomore (So) or Second Year: Roster Year + 2
    - Junior (Jr) or Third Year: Roster Year + 1
    - Senior (Sr) or Fourth Year: Roster Year
    - Graduate (Grad) or Fifth Year: Roster Year
    - If the year is unclear, set to null

    Format the output as a JSON string with the following structure:
    {{
        "success": true/false,
        "reason": "reason for failure" (or null if success),
        "rosterYear": YYYY,
        "players": [
            {{
                "name": "...",
                "position": "...",
                "year": "...",
                "hometown": "...",
                "highSchool": "...",
                "graduationYear": YYYY
            }},
            ...
        ]
    }}

    Set "success" to false if:
    1. No player data is found
    2. Any player is missing one or more of the required fields (name, position, year, hometown, highSchool)
    3. The roster year cannot be determined
    4. The school name or team name/nickname on the page doesn't match the expected "{school_name}" or "{nickname}"

    If "success" is false, provide a brief explanation in the "reason" field.
    """
    
    with ThreadPoolExecutor(max_workers=1) as executor:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            executor,
            model.generate_content,
            [prompt, image_parts[0]]
        )
    return response.text

async def search_oxylabs(query):
    url = "https://realtime.oxylabs.io/v1/queries"
    payload = {
        'source': 'google_search',
        'query': query,
        'geo_location': 'United States',
        'parse': True
    }
    
    auth_string = f"{OXYLABS_USERNAME}:{OXYLABS_PASSWORD}"
    auth_bytes = auth_string.encode('ascii')
    auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_b64}"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    results = await response.json()
                    # Extract organic search result URLs
                    organic_results = results.get('results', [{}])[0].get('content', {}).get('results', {}).get('organic', [])
                    return [result['url'] for result in organic_results if 'url' in result]
                else:
                    print(f"Oxylabs API request failed with status code: {response.status}")
                    print(await response.text())
                    return []
    except Exception as e:
        print(f"Error in Oxylabs API request: {str(e)}")
        return []

async def fallback_search(college_name, nickname, driver):
    search_query = f"{college_name} {nickname} 2024 softball roster"
    try:
        search_results = await search_oxylabs(search_query)
        for url in search_results:
            screenshot = await take_full_screenshot(driver, url)
            if screenshot:
                roster_data = await extract_roster_data(screenshot, url, college_name, nickname)
                cleaned_json = roster_data.strip().lstrip('```json').rstrip('```').strip()
                data = json.loads(cleaned_json)
                if data['success']:
                    print(f"Fallback search successful for {college_name} using URL: {url}")
                    return data, url
        print(f"Fallback search failed for {college_name}")
        return None, None
    except Exception as e:
        print(f"Error in fallback search for {college_name}: {str(e)}")
        return None, None
    
def process_roster_data(data, url):
    roster_year = data['rosterYear']
    players = data['players']
    
    processed_data = []
    for player in players:
        processed_data.append({
            "Name": player['name'],
            "Position": player['position'],
            "Year": player['year'],
            "Hometown": player['hometown'],
            "High School": player['highSchool'],
            "Graduation Year": player['graduationYear'],
            "Roster Year": roster_year,
            "URL": url
        })
    
    return pd.DataFrame(processed_data)

async def process_roster(driver, url, college_name, nickname):
    print(f"Attempting to scrape data for {college_name} from {url}")
    try:
        screenshot = await take_full_screenshot(driver, url)

        if screenshot:
            print(f"Successfully captured screenshot for {college_name}")
            roster_data = await extract_roster_data(screenshot, url, college_name, nickname)
            cleaned_json = roster_data.strip().lstrip('```json').rstrip('```').strip()
            data = json.loads(cleaned_json)

            if data['success']:
                print(f"Successfully extracted roster data for {college_name}")
                return process_roster_data(data, url), url
            else:
                print(f"Initial scraping failed for {college_name}: {data['reason']}")
                print(f"Attempting fallback search for {college_name}")
                data, new_url = await fallback_search(college_name, nickname, driver)
                if data:
                    print(f"Fallback search successful for {college_name}")
                    return process_roster_data(data, new_url), new_url
                else:
                    print(f"Fallback search failed for {college_name}")
                    return None, None
        else:
            print(f"Failed to capture screenshot for {college_name}")
            return None, None
    except Exception as e:
        print(f"Error processing roster for {college_name} from {url}: {str(e)}")
        return None, None

async def process_college(college_data, df, sheet_name, driver):
    url = college_data['2024 Roster URL']
    college_name = college_data['School']
    nickname = college_data['Nickname']
    # nickname = college_data.get('Nickname', ''
    # college_name = college_data['School'])  # Make sure this column exists in your Excel file
    row_index = college_data.name  # Get row index

    if pd.notna(url):
        print(f"\nProcessing {college_name} (existing URL: {url})")
        result_df, successful_url = await process_roster(driver, url, college_name, nickname) 
        if result_df is not None:
            print(f"Successfully scraped data for {college_name}")
            # Update the Excel file with the successful URL
            df.loc[row_index, '2024 Roster URL'] = successful_url
            with pd.ExcelWriter("your_excel_file.xlsx", mode="a", engine="openpyxl", if_sheet_exists="overlay") as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            return result_df
        else:
            print(f"Scraping failed for {college_name}")
            return (college_name, url, "Scraping failed")
    else:
        print(f"Skipping {college_name} - No URL provided")
    return None

async def process_sheet(sheet_name, df):
    all_rosters = []
    failed_urls = []

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install()) 
    driver = webdriver.Chrome(service=service, options=options)

    # Decrease max_workers to reduce resource usage
    with ThreadPoolExecutor(max_workers=10) as executor:  
        futures = [executor.submit(asyncio.run, process_college(row, df.copy(), sheet_name, driver))  
                   for _, row in df.iterrows()]

        for future in as_completed(futures):
            result = future.result()
            if isinstance(result, pd.DataFrame):
                all_rosters.append(result)
            elif result is not None:
                failed_urls.append(result)
    
    driver.quit()
    return all_rosters, failed_urls

async def main():
    excel_file = r"C:\Users\dell3\source\repos\school-data-scraper\Freelancer_Data_Mining_Project.xlsx"
    xls = pd.ExcelFile(excel_file)
    
    all_rosters = []
    all_failed_urls = []
    
    total_schools = 0
    successful_scrapes = 0
    failed_scrapes = 0

    for sheet_name in xls.sheet_names:
        if sheet_name != "NAIA":
            continue
        print(f"\nProcessing sheet: {sheet_name}")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        sheet_rosters, sheet_failed_urls = await process_sheet(sheet_name, df)
        
        total_schools += len(df)
        successful_scrapes += len(sheet_rosters)
        failed_scrapes += len(sheet_failed_urls)

        all_rosters.extend(sheet_rosters)
        all_failed_urls.extend([(sheet_name,) + failed_url for failed_url in sheet_failed_urls if failed_url])
    
    # Print summary
    print("\n--- Scraping Summary ---")
    print(f"Total schools processed: {total_schools}")
    print(f"Successful scrapes: {successful_scrapes}")
    print(f"Failed scrapes: {failed_scrapes}")
    print(f"Success rate: {successful_scrapes/total_schools:.2%}")    
    # Combine all roster data
    if all_rosters:
        combined_roster_df = pd.concat(all_rosters, ignore_index=True)
        
        # Save to Excel
        output_filename = "scraped_roster_data.xlsx"
        combined_roster_df.to_excel(output_filename, index=False)
        print(f"All roster data saved to {output_filename}")
        
        # Save to CSV
        csv_filename = "scraped_roster_data.csv"
        combined_roster_df.to_csv(csv_filename, index=False)
        print(f"All roster data saved to {csv_filename}")
    else:
        print("No roster data was successfully scraped.")
    
    # Save failed URLs
    if all_failed_urls:
        failed_urls_df = pd.DataFrame(all_failed_urls, columns=['Sheet', 'College', 'URL', 'Reason'])
        failed_urls_excel = "failed_roster_urls.xlsx"
        failed_urls_df.to_excel(failed_urls_excel, index=False)
        print(f"Failed URLs saved to {failed_urls_excel}")
        
        with open("failed_roster_urls.txt", "w") as f:
            for sheet, college, url, reason in all_failed_urls:
                f.write(f"Sheet: {sheet}, College: {college}, URL: {url}, Reason: {reason}\n")
        print("Failed URLs also saved to failed_roster_urls.txt")
    else:
        print("No failed URLs to report.")

if __name__ == "__main__":
    asyncio.run(main())