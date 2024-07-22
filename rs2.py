import base64
import asyncio
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import aiohttp
from ssl import SSLError
import sys
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import google.generativeai as genai
import json
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException
import time
import os

# Load environment variables (assuming you're using python-dotenv)
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEYS = os.getenv('GEMINI_API_KEYS').split(',')
OXYLABS_USERNAME = os.getenv('OXYLABS_USERNAME')
OXYLABS_PASSWORD = os.getenv('OXYLABS_PASSWORD')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
SEARCH_ENGINE_ID = os.getenv('SEARCH_ENGINE_ID')

def get_random_api_key():
    return random.choice(GEMINI_API_KEYS)

async def html_based_scraping(url):
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        player_selectors = [
            'li.sidearm-roster-player',
            'div.roster_player',
            'tr.roster_row'
        ]
        
        players = []
        for selector in player_selectors:
            players = soup.select(selector)
            if players:
                break
        
        if not players:
            print(f"No players found using HTML selectors for {url}")
            return None, False
        
        roster_data = []
        for player in players:
            player_data = {}
            
            name_selectors = ['.sidearm-roster-player-name', '.name', 'td:nth-of-type(2)']
            for selector in name_selectors:
                name_elem = player.select_one(selector)
                if name_elem:
                    player_data['Name'] = name_elem.text.strip()
                    break
            
            # Add similar multi-selector attempts for other fields (position, year, hometown, etc.)
            
            if player_data:
                roster_data.append(player_data)
        
        if roster_data:
            return pd.DataFrame(roster_data), True
        else:
            print(f"No player data extracted from {url}")
            return None, False
    
    except Exception as e:
        print(f"HTML-based scraping failed for {url}: {str(e)}")
        return None, False

async def take_full_screenshot(driver, url):
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

async def extract_roster_data(screenshot_base64, url, college_name, nickname):
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
    Analyze the screenshot of a college softball roster webpage from {url}. The expected school name is "{college_name}" and the team nickname or name should be related to "{nickname}". Focus ONLY on player information, ignoring any coach or staff data that might be present. Extract the following information for each player:
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
    4. The school name or team name/nickname on the page doesn't match the expected "{college_name}" or "{nickname}"

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

async def genai_based_scraping(url, college_name, nickname):
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

    try:
        screenshot = await take_full_screenshot(driver, url)
        if screenshot:
            roster_data = await extract_roster_data(screenshot, url, college_name, nickname)
            cleaned_json = roster_data.strip().lstrip('```json').rstrip('```').strip()
            data = json.loads(cleaned_json)

            if data['success']:
                df = pd.DataFrame(data['players'])
                return df, True
            else:
                print(f"GenAI-based scraping failed: {data['reason']}")
                return None, False
        else:
            print(f"Failed to capture screenshot for {url}")
            return None, False
    except Exception as e:
        print(f"Error in GenAI-based scraping for {url}: {str(e)}")
        return None, False
    finally:
        driver.quit()

async def fallback_search(college_name, nickname):
    search_query = f"{college_name} {nickname} softball roster"
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        'key': GOOGLE_API_KEY,
        'cx': SEARCH_ENGINE_ID,
        'q': search_query
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'items' in data:
                        for item in data['items']:
                            new_url = item['link']
                            df, success = await genai_based_scraping(new_url, college_name, nickname)
                            if success:
                                return df, True
                    print(f"No valid results found for {college_name}")
                    return None, False
                else:
                    print(f"Google Custom Search API request failed with status code: {response.status}")
                    return None, False
    except Exception as e:
        print(f"Error in fallback search for {college_name}: {str(e)}")
        return None, False

def save_results(results, sheet_name, pass_number, excel_file):
    # Save detailed results to JSON
    json_filename = f"{sheet_name}_Pass{pass_number}_results.json"
    with open(json_filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Save tabular data to Excel
    df = pd.DataFrame(results['scraped_data'])
    pass_sheet_name = f"{sheet_name}_Pass{pass_number}"
    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=pass_sheet_name, index=False)
    
    # Save summary statistics and failed URLs to text file
    txt_filename = f"{sheet_name}_Pass{pass_number}_summary.txt"
    with open(txt_filename, "w") as f:
        f.write(f"Results of Pass {pass_number} for {sheet_name}\n")
        f.write(f"Number of rows for which scraping succeeded: {results['success_count']}/{results['total_count']}\n\n")
        f.write("Failed URLs:\n")
        for failed in results['failed_urls']:
            f.write(f"School: {failed['school']}, URL: {failed['url']}, Reason: {failed['reason']}\n")

async def process_college(college_data, pass_number):
    url = college_data['2024 Roster URL']
    college_name = college_data['School']
    nickname = college_data['Nickname']

    if pd.notna(url):
        print(f"\nProcessing {college_name} (URL: {url})")
        if pass_number == 1:
            result_df, success = await html_based_scraping(url)
            method = "HTML"
        elif pass_number == 2:
            result_df, success = await genai_based_scraping(url, college_name, nickname)
            method = "GenAI"
        else:  # pass_number == 3
            result_df, success = await fallback_search(college_name, nickname)
            method = "Fallback"

        if success:
            print(f"Successfully scraped data for {college_name} using {method}")
            return {
                'school': college_name,
                'url': url,
                'method': method,
                'success': True,
                'data': result_df.to_dict('records') if isinstance(result_df, pd.DataFrame) else []
            }
        else:
            print(f"Scraping failed for {college_name} using {method}")
            return {
                'school': college_name,
                'url': url,
                'method': method,
                'success': False,
                'reason': 'Scraping failed'
            }
    else:
        print(f"Skipping {college_name} - No URL provided")
        return {
            'school': college_name,
            'url': 'N/A',
            'method': 'N/A',
            'success': False,
            'reason': 'No URL provided'
        }

async def process_sheet(sheet_name, df, excel_file):
    all_results = []

    for pass_number in range(1, 4):
        print(f"\nStarting Pass {pass_number} for sheet: {sheet_name}")
        
        pass_results = {
            'scraped_data': [],
            'failed_urls': [],
            'success_count': 0,
            'total_count': len(df)
        }
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(asyncio.run, process_college(row, pass_number))  
                   for _, row in df.iterrows()]

            for future in as_completed(futures):
                result = future.result()
                if result['success']:
                    pass_results['scraped_data'].extend(result['data'])
                    pass_results['success_count'] += 1
                else:
                    pass_results['failed_urls'].append({
                        'school': result['school'],
                        'url': result['url'],
                        'reason': result.get('reason', 'Unknown')
                    })

        # Save results for this pass
        save_results(pass_results, sheet_name, pass_number, excel_file)

        # Display stats in terminal
        print(f"\nResults for {sheet_name} - Pass {pass_number}:")
        print(f"Successful scrapes: {pass_results['success_count']}")
        print(f"Failed scrapes: {len(pass_results['failed_urls'])}")

        all_results.append(pass_results)

        if pass_number < 3:
            proceed = input(f"Do you want to proceed with Pass {pass_number + 1} for {sheet_name}? (y/n): ").lower()
            if proceed != 'y':
                break

    return all_results

async def main():
    excel_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    xls = pd.ExcelFile(excel_file)
    
    all_results = {}
    
    for sheet_name in xls.sheet_names:
        print(f"\nProcessing sheet: {sheet_name}")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        sheet_results = await process_sheet(sheet_name, df, excel_file)

if __name__ == "__main__":
    asyncio.run(main())