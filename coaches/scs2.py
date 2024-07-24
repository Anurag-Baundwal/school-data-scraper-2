import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import json
import re
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import google.generativeai as genai
import base64
from config import GOOGLE_API_KEY, SEARCH_ENGINE_ID, GEMINI_API_KEYS
import random
import os
import traceback
from aiohttp import ClientTimeout
from asyncio import TimeoutError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Suppress Selenium and ChromeDriver logs
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('webdriver_manager').setLevel(logging.WARNING)

# Configure Gemini
genai.configure(api_key=random.choice(GEMINI_API_KEYS))

# Configure Chrome options to reduce console output
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--log-level=3")  # Only show fatal errors
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

def load_excel_data(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        return xls
    except Exception as e:
        logging.error(f"Error loading Excel file: {e}")
        return None

async def html_based_scraping(url, school_name, max_retries=3, base_timeout=10):
    for attempt in range(max_retries):
        try:
            timeout = ClientTimeout(total=base_timeout * (attempt + 1))
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        soup = BeautifulSoup(await response.text(), 'html.parser')
                        coaches = extract_coach_info(soup)
                        return coaches, True
                    else:
                        logging.warning(f"Failed to fetch {url}: HTTP {response.status}")
        except (TimeoutError, aiohttp.ClientError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {school_name}: {str(e)}")
            if attempt == max_retries - 1:
                logging.error(f"All attempts failed for {school_name}: {str(e)}\n{traceback.format_exc()}")
                return None, False
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    return None, False

def extract_coach_info(soup):
    coaches = []
    softball_section = None
    
    for section in soup.find_all('tr', class_='sidearm-staff-category'):
        if 'Softball' in section.text:
            softball_section = section
            break
    
    if softball_section:
        current_section = softball_section.find_next_sibling('tr')
        while current_section and 'sidearm-staff-category' not in current_section.get('class', []):
            name = current_section.find('a', class_='text-no-wrap')
            title = current_section.find('td', headers=re.compile('col-staff_title'))
            phone = current_section.find('td', headers=re.compile('col-staff_phone'))
            email = current_section.find('td', headers=re.compile('col-staff_email'))
            
            coach = {
                'Name': name.text.strip() if name else '',
                'Title': title.text.strip() if title else '',
                'Phone': phone.text.strip() if phone else '',
                'Email': email.find('a').text.strip() if email and email.find('a') else '',
                'Twitter': ''  # We'll need to add logic to find Twitter handles if available
            }
            coaches.append(coach)
            current_section = current_section.find_next_sibling('tr')
    
    return coaches

async def genai_based_scraping(url, school_name):
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(url)
        await asyncio.sleep(5)  # Allow time for JavaScript to render
        screenshot = driver.get_screenshot_as_base64()
        coaches_data = await extract_coaches_data(screenshot, url, school_name)
        return coaches_data, True
    except Exception as e:
        logging.error(f"Error in GenAI-based scraping for {school_name}: {str(e)}")
        return None, False
    finally:
        driver.quit()

async def extract_coaches_data(screenshot_base64, url, school_name):
    model = genai.GenerativeModel('gemini-1.5-flash')
    
    prompt = f"""
    Analyze the screenshot of the staff directory page for {school_name} from {url}.
    Focus only on softball coaches. Extract the following information for each coach:
    - Name
    - Title
    - Phone number
    - Email address
    - Twitter handle (if available)

    Format the output as a JSON string with the following structure:
    {{
        "success": true/false,
        "reason": "reason for failure" (or null if success),
        "coaches": [
            {{
                "name": "...",
                "title": "...",
                "phone": "...",
                "email": "...",
                "twitter": "..."
            }},
            ...
        ]
    }}

    Set "success" to false if no softball coaches are found.
    """
    
    image_part = {
        "mime_type": "image/jpeg",
        "data": base64.b64decode(screenshot_base64)
    }
    
    response = await model.generate_content_async([prompt, image_part])
    return response.text

async def fallback_search(school_name):
    search_query = f"{school_name} softball coaches staff directory"
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
                            coaches, success = await genai_based_scraping(new_url, school_name)
                            if success:
                                return coaches, True
                    logging.warning(f"No valid results found for {school_name}")
                    return None, False
                else:
                    logging.error(f"Google Custom Search API request failed with status code: {response.status}")
                    return None, False
    except Exception as e:
        logging.error(f"Error in fallback search for {school_name}: {str(e)}")
        return None, False

async def process_school(school_data, pass_number):
    url = school_data['Staff Directory']
    school_name = school_data['School']

    if pd.notna(url):
        logging.info(f"Processing {school_name} (URL: {url})")
        
        try:
            if pass_number == 1:
                result, success = await html_based_scraping(url, school_name)
                method = "HTML"
            elif pass_number == 2:
                result, success = await genai_based_scraping(url, school_name)
                method = "GenAI"
            else:  # pass_number == 3
                result, success = await fallback_search(school_name)
                method = "Fallback"

            if success:
                logging.info(f"Successfully scraped data for {school_name} using {method}")
                return {
                    'school': school_name,
                    'url': url,
                    'method': method,
                    'success': True,
                    'data': result
                }
            else:
                logging.warning(f"Scraping failed for {school_name} using {method}")
                return {
                    'school': school_name,
                    'url': url,
                    'method': method,
                    'success': False,
                    'reason': 'Scraping failed after all attempts'
                }
        except Exception as e:
            logging.error(f"Unexpected error processing {school_name}: {str(e)}\n{traceback.format_exc()}")
            return {
                'school': school_name,
                'url': url,
                'method': f"Error in {method}" if 'method' in locals() else "Unknown",
                'success': False,
                'reason': str(e)
            }
    else:
        logging.warning(f"Skipping {school_name} - No URL provided")
        return {
            'school': school_name,
            'url': 'N/A',
            'method': 'N/A',
            'success': False,
            'reason': 'No URL provided'
        }

async def process_sheet(sheet_name, df):
    all_results = []

    for pass_number in range(1, 4):
        logging.info(f"\nStarting Pass {pass_number} for sheet: {sheet_name}")
        
        # Create tasks for all schools
        tasks = [process_school(row, pass_number) for _, row in df.iterrows()]
        
        # Run tasks concurrently with a limit
        pass_results = []
        for i in range(0, len(tasks), 10):  # Process in batches of 10
            batch = tasks[i:i+10]
            batch_results = await asyncio.gather(*batch)
            pass_results.extend(batch_results)

        successful_scrapes = sum(1 for r in pass_results if r['success'])
        failed_scrapes = len(pass_results) - successful_scrapes

        logging.info(f"\nResults for {sheet_name} - Pass {pass_number}:")
        logging.info(f"Successful scrapes: {successful_scrapes}")
        logging.info(f"Failed scrapes: {failed_scrapes}")

        save_results(pass_results, f"{sheet_name}_Pass{pass_number}_results.json")

        all_results.append(pass_results)

        if pass_number < 3:
            proceed = input(f"Do you want to proceed with Pass {pass_number + 1} for {sheet_name}? (y/n): ").lower()
            if proceed != 'y':
                break

    return all_results


def save_results(results, output_file):
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

async def main():
    input_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    
    xls = load_excel_data(input_file)
    if xls is not None:
        for sheet_name in xls.sheet_names:
            logging.info(f"\nProcessing sheet: {sheet_name}")
            df = pd.read_excel(xls, sheet_name=sheet_name)
            await process_sheet(sheet_name, df)
    else:
        logging.error("Failed to load Excel file. Exiting.")

if __name__ == "__main__":
    asyncio.run(main())