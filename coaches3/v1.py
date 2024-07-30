import asyncio
import queue
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import json
import re
import google.generativeai as genai
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
from config import GEMINI_API_KEYS, GOOGLE_API_KEY, SEARCH_ENGINE_ID
import random

class APIKeyManager:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_index = 0

    def get_next_key(self):
        key = self.api_keys[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.api_keys)
        return key

api_key_manager = APIKeyManager(GEMINI_API_KEYS)

# # Configure Gemini
# genai.configure(api_key=random.choice(GEMINI_API_KEYS))

# Configure Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

class WebDriverPool:
    def __init__(self, max_drivers=10):
        self.max_drivers = max_drivers
        self.drivers = queue.Queue()
        self.count = 0

    def get_driver(self):
        if not self.drivers.empty():
            return self.drivers.get()
        elif self.count < self.max_drivers:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            self.count += 1
            return driver
        else:
            return self.drivers.get(block=True)

    def return_driver(self, driver):
        self.drivers.put(driver)

    def cleanup(self):
        while not self.drivers.empty():
            driver = self.drivers.get()
            driver.quit()

driver_pool = WebDriverPool(max_drivers=15)

async def load_excel_data(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        return xls
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return None

async def scroll_page(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        await asyncio.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

async def extract_relevant_html(soup):
    body = soup.find('body')
    if not body:
        return ""

    relevant_parts = []
    lines = body.prettify().split('\n')
    for i, line in enumerate(lines):
        if 'softball' in line.lower():
            # start = max(0, i - 30)
            # end = min(len(lines), i + 76)
            start = max(0, i - 10)
            end = min(len(lines), i + 16)
            part = '\n'.join(lines[start:end])
            relevant_parts.append(part)
            
            if 'coach' in part.lower():
                # end = min(len(lines), i + 100)
                end = min(len(lines), i + 50)
                part = '\n'.join(lines[start:end])
                relevant_parts[-1] = part

    return '\n'.join(relevant_parts)

async def gemini_based_scraping(url, school_name):
    driver = driver_pool.get_driver()
    try:
        driver.get(url)
        await scroll_page(driver)
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        relevant_html = await extract_relevant_html(soup)
        
        genai.configure(api_key=api_key_manager.get_next_key())
        model = genai.GenerativeModel('gemini-1.5-flash')
        # model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"""
        Analyze the HTML content of the coaching staff webpage for {school_name} and extract information ONLY for softball *coaches* (head coach and assistant coaches - sometimes you'll see interim coaches too. Include those). They will typically be found under a softball section, and will usually only be 3-4 in number. Do not include coaches from other sports or general staff members. Extract the following information for each softball coach:
        - Name
        - Title
        - Email address (if available)
        - Phone number (If available. Sometimes it will be in the section heading (eg:Softball - Phone: 828-262-7310))
        - Twitter/X handle (if available)
        Note: Phone number is always 10 digits. If some part is in the section heading and some part is in the row for the particular coach, piece together the information to find the full phone number.
        Determine if the scraping was successful or not. If not, provide a reason from the following options:
        - broken link (ie, 404 or page doesn't contain required data)
        - bot detection (ie, verify you're a human, captcha, that sort of stuff)
        - incomplete data (only some of the fields are present on the screen and the rest require additional clicks)
        - other 
        Format the output as a JSON string with the following structure:
        {{
            "success": true/false,
            "reason": "reason for failing to scrape data" (or null if success),
            "coachingStaff": [
                {{
                    "name": "...",
                    "title": "...",
                    "email": null,
                    "phone": null,
                    "twitter": null
                }},
                ...
            ]
        }}
        If you can find any softball coaching staff information, even if incomplete, set "success" to true and include the available data. If no softball coaches are found, set "success" to false and provide the reason "no softball coaches found".
        Important: Do not surround the JSON with backticks or any other characters. The response should be a valid JSON string only.
        """
        
        response = await model.generate_content_async([prompt, relevant_html])
        try:
            result = json.loads(response.text)
            return result, result['success']
        except json.JSONDecodeError:
            print(f"Failed to parse JSON from Gemini response for {school_name}")
            return None, False
    except Exception as e:
        print(f"Error in Gemini-based scraping for {school_name}: {str(e)}")
        return None, False
    finally:
        driver_pool.return_driver(driver)

async def process_school(school_data, url_column):
    url = school_data[url_column]
    school_name = school_data['School']

    if pd.notna(url):
        print(f"Processing {school_name} (URL: {url})")
        result, success = await gemini_based_scraping(url, school_name)
        if success and result:
            print(f"Successfully scraped data for {school_name}")
            return {
                'school': school_name,
                'url': url,
                'success': True,
                'data': result
            }
        else:
            print(f"Scraping failed for {school_name}")
            return {
                'school': school_name,
                'url': url,
                'success': False,
                'reason': result['reason'] if result else 'Unknown error'
            }
    else:
        print(f"Skipping {school_name} - No URL provided")
        return {
            'school': school_name,
            'url': 'N/A',
            'success': False,
            'reason': 'No URL provided'
        }

async def process_sheet(sheet_name, df):
    all_results = []

    # for url_column in ['Staff Directory', '2024 Coaches URL']:
    for url_column in ['Staff Directory']:
        print(f"\nProcessing {url_column} URLs for sheet: {sheet_name}")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            loop = asyncio.get_event_loop()
            futures = [loop.run_in_executor(executor, asyncio.run, process_school(row, url_column)) 
                       for _, row in df.iterrows()]
            results = await asyncio.gather(*futures)

        successful_scrapes = sum(1 for r in results if r['success'])
        failed_scrapes = len(results) - successful_scrapes

        print(f"\nResults for {sheet_name} - {url_column}:")
        print(f"Successful scrapes: {successful_scrapes}")
        print(f"Failed scrapes: {failed_scrapes}")

        save_results(results, f"{sheet_name}_{url_column}_results.json")
        save_failed_schools(results, f"{sheet_name}_{url_column}_failed_schools.txt")

        all_results.extend(results)

    return all_results

def save_results(results, output_file):
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

def save_failed_schools(results, output_file):
    failed_schools = [f"{r['school']}: {r['url']}" for r in results if not r['success']]
    with open(output_file, 'w') as f:
        f.write('\n'.join(failed_schools))

async def main():
    input_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    
    global api_key_manager
    api_key_manager = APIKeyManager(GEMINI_API_KEYS)

    try:
        xls = await load_excel_data(input_file)
        if xls is not None:
            for sheet_name in xls.sheet_names:
                if sheet_name != "NCAA D1":
                    continue
                print(f"\nProcessing sheet: {sheet_name}")
                df = pd.read_excel(xls, sheet_name=sheet_name)
                await process_sheet(sheet_name, df)
        else:
            print("Failed to load Excel file. Exiting.")
    finally:
        driver_pool.cleanup()

if __name__ == "__main__":
    asyncio.run(main())