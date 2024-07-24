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
from config import GEMINI_API_KEYS
import random
from concurrent.futures import ThreadPoolExecutor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Suppress Selenium and ChromeDriver logs
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('webdriver_manager').setLevel(logging.WARNING)

# Configure Gemini
genai.configure(api_key=random.choice(GEMINI_API_KEYS))

# Configure Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--log-level=3")
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

async def load_excel_data(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        return xls
    except Exception as e:
        logging.error(f"Error loading Excel file: {e}")
        return None

async def html_based_scraping(url, school_name):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    coaches = extract_softball_coaches(soup) # Modified to only get softball coach data
                    return coaches, len(coaches) > 0
                else:
                    logging.warning(f"Failed to fetch {url}: HTTP {response.status}")
                    return None, False
    except Exception as e:
        logging.error(f"Error in HTML-based scraping for {school_name}: {str(e)}")
        return None, False

def extract_softball_coaches(soup): # Modified to only get softball coach data
    """
    Extracts softball coach information from the given BeautifulSoup object.

    This function checks various HTML patterns to accommodate different website structures.
    """
    coaches = []

    # 1. Search for section headings containing "Softball"
    softball_section = soup.find(lambda tag: tag.name == 'h3' and 'Softball' in tag.text)
    if softball_section:
        # Find the nearest table following the heading
        table = softball_section.find_next_sibling('div', class_='table-wrap').find('table')
        if table:
            coaches.extend(extract_coaches_from_table(table))

    # 2. Search for div elements containing "Softball"
    softball_div = soup.find('div', class_='bg-primary', text=lambda text: 'Softball' in text if text else False)
    if softball_div:
        table = softball_div.find_next_sibling('div', class_='s-table__wrapper').find('table')
        if table:
            coaches.extend(extract_coaches_from_table(table))

    return coaches

def extract_coaches_from_table(table):
    coaches = []
    for row in table.find_all('tr'):
        coach = extract_coach_from_row(row)
        if coach:
            coaches.append(coach)
    return coaches

def extract_coach_from_row(row):
    cells = row.find_all('td')
    if len(cells) >= 3: # Modified to handle empty cells
        name_cell = cells[0]
        name = name_cell.text.strip() if name_cell else ''
        title_cell = cells[1]
        title = title_cell.text.strip() if title_cell else ''
        phone_cell = cells[2]
        phone = clean_phone_number(phone_cell.text.strip()) if phone_cell else ''
        email_cell = cells[3] if len(cells) > 3 else None
        email = extract_email_from_cell(email_cell)
        return {
            'Name': name,
            'Title': title,
            'Phone': phone,
            'Email': email,
            'Twitter': ''  # Add logic to find Twitter handles if available
        }
    return None


def extract_email_from_cell(cell):
    if cell:
        email_link = cell.find('a', href=lambda href: href and 'mailto:' in href)
        if email_link:
            return email_link['href'].replace('mailto:', '').strip()
    return ''

def clean_phone_number(phone):
    return re.sub(r'\D', '', phone)




async def genai_based_scraping(url, school_name):
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(url)
        await asyncio.sleep(5)  # Allow time for JavaScript to render
        screenshot = driver.get_screenshot_as_base64()
        coaches_data = await extract_coaches_data(screenshot, url, school_name)
        return coaches_data, len(coaches_data) > 0 if coaches_data else False
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

    If no softball coaches are found, return an empty list for "coaches".
    """
    
    image_part = {
        "mime_type": "image/jpeg",
        "data": base64.b64decode(screenshot_base64)
    }
    
    response = await model.generate_content_async([prompt, image_part])
    try:
        coaches_data = json.loads(response.text)
        return coaches_data.get('coaches', [])
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON from Gemini response for {school_name}")
        return []

async def process_school(school_data, pass_number):
    url = school_data['Staff Directory']
    school_name = school_data['School']

    if pd.notna(url):
        logging.info(f"Processing {school_name} (URL: {url})")
        
        if pass_number == 1:
            result, success = await html_based_scraping(url, school_name)
            method = "HTML"
        else:  # pass_number == 2
            result, success = await genai_based_scraping(url, school_name)
            method = "GenAI"

        if success and result:
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
                'reason': 'Scraping failed or no data found'
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

    for pass_number in range(1, 3):  # Only two passes now
        logging.info(f"\nStarting Pass {pass_number} for sheet: {sheet_name}")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            loop = asyncio.get_event_loop()
            futures = [loop.run_in_executor(executor, asyncio.run, process_school(row, pass_number)) 
                       for _, row in df.iterrows()]
            pass_results = await asyncio.gather(*futures)

        successful_scrapes = sum(1 for r in pass_results if r['success'])
        failed_scrapes = len(pass_results) - successful_scrapes

        logging.info(f"\nResults for {sheet_name} - Pass {pass_number}:")
        logging.info(f"Successful scrapes: {successful_scrapes}")
        logging.info(f"Failed scrapes: {failed_scrapes}")

        save_results(pass_results, f"{sheet_name}_Pass{pass_number}_results.json")

        all_results.append(pass_results)

        if pass_number == 1:
            proceed = input(f"Do you want to proceed with Pass 2 (GenAI-based scraping) for {sheet_name}? (y/n): ").lower()
            if proceed != 'y':
                break

    return all_results

def save_results(results, output_file):
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

async def main():
    input_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    
    xls = await load_excel_data(input_file)
    if xls is not None:
        for sheet_name in xls.sheet_names:
            logging.info(f"\nProcessing sheet: {sheet_name}")
            df = pd.read_excel(xls, sheet_name=sheet_name)
            await process_sheet(sheet_name, df)
    else:
        logging.error("Failed to load Excel file. Exiting.")

if __name__ == "__main__":
    asyncio.run(main())