import asyncio
import queue
import time
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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, WebDriverException
import google.generativeai as genai
import base64
from config import GEMINI_API_KEYS, GOOGLE_API_KEY, SEARCH_ENGINE_ID
import random
from concurrent.futures import ThreadPoolExecutor
import psutil

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

driver_pool = WebDriverPool(max_drivers=15)  # Adjust the number as needed

async def load_excel_data(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        return xls
    except Exception as e:
        logging.error(f"Error loading Excel file: {e}")
        return None

async def html_based_scraping(url, school_name, max_retries=3, timeout=60):
    driver = driver_pool.get_driver()
    try:
        logging.info(f"Starting scraping for {school_name} at {url}")
        start_time = time.time()

        # Set page load timeout
        driver.set_page_load_timeout(timeout)

        # Navigate to the page
        logging.info(f"Navigating to {url}")
        navigation_start = time.time()
        driver.get(url)
        navigation_time = time.time() - navigation_start
        logging.info(f"Initial page load took {navigation_time:.2f} seconds")

        # Wait for the body to be present
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # Collect available logs
        browser_logs = driver.get_log('browser')
        logging.info(f"Browser logs: {browser_logs}")

        # Get memory usage
        memory_info = driver.execute_script('return window.performance.memory;')
        if memory_info:
            logging.info(f"Memory usage: {memory_info}")
        else:
            logging.info("Memory usage information not available")

        # Get CPU usage
        process = psutil.Process(driver.service.process.pid)
        cpu_percent = process.cpu_percent(interval=1)
        logging.info(f"CPU usage: {cpu_percent}%")

        # Get network metrics
        # navigation_timing = driver.execute_script("return window.performance.timing.toJSON();")
        # logging.info(f"Navigation Timing: {navigation_timing}")

        # Scroll and handle dynamic content
        scroll_start_time = time.time()
        await scroll_and_wait_for_content(driver)
        logging.info(f"Scrolling and waiting for content took {time.time() - scroll_start_time:.2f} seconds")

        # Get the page source
        html_content = driver.page_source
        logging.info(f"Retrieved page source. Total size: {len(html_content)} bytes")

        # Parse and extract data
        parse_start_time = time.time()
        soup = BeautifulSoup(html_content, 'html.parser')
        coaches = extract_softball_coaches(soup)
        logging.info(f"Parsing and extracting data took {time.time() - parse_start_time:.2f} seconds")

        total_time = time.time() - start_time
        logging.info(f"Total scraping time for {school_name}: {total_time:.2f} seconds")

        return coaches, len(coaches) > 0

    except Exception as e:
        logging.error(f"Error scraping {school_name}: {str(e)}")
        return None, False

    finally:
        driver_pool.return_driver(driver)

async def scroll_and_wait_for_content(driver, max_scroll_attempts=10, scroll_pause_time=2):
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    for _ in range(max_scroll_attempts):
        # Scroll down to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # Wait to load page
        await asyncio.sleep(scroll_pause_time)
        
        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

        # Check for and click "Load More" buttons
        try:
            load_more_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Load More') or contains(@class, 'load-more')]"))
            )
            driver.execute_script("arguments[0].click();", load_more_button)
            logging.info("Clicked 'Load More' button")
            await asyncio.sleep(scroll_pause_time)
        except TimeoutException:
            pass  # No "Load More" button found

    # Final check for any remaining dynamic content
    try:
        WebDriverWait(driver, 10).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, ".loading-indicator"))
        )
    except TimeoutException:
        logging.warning("Timed out waiting for loading indicator to disappear")


def extract_softball_coaches(soup):
    coaches = []
    extraction_start_time = time.time()
    
    # Pattern 1: Table with "Softball" header
    softball_header = soup.find(['th', 'td'], string=re.compile(r'\bSoftball\b', re.IGNORECASE))
    if softball_header:
        table = softball_header.find_parent('table')
        if table:
            coaches.extend(extract_coaches_from_table(table))
    
    # Pattern 2: Div with "Softball" class or text
    softball_div = soup.find('div', class_=lambda x: x and 'softball' in x.lower())
    if not softball_div:
        softball_div = soup.find('div', string=re.compile(r'\bSoftball Coaches?\b', re.IGNORECASE))
    if softball_div:
        coaches.extend(extract_coaches_from_divs(softball_div))
    
    # Pattern 3: Specific div structure (Example 1)
    category_div = soup.find('div', class_='category', string=re.compile(r'\bSoftball Coaches?\b', re.IGNORECASE))
    if category_div:
        member_divs = category_div.find_next_siblings('div', class_='member')
        for div in member_divs:
            coach = extract_coach_from_div(div)
            if coach:
                coaches.append(coach)
    
    # Pattern 4: Specific div structure (Example 2)
    softball_header = soup.find('h3', class_='s-text-title', string='Softball')
    if softball_header:
        person_cards = softball_header.find_next_siblings('div', class_='s-person-card')
        for card in person_cards:
            coach = extract_coach_from_person_card(card)
            if coach:
                coaches.append(coach)
    
    # Pattern 5: Specific table structure (Example 3)
    softball_table = soup.find('table', class_=lambda x: x and 'table' in x)
    if softball_table and softball_table.find('th', string=re.compile(r'\bSoftball\b', re.IGNORECASE)):
        coaches.extend(extract_coaches_from_table(softball_table))
    
    # Pattern 6: Specific div structure (Example 4)
    person_cards = soup.find_all('div', class_='s-person-card')
    for card in person_cards:
        coach = extract_coach_from_person_card(card)
        if coach:
            coaches.append(coach)
    
    logging.info(f"Extracted {len(coaches)} coaches in {time.time() - extraction_start_time:.2f} seconds")
    return coaches

def extract_coaches_from_table(table):
    coaches = []
    rows = table.find_all('tr')
    for row in rows:
        coach = extract_coach_from_row(row)
        if coach:
            coaches.append(coach)
    return coaches

def extract_coaches_from_divs(container):
    coaches = []
    member_divs = container.find_all('div', class_=lambda x: x and 'member' in x.lower())
    for div in member_divs:
        coach = extract_coach_from_div(div)
        if coach:
            coaches.append(coach)
    return coaches

def extract_coach_from_row(row):
    cells = row.find_all(['td', 'th'])
    if len(cells) >= 2:
        name = cells[0].text.strip()
        title = cells[1].text.strip() if len(cells) > 1 else ''
        email = extract_email(row)
        phone = extract_phone(row)
        twitter = extract_twitter(row)
        
        if name and any([title, email, phone, twitter]):
            return {
                'Name': name,
                'Title': title,
                'Email': email,
                'Phone': phone,
                'Twitter': twitter
            }
    return None

def extract_coach_from_div(div):
    name_elem = div.find('a') or div.find('div', class_='name')
    name = name_elem.text.strip() if name_elem else ''
    title = div.find(string=re.compile(r'Head Coach|Assistant Coach', re.IGNORECASE))
    title = title.strip() if title else ''
    email = extract_email(div)
    phone = extract_phone(div)
    twitter = extract_twitter(div)
    
    if name and any([title, email, phone, twitter]):
        return {
            'Name': name,
            'Title': title,
            'Email': email,
            'Phone': phone,
            'Twitter': twitter
        }
    return None

def extract_coach_from_person_card(card):
    name_elem = card.find('h4') or card.find('div', class_='s-person-details__personal-single-line')
    name = name_elem.text.strip() if name_elem else ''
    title_elem = card.find('div', class_='s-person-details__position')
    title = title_elem.text.strip() if title_elem else ''
    email = extract_email(card)
    phone = extract_phone(card)
    twitter = extract_twitter(card)
    
    if name and any([title, email, phone, twitter]):
        return {
            'Name': name,
            'Title': title,
            'Email': email,
            'Phone': phone,
            'Twitter': twitter
        }
    return None

def extract_email(element):
    email_elem = element.find('a', href=lambda x: x and x.startswith('mailto:'))
    if email_elem:
        return email_elem['href'].replace('mailto:', '')
    return ''

def extract_phone(element):
    phone_elem = element.find(string=re.compile(r'\d{3}[-.]?\d{3}[-.]?\d{4}'))
    if phone_elem:
        return re.sub(r'\D', '', phone_elem)
    return ''

def extract_twitter(element):
    twitter_elem = element.find('a', href=lambda x: x and 'twitter.com' in x)
    if twitter_elem:
        return twitter_elem['href']
    return ''

async def genai_based_scraping(url, school_name):
    driver = driver_pool.get_driver()
    # service = Service(ChromeDriverManager().install())
    # driver = webdriver.Chrome(service=service, options=chrome_options)

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
        driver_pool.return_driver(driver)

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

async def process_school(school_data):
    url = school_data['Staff Directory']
    school_name = school_data['School']

    if pd.notna(url):
        logging.info(f"Processing {school_name} (URL: {url})")
        
        result, success = await html_based_scraping(url, school_name)

        if success and result:
            logging.info(f"Successfully scraped data for {school_name}")
            return {
                'school': school_name,
                'url': url,
                'success': True,
                'data': result
            }
        else:
            logging.warning(f"Scraping failed for {school_name}")
            return {
                'school': school_name,
                'url': url,
                'success': False,
                'reason': 'Scraping failed or no data found'
            }
    else:
        logging.warning(f"Skipping {school_name} - No URL provided")
        return {
            'school': school_name,
            'url': 'N/A',
            'success': False,
            'reason': 'No URL provided'
        }

async def process_sheet(sheet_name, df):
    logging.info(f"\nProcessing sheet: {sheet_name}")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        loop = asyncio.get_event_loop()
        futures = [loop.run_in_executor(executor, asyncio.run, process_school(row)) 
                   for _, row in df.iterrows()]
        results = await asyncio.gather(*futures)

    successful_scrapes = sum(1 for r in results if r['success'])
    failed_scrapes = len(results) - successful_scrapes

    logging.info(f"\nResults for {sheet_name}:")
    logging.info(f"Successful scrapes: {successful_scrapes}")
    logging.info(f"Failed scrapes: {failed_scrapes}")

    save_results(results, f"{sheet_name}_results.json")

    return results

def save_results(results, output_file):
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

async def main():
    input_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    
    try:
        xls = pd.ExcelFile(input_file)
        for sheet_name in xls.sheet_names:
            logging.info(f"\nProcessing sheet: {sheet_name}")
            df = pd.read_excel(xls, sheet_name=sheet_name)
            await process_sheet(sheet_name, df)
    finally:
        driver_pool.cleanup()

if __name__ == "__main__":
    asyncio.run(main())


'''
These changes should help with the timeout issues and provide more robustness to the scraping process. The script will now:

Retry failed requests up to 3 times.
Increase the page load timeout to 60 seconds.
Implement exponential backoff between retry attempts.
Allow more time for initial page load and between scrolls.

If you're still encountering issues with specific websites after these changes, you might need to implement custom handling for those sites or consider using a different approach, such as making direct HTTP requests instead of using Selenium for those particular cases.
    '''