# scraping_methods.py for scraping player info from college softball rosters


import asyncio
import base64
import requests
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import google.generativeai as genai
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from utils import get_random_api_key
from config import GOOGLE_API_KEY, SEARCH_ENGINE_ID
import re
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def html_based_scraping(url, college_name):
    try:
        logger.info(f"Starting to scrape {college_name} from {url}")
        
        # Use a custom User-Agent to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        logger.debug(f"Response status code: {response.status_code}")
        logger.debug(f"Response content type: {response.headers.get('Content-Type')}")
        
        if 'text/html' not in response.headers.get('Content-Type', ''):
            logger.error(f"Unexpected content type for {url}")
            return None, False
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Try to find the roster year
        roster_year = find_roster_year(soup)
        logger.info(f"Roster year found: {roster_year}")
        
        # Try to find player elements using different possible structures
        player_elements = find_player_elements(soup)
        
        if not player_elements:
            logger.error(f"No player elements found for {college_name}")
            return None, False
        
        logger.info(f"Number of players found: {len(player_elements)}")
        
        players = extract_player_data(player_elements, roster_year)
        
        if players:
            logger.info(f"Successfully extracted data for {len(players)} players")
            return {college_name: players}, True
        else:
            logger.error(f"No player data extracted from {url}")
            return None, False
    
    except requests.RequestException as e:
        logger.error(f"Request error for {college_name}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error scraping {college_name}: {str(e)}", exc_info=True)
    
    return None, False

def find_roster_year(soup):
    year_pattern = re.compile(r'(202[4-5]).*Roster')
    for text in soup.stripped_strings:
        match = year_pattern.search(text)
        if match:
            return int(match.group(1))
    return datetime.now().year + 1  # Default to next year if not found

def find_player_elements(soup):
    player_elements = soup.find_all('li', class_='sidearm-roster-player')
    if not player_elements:
        player_elements = soup.find_all('tr', class_=lambda x: x and 'roster__player' in x)
    if not player_elements:
        # Try other potential selectors
        player_elements = soup.select('.roster-players__group .table--roster tbody tr')
    return player_elements

def extract_player_data(player_elements, roster_year):
    players = []
    for player in player_elements:
        player_data = {}
        
        # Try different methods to extract data
        if 'sidearm-roster-player' in player.get('class', []):
            player_data = extract_sidearm_data(player)
        elif player.name == 'tr':
            player_data = extract_table_data(player)
        
        # Calculate graduation year
        player_data['Graduation Year'] = calculate_graduation_year(player_data.get('Year', ''), roster_year)
        
        if player_data.get('Name'):  # Only add player if we at least have a name
            players.append(player_data)
        else:
            logger.warning(f"Skipping player due to missing name: {player_data}")
    
    return players

def extract_sidearm_data(player):
    return {
        'Number': player.select_one('.sidearm-roster-player-jersey-number').text.strip() if player.select_one('.sidearm-roster-player-jersey-number') else '',
        'Name': player.select_one('.sidearm-roster-player-name h3 a').text.strip() if player.select_one('.sidearm-roster-player-name h3 a') else '',
        'Position': player.select_one('.sidearm-roster-player-position-long-short').text.strip() if player.select_one('.sidearm-roster-player-position-long-short') else '',
        'Year': player.select_one('.sidearm-roster-player-academic-year').text.strip() if player.select_one('.sidearm-roster-player-academic-year') else '',
        'Hometown': player.select_one('.sidearm-roster-player-hometown').text.strip() if player.select_one('.sidearm-roster-player-hometown') else '',
        'High School': player.select_one('.sidearm-roster-player-highschool').text.strip() if player.select_one('.sidearm-roster-player-highschool') else '',
    }

def extract_table_data(player):
    return {
        'Number': player.select_one('td:nth-of-type(1)').text.strip() if player.select_one('td:nth-of-type(1)') else '',
        'Name': player.select_one('td:nth-of-type(2) a').text.strip() if player.select_one('td:nth-of-type(2) a') else '',
        'Position': player.select_one('td:nth-of-type(3)').text.strip() if player.select_one('td:nth-of-type(3)') else '',
        'Year': player.select_one('td:nth-of-type(4)').text.strip() if player.select_one('td:nth-of-type(4)') else '',
        'Hometown': player.select_one('td:nth-of-type(7)').text.strip() if player.select_one('td:nth-of-type(7)') else '',
        'High School': player.select_one('td:nth-of-type(8)').text.strip() if player.select_one('td:nth-of-type(8)') else '',
    }

def calculate_graduation_year(year, roster_year):
    year_map = {
        'FR': 3, 'SO': 2, 'JR': 1, 'SR': 0, 'GR': 0,
        'Freshman': 3, 'Sophomore': 2, 'Junior': 1, 'Senior': 0, 'Graduate': 0,
        'Fr.': 3, 'So.': 2, 'Jr.': 1, 'Sr.': 0, 'Gr.': 0,
        'R-FR': 4, 'R-SO': 3, 'R-JR': 2, 'R-SR': 1,
        'Redshirt Freshman': 4, 'Redshirt Sophomore': 3, 'Redshirt Junior': 2, 'Redshirt Senior': 1,
    }
    
    for key, value in year_map.items():
        if key in year:
            return roster_year + value
    
    return None  # If year couldn't be determined

# def html_based_scraping(url, college_name):
#     try:
#         print(f"Scraping {college_name} from {url}")
#         response = requests.get(url, timeout=10)
#         soup = BeautifulSoup(response.content, 'html.parser')
        
#         # Try to find the roster year
#         roster_year = None
#         year_pattern = re.compile(r'(202[4-5]).*Roster')
#         for text in soup.stripped_strings:
#             match = year_pattern.search(text)
#             if match:
#                 roster_year = int(match.group(1))
#                 break
        
#         if not roster_year:
#             roster_year = datetime.now().year + 1  # Default to next year if not found
        
#         print(f"Roster year found: {roster_year}")
        
#         # Find all player elements
#         player_elements = soup.find_all('li', class_='sidearm-roster-player')
        
#         if not player_elements:
#             print(f"No player elements found for {college_name}")
#             return None, False
        
#         print(f"Number of players found: {len(player_elements)}")
        
#         players = []
#         for player in player_elements:
#             player_data = {}
            
#             # Extract jersey number
#             jersey_elem = player.select_one('.sidearm-roster-player-jersey-number')
#             player_data['Number'] = jersey_elem.text.strip() if jersey_elem else ''
            
#             # Extract name
#             name_elem = player.select_one('.sidearm-roster-player-name h3 a')
#             player_data['Name'] = name_elem.text.strip() if name_elem else ''
            
#             # Extract position
#             position_elem = player.select_one('.sidearm-roster-player-position-long-short')
#             player_data['Position'] = position_elem.text.strip() if position_elem else ''
            
#             # Extract year
#             year_elem = player.select_one('.sidearm-roster-player-academic-year')
#             player_data['Year'] = year_elem.text.strip() if year_elem else ''
            
#             # Extract hometown
#             hometown_elem = player.select_one('.sidearm-roster-player-hometown')
#             player_data['Hometown'] = hometown_elem.text.strip() if hometown_elem else ''
            
#             # Extract high school
#             highschool_elem = player.select_one('.sidearm-roster-player-highschool')
#             player_data['High School'] = highschool_elem.text.strip() if highschool_elem else ''
            
#             # Calculate graduation year
#             grad_year = calculate_graduation_year(player_data['Year'], roster_year)
#             player_data['Graduation Year'] = grad_year
            
#             players.append(player_data)
        
#         if players:
#             print(f"Successfully extracted data for {len(players)} players")
#             return {college_name: players}, True
#         else:
#             print(f"No player data extracted from {url}")
#             return None, False
    
#     except Exception as e:
#         print(f"Error scraping {college_name}: {str(e)}")
#         return None, False
    
# def calculate_graduation_year(year, roster_year):
#     year_map = {
#         'FR': 3, 'SO': 2, 'JR': 1, 'SR': 0, 'GR': 0,
#         'Freshman': 3, 'Sophomore': 2, 'Junior': 1, 'Senior': 0, 'Graduate': 0,
#         'Fr.': 3, 'So.': 2, 'Jr.': 1, 'Sr.': 0, 'Gr.': 0,
#         'R-FR': 4, 'R-SO': 3, 'R-JR': 2, 'R-SR': 1,
#         'Redshirt Freshman': 4, 'Redshirt Sophomore': 3, 'Redshirt Junior': 2, 'Redshirt Senior': 1,
#     }
    
#     for key, value in year_map.items():
#         if key in year:
#             return roster_year + value
    
#     return None  # If year couldn't be determined

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