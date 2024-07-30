import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import json
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor
from config import GEMINI_API_KEYS, GOOGLE_API_KEY, SEARCH_ENGINE_ID
import random
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APIKeyManager:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_index = 0

    def get_next_key(self):
        key = self.api_keys[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.api_keys)
        return key

api_key_manager = APIKeyManager(GEMINI_API_KEYS)

async def load_excel_data(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        return xls
    except Exception as e:
        logger.error(f"Error loading Excel file: {e}")
        return None

async def gemini_based_scraping(url, school_name, nickname):
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36'
    ]
    
    headers = {'User-Agent': random.choice(user_agents)}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')
                else:
                    logger.warning(f"Failed to fetch {url}. Status code: {response.status}")
                    return None, False, 0, 0
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None, False, 0, 0

    try:
        current_year = datetime.now().year
        genai.configure(api_key=api_key_manager.get_next_key())
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        prompt = f"""
        Analyze the HTML content of the college softball roster webpage from {url}. The expected school name is "{school_name}" and the team nickname or name should be related to "{nickname}". Focus ONLY on player information, ignoring any coach or staff data that might be present. Extract the following information for each player:
        - Name
        - Position
        - Year (Fr, So, Jr, Sr, Grad, etc)
        - Hometown
        - High School
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
        Important: Do not surround the JSON with backticks or any other characters. The response should be a valid JSON string only.
        """

        token_response = model.count_tokens(prompt + str(soup))
        input_tokens = token_response.total_tokens

        response = await model.generate_content_async([prompt, str(soup)])
        
        output_tokens = model.count_tokens(response.text).total_tokens

        try:
            result = json.loads(response.text)
            return result, result['success'], input_tokens, output_tokens
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON from Gemini response for {school_name}")
            return None, False, input_tokens, output_tokens
    except Exception as e:
        logger.error(f"Error in Gemini-based scraping for {school_name}: {str(e)}")
        return None, False, 0, 0

async def process_school(school_data, url_column):
    url = school_data[url_column]
    school_name = school_data['School']
    nickname = school_data.get('Nickname', '')  # Assuming there's a 'Nickname' column
    max_retries = 3
    base_delay = 5  # seconds

    if pd.notna(url):
        for attempt in range(max_retries):
            try:
                logger.info(f"Processing {school_name} (URL: {url}) - Attempt {attempt + 1}")
                result, success, input_tokens, output_tokens = await gemini_based_scraping(url, school_name, nickname)
                total_tokens = input_tokens + output_tokens
                logger.info(f"Tokens used for {school_name} {url_column}: {total_tokens}")
                
                if success and result:
                    logger.info(f"Successfully scraped data for {school_name}")
                    return {
                        'school': school_name,
                        'url': url,
                        'success': True,
                        'data': result,
                        'input_tokens': input_tokens,
                        'output_tokens': output_tokens,
                        'total_tokens': total_tokens
                    }
                else:
                    logger.warning(f"Scraping failed for {school_name} - Attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)  # Exponential backoff
                        await asyncio.sleep(delay)
                    else:
                        return {
                            'school': school_name,
                            'url': url,
                            'success': False,
                            'reason': result['reason'] if result else 'Unknown error after retries',
                            'input_tokens': input_tokens,
                            'output_tokens': output_tokens,
                            'total_tokens': total_tokens
                        }
            except Exception as e:
                logger.error(f"Error in processing {school_name}: {str(e)} - Attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    await asyncio.sleep(delay)
                else:
                    return {
                        'school': school_name,
                        'url': url,
                        'success': False,
                        'reason': f'Exception: {str(e)}',
                        'input_tokens': 0,
                        'output_tokens': 0,
                        'total_tokens': 0
                    }
    else:
        logger.info(f"Skipping {school_name} - No URL provided")
        return {
            'school': school_name,
            'url': 'N/A',
            'success': False,
            'reason': 'No URL provided',
            'input_tokens': 0,
            'output_tokens': 0,
            'total_tokens': 0
        }

async def process_sheet(sheet_name, df):
    all_results = []
    total_tokens_used = 0

    semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests

    async def process_with_semaphore(row, url_column):
        async with semaphore:
            return await process_school(row, url_column)

    roster_url_column = next((col for col in df.columns if 'Roster URL' in col), None)
    if roster_url_column:
        logger.info(f"\nProcessing {roster_url_column} URLs for sheet: {sheet_name}")
        
        tasks = [process_with_semaphore(row, roster_url_column) for _, row in df.iterrows()]
        results = await asyncio.gather(*tasks)

        successful_scrapes = sum(1 for r in results if r['success'])
        failed_scrapes = len(results) - successful_scrapes
        tokens_used = sum(r['total_tokens'] for r in results)
        total_tokens_used += tokens_used

        logger.info(f"\nResults for {sheet_name} - {roster_url_column}:")
        logger.info(f"Successful scrapes: {successful_scrapes}")
        logger.info(f"Failed scrapes: {failed_scrapes}")
        logger.info(f"Tokens used: {tokens_used}")

        save_results(results, f"{sheet_name}_{roster_url_column}_results.json")
        save_failed_schools(results, f"{sheet_name}_{roster_url_column}_failed_schools.txt")

        all_results.extend(results)
    else:
        logger.warning(f"No 'Roster URL' column found in sheet: {sheet_name}")

    logger.info(f"\nTotal tokens used for {sheet_name}: {total_tokens_used}")
    return all_results, total_tokens_used

def save_results(results, output_file):
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

def save_failed_schools(results, output_file):
    failed_schools = [f"{r['school']}: {r['url']}" for r in results if not r['success']]
    with open(output_file, 'w') as f:
        f.write('\n'.join(failed_schools))

async def main():
    global api_key_manager
    input_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    
    api_key_manager = APIKeyManager(GEMINI_API_KEYS)
    total_tokens_used = 0
    
    try:
        xls = await load_excel_data(input_file)
        if xls is not None:
            for sheet_name in xls.sheet_names:
                # if sheet_name != "NCAA D1":
                #     continue
                logger.info(f"\nProcessing sheet: {sheet_name}")
                df = pd.read_excel(xls, sheet_name=sheet_name)
                _, sheet_tokens = await process_sheet(sheet_name, df)
                total_tokens_used += sheet_tokens
            logger.info(f"\nTotal tokens used across all sheets: {total_tokens_used}")
        else:
            logger.error("Failed to load Excel file. Exiting.")
    except Exception as e:
        logger.error(f"An error occurred in the main function: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())

# fails for 16 urls out of 309 in sheet 1
# will try more retries (5 instead of 3) and lower model temp in v1_fixed.py
# also going to start properly storing reason for failure in txt and json outputng (store reasons for all 3 attempts?)
# also going to replace double curly braces {{}} with single {} in prompt for gemini

# note: v2 is inferior to this because it is very very slow. it's only called v2 because it was the second script generated by claude in the chat
# https://claude.ai/chat/ad80454f-ac97-48d6-b764-fdedb4b2961d

# 