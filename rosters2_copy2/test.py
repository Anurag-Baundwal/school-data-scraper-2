import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import google.generativeai as genai
import random
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from config import GEMINI_API_KEYS

class APIKeyManager:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_index = 0

    def get_next_key(self):
        key = self.api_keys[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.api_keys)
        return key

api_key_manager = APIKeyManager(GEMINI_API_KEYS)

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, str):
            return obj.encode('utf-8').decode('unicode_escape')
        return super().default(obj)

async def scrape_url(url):
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36'
    ]
    
    headers = {'User-Agent': random.choice(user_agents)}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.text()
            else:
                return None

async def process_with_gemini(html_content, url):
    genai.configure(api_key=api_key_manager.get_next_key())
    model = genai.GenerativeModel('gemini-1.5-flash')
    
    prompt = f"""
    Analyze the HTML content of the college softball roster webpage from {url}. Extract the following information:
    - School name (if available)
    - Roster year
    For each player, extract:
    - Name
    - Position
    - Year (Fr, So, Jr, Sr, Grad, etc)
    - Hometown
    - High School
    - Graduation Year (calculate based on the player's year and the roster year)
    Determine the roster year. Look for an explicit mention of the roster year on the page (e.g., "2024 Softball Roster"). If not found, assume it's for the upcoming season (2025).
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
        "schoolName": "...",
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
    Important: Ensure all names, including those with non-English characters, are preserved exactly as they appear in the HTML. Do not escape or modify any special characters in names, hometowns, or school names. For example, 'Montañez' should remain as 'Montañez', not 'Monta\\u00f1ez', and "O'ahu" should remain as "O'ahu", not "O\\u2018ahu".
    The response should be a valid JSON string only, without any additional formatting or markdown syntax.
    """

    try:
        response = await model.generate_content_async([prompt, html_content])
        logger.info("Raw response from Gemini:")
        logger.info(response.text)
        
        # Remove Markdown code block syntax if present
        json_string = response.text.strip()
        if json_string.startswith("```json"):
            json_string = json_string[7:]
        if json_string.endswith("```"):
            json_string = json_string[:-3]
        
        json_string = json_string.strip()
        
        return json.loads(json_string)
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        logger.error(f"Processed JSON string: {json_string}")
        return {"success": False, "reason": "Failed to parse JSON response from Gemini"}
    except Exception as e:
        logger.error(f"Error in processing with Gemini: {e}")
        return {"success": False, "reason": str(e)}

async def main():
    url = input("Enter the URL to scrape: ")
    html_content = await scrape_url(url)
    
    if html_content:
        result = await process_with_gemini(html_content, url)
        
        if result['success']:
            cleaned_result = {
                'school': result.get('schoolName', 'Unknown'),  # Add this line
                'url': url,
                'success': True,
                'rosterYear': result['rosterYear'],
                'players': result['players']
            }
            
            with open('test_result.json', 'w', encoding='utf-8') as f:
                json.dump(cleaned_result, f, ensure_ascii=False, indent=2)
            
            print("Data has been scraped and saved to test_result.json")
        else:
            print(f"Scraping failed: {result['reason']}")
    else:
        print("Failed to fetch the webpage")

if __name__ == "__main__":
    asyncio.run(main())