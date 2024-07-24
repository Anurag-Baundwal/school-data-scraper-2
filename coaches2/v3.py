import os
import base64
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import time
import google.generativeai as genai
import json
from urllib.parse import urlparse
import pandas as pd
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import sys
from dotenv import load_dotenv
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from PIL import Image 
import io  
import logging
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

GEMINI_API_KEYS = os.getenv('GEMINI_API_KEYS').split(',')
OXYLABS_USERNAME = os.getenv('OXYLABS_USERNAME')
OXYLABS_PASSWORD = os.getenv('OXYLABS_PASSWORD')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
SEARCH_ENGINE_ID = os.getenv('SEARCH_ENGINE_ID')

async def take_screenshot_async(url, context, max_retries=3, initial_timeout=60000):
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    
    for attempt in range(max_retries):
        try:
            page = await context.new_page()
            await page.set_extra_http_headers({"User-Agent": user_agent})
            
            timeout = initial_timeout * (attempt + 1)  # Increase timeout for each retry
            await page.goto(url, wait_until='domcontentloaded', timeout=timeout)
            
            # Scroll the page multiple times to ensure all content is loaded
            for _ in range(10):  # Increased from 3 to 5
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(3)  # Increased from 2 to 3 seconds
            
            await page.wait_for_load_state('networkidle', timeout=timeout)
            
            screenshot = await page.screenshot(full_page=True, type='jpeg', quality=100)
            await page.close()
            return screenshot, None
        except PlaywrightTimeoutError:
            if attempt == max_retries - 1:
                return None, "Timeout error after multiple attempts"
            await asyncio.sleep(random.uniform(5, 10))  # Longer random delay between retries
        except Exception as e:
            return None, str(e)
        finally:
            if 'page' in locals():
                await page.close()
    return None, "Max retries reached"

def split_screenshot(screenshot_bytes, max_aspect_ratio=3.5):
    image = Image.open(io.BytesIO(screenshot_bytes))
    width, height = image.size
    max_height = int(width * max_aspect_ratio)
    
    if height <= max_height:
        return [screenshot_bytes]
    
    pieces = []
    for y in range(0, height, max_height):
        piece = image.crop((0, y, width, min(y + max_height, height)))
        buffer = io.BytesIO()
        piece.save(buffer, format="JPEG")
        pieces.append(buffer.getvalue())
    
    return pieces

def extract_coaching_data(screenshots):
    api_key = random.choice(GEMINI_API_KEYS)
    genai.configure(api_key=api_key)

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
    
    prompt = """
    Analyze the screenshot(s) of a coaching staff webpage and extract information ONLY for softball *coaches* (head coach and assistant coaches - sometimes you'll see interim coaches too. Include those). They will typically be found under a softball section, and will usually only be 3-4 in number. Do not include coaches from other sports or general staff members. Extract the following information for each softball coach:
    - Name
    - Title
    - Email address (if available)
    - Phone number (If available. Sometimes it will be in the section heading (eg:Softball - Phone: 828-262-7310))
    - Twitter/X handle (if available)

    Note: Phone number is always 10 digits. If some part is in the section heading and some part is in the row for the particular coach, piece together the information to find the full phone number.

    Note2: For pages which are very lengthy, their screenshots have been split into multiple pieces to keep the aspect ratio of the image in a manageable range. However, all the images given to you will still belong to one single webpage. In such cases you'll need to integillently piece together the information in the variours pieces of the screenshot to find the coaches' info.

    Determine if the scraping was successful or not. If not, provide a reason from the following options:
    - broken link (ie, 404 or page doesn't contain required data)
    - bot detection (ie, verify you're a human, captcha, that sort of stuff)
    - incomplete data (only some of the fields are present on the screen and the rest require additional clicks)
    - other 

    Format the output as a JSON string with the following structure:
    {
        "success": true/false,
        "reason": "reason for failing to scrape data" (or null if success),
        "coachingStaff": [
            {
                "name": "...",
                "title": "...",
                "email": null,
                "phone": null,
                "twitter": null
            },
            ...
        ]
    }

    If you can find any softball coaching staff information, even if incomplete, set "success" to true and include the available data. If no softball coaches are found, set "success" to false and provide the reason "no softball coaches found".

    Important: Do not surround the JSON with backticks or any other characters. The response should be a valid JSON string only.
    """
    
    image_parts = [
        {
            "mime_type": "image/jpeg",
            "data": screenshot
        }
        for screenshot in screenshots
    ]
    response = model.generate_content([prompt] + image_parts)
    return response.text

async def process_url_async(url, school, context, staff_directory_url, softball_coaches_url, max_retries=3):
    logging.info(f"Processing: {school}")
    staff_failure_reason = None
    coaches_failure_reason = None
    screenshot = None
    successful_url = None

    async def try_url(url, max_retries):
        for attempt in range(max_retries):
            try:
                screenshot_bytes, failure_reason = await take_screenshot_async(url, context)
                if screenshot_bytes:
                    return screenshot_bytes, None
                logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}. Reason: {failure_reason}")
            except Exception as e:
                logging.error(f"Error capturing screenshot for {url}: {str(e)}")
                failure_reason = str(e)
            
            if attempt < max_retries - 1:
                logging.info(f"Retrying {url}. Attempt {attempt + 2}/{max_retries}")
                await asyncio.sleep(5 * (attempt + 1))  # Exponential backoff
        return None, f"Max retries reached. Last error: {failure_reason}"

    # Try Staff Directory URL
    logging.info(f"Trying Staff Directory URL: {staff_directory_url}")
    staff_screenshot, staff_failure_reason = await try_url(staff_directory_url, max_retries)
    
    if staff_failure_reason:
        logging.warning(f"Failed Staff Directory URL: {school} - {staff_directory_url} - Reason: {staff_failure_reason}")
        # Try Coaches URL
        logging.info(f"Trying Coaches URL: {softball_coaches_url}")
        coaches_screenshot, coaches_failure_reason = await try_url(softball_coaches_url, max_retries)
        if coaches_failure_reason:
            logging.warning(f"Failed Coaches URL: {school} - {softball_coaches_url} - Reason: {coaches_failure_reason}")
        else:
            screenshot = coaches_screenshot
            successful_url = softball_coaches_url
    else:
        screenshot = staff_screenshot
        successful_url = staff_directory_url

    if screenshot:
        try:
            screenshot_pieces = split_screenshot(screenshot)
            encoded_pieces = [base64.b64encode(piece).decode('utf-8') for piece in screenshot_pieces]
            coaching_data = extract_coaching_data(encoded_pieces)
            data = json.loads(coaching_data)
            if not data["success"] or len(data["coachingStaff"]) == 0:
                reason = data.get("reason", "No softball coaches found")
                logging.warning(f"Failed: {school} - Reason: {reason}")
                return {
                    "url": url,
                    "staff_directory_url": staff_directory_url,
                    "staff_directory_reason": staff_failure_reason or reason,
                    "coaches_url": softball_coaches_url,
                    "coaches_reason": coaches_failure_reason or reason,
                    "school": school
                }, None
            else:
                coaches = []
                for staff in data["coachingStaff"]:
                    staff_data = {
                        "School": school,
                        "Coaches URL": url,
                        "Scraped URL": successful_url,
                        "Name": staff.get("name"),
                        "Title": staff.get("title"),
                        "Email": staff.get("email"),
                        "Phone": staff.get("phone"),
                        "Twitter": staff.get("twitter")
                    }
                    coaches.append(staff_data)
                logging.info(f"Success: {school} - Found {len(coaches)} softball coaches - Scraped from: {successful_url}")
                return None, coaches
        except json.JSONDecodeError as e:
            reason = f"JSON parse error: {str(e)}"
            logging.error(f"Failed: {school} - Reason: {reason}")
        except Exception as exc:
            reason = str(exc)
            logging.error(f"Failed: {school} - Reason: {reason}")
    else:
        reason = "Failed to obtain screenshot from both URLs"
        logging.error(f"Failed: {school} - Reason: {reason}")

    return {
        "url": url,
        "staff_directory_url": staff_directory_url,
        "staff_directory_reason": staff_failure_reason or reason,
        "coaches_url": softball_coaches_url,
        "coaches_reason": coaches_failure_reason or reason,
        "school": school
    }, None


# def extract_coaching_data(screenshot):
#     api_key = random.choice(GEMINI_API_KEYS)
#     genai.configure(api_key=api_key)

#     generation_config = {
#         "temperature": 0.2,
#         "top_p": 0.95,
#         "top_k": 64,
#         "max_output_tokens": 8192,
#     }
#     model = genai.GenerativeModel(
#         model_name="gemini-1.5-flash",
#         generation_config=generation_config,
#     )
    
#     prompt = """
#     Analyze the screenshot of a coaching staff webpage and extract information ONLY for softball *coaches* (head coach and assistant coaches - sometimes you'll see interim coaches too. Include those). They will typically be found under a softball section, and will usually only be 3-4 in number. Do not include coaches from other sports or general staff members. Extract the following information for each softball coach:
#     - Name
#     - Title
#     - Email address (if available)
#     - Phone number (if available. sometimes will in the section heading (eg:Softball - Phone: 828-262-7310))
#     - Twitter/X handle (if available)

#     Determine if the scraping was successful or not. If not, provide a reason from the following options:
#     - broken link (ie, 404 or page doesn't contain required data)
#     - bot detection (ie, verify you're a human, captcha, that sort of stuff)
#     - incomplete data (only some of the fields are present on the screen and the rest require additional clicks)
#     - other 

#     Format the output as a JSON string with the following structure:
#     {
#         "success": true/false,
#         "reason": "reason for failing to scrape data" (or null if success),
#         "coachingStaff": [
#             {
#                 "name": "...",
#                 "title": "...",
#                 "email": null,
#                 "phone": null,
#                 "twitter": null
#             },
#             ...
#         ]
#     }

#     If you can find any softball coaching staff information, even if incomplete, set "success" to true and include the available data. If no softball coaches are found, set "success" to false and provide the reason "no softball coaches found".

#     Important: Do not surround the JSON with backticks or any other characters. The response should be a valid JSON string only.
#     """
    
#     image_parts = [
#         {
#             "mime_type": "image/jpeg",
#             "data": base64.b64decode(screenshot)
#         }
#     ]
#     response = model.generate_content([prompt, image_parts[0]])
#     return response.text

# async def process_url_async(url, school, context, staff_directory_url, softball_coaches_url, max_retries=3):
#     print(f"Processing: {school}")
#     staff_failure_reason = None
#     coaches_failure_reason = None
#     screenshot = None
#     successful_url = None
#     staff_screenshot_path = None
#     coaches_screenshot_path = None

#     async def try_url(url, max_retries, screenshot_prefix):
#         screenshot_path = f"{screenshot_prefix}_{school.replace(' ', '_')}.png"
#         for attempt in range(max_retries):
#             try:
#                 page = await context.new_page()
#                 await page.goto(url, wait_until='networkidle', timeout=60000)
#                 await page.screenshot(path=screenshot_path, full_page=True)
#                 await page.close()
#                 return screenshot_path, None
#             except PlaywrightTimeoutError:
#                 failure_reason = "Timeout error"
#             except Exception as e:
#                 failure_reason = str(e)
            
#             print(f"Attempt {attempt + 1}/{max_retries} failed for {url}. Reason: {failure_reason}")
#             if attempt < max_retries - 1:
#                 print(f"Retrying {url}. Attempt {attempt + 2}/{max_retries}")
#                 await asyncio.sleep(5 * (attempt + 1))  # Exponential backoff
#         return screenshot_path, f"Max retries reached. Last error: {failure_reason}"

#     # Try Staff Directory URL
#     print(f"Trying Staff Directory URL: {staff_directory_url}")
#     staff_screenshot_path, staff_failure_reason = await try_url(staff_directory_url, max_retries, "staff")
    
#     if staff_failure_reason:
#         print(f"Failed Staff Directory URL: {school} - {staff_directory_url} - Reason: {staff_failure_reason}")
#         # Try Coaches URL
#         print(f"Trying Coaches URL: {softball_coaches_url}")
#         coaches_screenshot_path, coaches_failure_reason = await try_url(softball_coaches_url, max_retries, "coaches")
#         if coaches_failure_reason:
#             print(f"Failed Coaches URL: {school} - {softball_coaches_url} - Reason: {coaches_failure_reason}")
#         else:
#             screenshot = coaches_screenshot_path
#             successful_url = softball_coaches_url
#     else:
#         screenshot = staff_screenshot_path
#         successful_url = staff_directory_url

#     if screenshot:
#         try:
#             with open(screenshot, "rb") as image_file:
#                 encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
#             coaching_data = extract_coaching_data(encoded_string)
#             data = json.loads(coaching_data)
#             if not data["success"] or len(data["coachingStaff"]) == 0:
#                 reason = data.get("reason", "No softball coaches found")
#                 print(f"Failed: {school} - Reason: {reason}")
#                 return {
#                     "url": url,
#                     "staff_directory_url": staff_directory_url,
#                     "staff_directory_reason": staff_failure_reason or reason,
#                     "coaches_url": softball_coaches_url,
#                     "coaches_reason": coaches_failure_reason or reason,
#                     "school": school,
#                     "staff_screenshot": staff_screenshot_path,
#                     "coaches_screenshot": coaches_screenshot_path
#                 }, None
#             else:
#                 coaches = []
#                 for staff in data["coachingStaff"]:
#                     staff_data = {
#                         "School": school,
#                         "Coaches URL": url,
#                         "Scraped URL": successful_url,
#                         "Name": staff.get("name"),
#                         "Title": staff.get("title"),
#                         "Email": staff.get("email"),
#                         "Phone": staff.get("phone"),
#                         "Twitter": staff.get("twitter")
#                     }
#                     coaches.append(staff_data)
#                 print(f"Success: {school} - Found {len(coaches)} softball coaches - Scraped from: {successful_url}")
#                 # Delete successful screenshot
#                 os.remove(screenshot)
#                 if staff_screenshot_path and staff_screenshot_path != screenshot:
#                     os.remove(staff_screenshot_path)
#                 if coaches_screenshot_path and coaches_screenshot_path != screenshot:
#                     os.remove(coaches_screenshot_path)
#                 return None, coaches
#         except json.JSONDecodeError as e:
#             reason = f"JSON parse error: {str(e)}"
#             print(f"Failed: {school} - Reason: {reason}")
#         except Exception as exc:
#             reason = str(exc)
#             print(f"Failed: {school} - Reason: {reason}")
#     else:
#         reason = "Failed to obtain screenshot from both URLs"
#         print(f"Failed: {school} - Reason: {reason}")

#     return {
#         "url": url,
#         "staff_directory_url": staff_directory_url,
#         "staff_directory_reason": staff_failure_reason or reason,
#         "coaches_url": softball_coaches_url,
#         "coaches_reason": coaches_failure_reason or reason,
#         "school": school,
#         "staff_screenshot": staff_screenshot_path,
#         "coaches_screenshot": coaches_screenshot_path
#     }, None

def save_failed_urls(failed_urls, sheet_name):
    try:
        failed_df = pd.DataFrame(failed_urls)
        failed_df.to_excel(f"failed_urls_{sheet_name}.xlsx", index=False)
        failed_df.to_csv(f"failed_urls_{sheet_name}.csv", index=False, encoding='utf-8-sig')
        with open(f"failed_urls_{sheet_name}.txt", "w", encoding='utf-8') as f:
            for entry in failed_urls:
                f.write(f"{entry['school']} - {entry['url']}\n")
                f.write(f"  Staff Directory URL: {entry['staff_directory_url']} - Reason: {entry['staff_directory_reason']}\n")
                f.write(f"  Staff Screenshot: {entry['staff_screenshot']}\n")
                f.write(f"  Coaches URL: {entry['coaches_url']} - Reason: {entry['coaches_reason']}\n")
                f.write(f"  Coaches Screenshot: {entry['coaches_screenshot']}\n\n")
        print(f"Failed URLs for {sheet_name} saved to failed_urls_{sheet_name}.txt, failed_urls_{sheet_name}.csv, and failed_urls_{sheet_name}.xlsx")
    except Exception as e:
        print(f"Error saving failed URLs for {sheet_name}: {str(e)}")

def save_coaches_data(all_coaches, sheet_name):
    try:
        coaches_df = pd.DataFrame(all_coaches)
        output_filename_xlsx = f"scraped_coaches_data_{sheet_name}.xlsx"
        output_filename_csv = f"scraped_coaches_data_{sheet_name}.csv"
        coaches_df.to_excel(output_filename_xlsx, index=False)
        coaches_df.to_csv(output_filename_csv, index=False)
        print(f"All coaches data for {sheet_name} saved to {output_filename_xlsx} and {output_filename_csv}")
    except Exception as e:
        print(f"Error saving coaches data for {sheet_name}: {str(e)}")

async def process_batch(batch, context):
    tasks = [process_url_async(row['2024 Coaches URL'], row['School'], context, row['Staff Directory'], row['2024 Coaches URL']) for row in batch]
    results = await asyncio.gather(*tasks)
    
    all_coaches = []
    failed_urls = []
    
    for result in results:
        failed_url, coaches = result
        if failed_url:
            failed_urls.append(failed_url)
        elif coaches:
            all_coaches.extend(coaches)
    
    return all_coaches, failed_urls

async def process_sheet(sheet_name, df):
    all_coaches = []
    all_failed_urls = []
    total_processed = 0
    total_successful = 0
    total_failed = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            java_script_enabled=True,
            ignore_https_errors=True,
            bypass_csp=True
        )

        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        batch_size = 10
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size].to_dict('records')
            coaches, failed_urls = await process_batch(batch, context)
            
            all_coaches.extend(coaches)
            all_failed_urls.extend(failed_urls)
            
            total_processed += len(batch)
            total_successful += len(coaches)
            total_failed += len(failed_urls)
            
            print(f"Progress: {total_processed}/{len(df)} - Successful: {total_successful} - Failed: {total_failed}")

            # Save intermediate results
            save_coaches_data(all_coaches, f"{sheet_name}_intermediate")
            save_failed_urls(all_failed_urls, f"{sheet_name}_intermediate")

        await browser.close()

    # Save final results for the sheet
    save_coaches_data(all_coaches, sheet_name)
    save_failed_urls(all_failed_urls, sheet_name)

    return all_coaches, all_failed_urls, total_processed, total_successful, total_failed

async def main_async():
    excel_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    xls = pd.ExcelFile(excel_file)
    
    for sheet_name in xls.sheet_names:
        if sheet_name != "NCAA D1":  # process only sheet 2 for now
            continue
        print(f"Processing sheet: {sheet_name}")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        coaches, failed_urls, processed, successful, failed = await process_sheet(sheet_name, df)
        
        print(f"Finished processing sheet: {sheet_name}")
        print(f"Coaches found: {successful}")
        print(f"Failed URLs: {failed}")
        print(f"Total processed: {processed}")
        print("=" * 50)
    
    print("Scraping completed!")

if __name__ == "__main__":
    asyncio.run(main_async())