# fix counting issue in v8_
# script for visually scraping coaching staff data using Gemini 1.5 Flash

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

async def take_screenshot_async(url, context, max_retries=10):
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    
    for attempt in range(max_retries):
        try:
            page = await context.new_page()
            await page.set_extra_http_headers({"User-Agent": user_agent})
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await asyncio.sleep(random.uniform(3, 8))  # Random delay
            await page.wait_for_load_state('networkidle', timeout=40000)
            
            # Scroll the page to ensure all content is loaded
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)  # Wait for any lazy-loaded content
            
            screenshot = await page.screenshot(full_page=True, type='jpeg', quality=100)
            screenshot_file = f"screenshot_{url.split('/')[-1]}.jpeg"
            with open(screenshot_file, 'wb') as f:
                f.write(screenshot)
            await page.close()
            return base64.b64encode(screenshot).decode('utf-8'), None
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

async def process_url_async(url, school, context, staff_directory_url, softball_coaches_url):
    print(f"Processing: {school} - {url}")
    try:
        # Try scraping from Staff Directory URL
        screenshot, failure_reason = await take_screenshot_async(staff_directory_url, context)
        if failure_reason:
            # If scraping fails, try Softball Coaches URL
            print(f"Failed: {school} - {staff_directory_url} - Reason: {failure_reason}")
            screenshot, failure_reason = await take_screenshot_async(softball_coaches_url, context)
            if failure_reason:
                # If scraping fails again, mark as failed URL
                print(f"Failed: {school} - {softball_coaches_url} - Reason: {failure_reason}")
                return {"url": url, "reason": failure_reason, "school": school}, None
        
        # Extract coaching data from screenshot
        coaching_data = await extract_coaching_data_async(screenshot)
        
        try:
            data = json.loads(coaching_data)
        except json.JSONDecodeError as e:
            print(f"Failed: {school} - {url} - Reason: JSON parse error")
            return {"url": url, "reason": f"JSON parse error: {str(e)}", "school": school}, None

        if not data["success"] or len(data["coachingStaff"]) == 0:
            reason = data.get("reason", "No softball coaches found")
            print(f"Failed: {school} - {url} - Reason: {reason}")
            return {"url": url, "reason": reason, "school": school}, None

        coaches = []
        for staff in data["coachingStaff"]:
            staff_data = {
                "School": school,
                "Coaches URL": url,
                "Name": staff.get("name"),
                "Title": staff.get("title"),
                "Email": staff.get("email"),
                "Phone": staff.get("phone"),
                "Twitter": staff.get("twitter")
            }
            coaches.append(staff_data)
        
        print(f"Success: {school} - Found {len(coaches)} softball coaches")
        return None, coaches
    
    except Exception as e:
        print(f"Unexpected error: {school} - {url} - {str(e)}")
        return {"url": url, "reason": f"Unexpected error: {str(e)}", "school": school}, None


async def extract_coaching_data_async(screenshot_base64):
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
    
    image_parts = [
        {
            "mime_type": "image/jpeg",
            "data": base64.b64decode(screenshot_base64)
        }
    ]
    
    prompt = """
    Analyze the screenshot of a coaching staff webpage and extract information ONLY for softball coaches. Do not include coaches from other sports or general staff members. Extract the following information for each softball coach:
    - Name
    - Title (must contain 'softball' or be a clear softball coaching position)
    - Email address (if available)
    - Phone number (if available)
    - Twitter/X handle (if available)

    Determine if the scraping was successful or not. If not, provide a reason from the following options:
    - broken link (ie, 404 or page doesn't contain required data)
    - bot detection (ie, verify you're a human, captcha, that sort of stuff)
    - incomplete data (only some of the fields are present on the screen and the rest require additional clicks)
    - no softball coaches found
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
    
    response = await asyncio.to_thread(model.generate_content, [prompt, image_parts[0]])
    return response.text


def save_failed_urls(failed_urls):
    try:
        failed_df = pd.DataFrame(failed_urls)
        failed_df.to_excel("failed_urls.xlsx", index=False)
        failed_df.to_csv("failed_urls.csv", index=False)
        with open("failed_urls.txt", "w") as f:
            for entry in failed_urls:
                f.write(f"{entry['url']} - {entry['reason']}\n")
        print(f"Failed URLs saved to failed_urls.txt, failed_urls.csv, and failed_urls.xlsx")
    except Exception as e:
        print(f"Error saving failed URLs: {str(e)}")

def save_coaches_data(all_coaches):
    try:
        coaches_df = pd.DataFrame(all_coaches)
        output_filename_xlsx = "scraped_coaches_data.xlsx"
        output_filename_csv = "scraped_coaches_data.csv"
        coaches_df.to_excel(output_filename_xlsx, index=False)
        coaches_df.to_csv(output_filename_csv, index=False)
        print(f"All coaches data saved to {output_filename_xlsx} and {output_filename_csv}")
    except Exception as e:
        print(f"Error saving coaches data: {str(e)}")

async def process_sheet_async(sheet_name, df):
    all_coaches = []
    all_failed_urls = []
    total_rows = len(df)
    processed_rows = 0
    successful_rows = 0
    failed_rows = 0

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

        semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

        async def process_url_with_semaphore(url, school, staff_directory_url, softball_coaches_url):
            nonlocal processed_rows, successful_rows, failed_rows
            async with semaphore:
                if pd.isna(url):
                    print(f"Skipped: {school} - Empty URL")
                    failed_rows += 1
                    all_failed_urls.append({"url": "N/A", "reason": "Empty URL", "school": school})
                else:
                    try:
                        failed_url, coaches = await process_url_async(url, school, context, staff_directory_url, softball_coaches_url)
                        if failed_url:
                            all_failed_urls.append(failed_url)
                            failed_rows += 1
                        elif coaches:
                            all_coaches.extend(coaches)
                            successful_rows += 1
                        else:
                            # This case handles when both failed_url and coaches are None
                            failed_rows += 1
                            all_failed_urls.append({"url": url, "reason": "Unknown error", "school": school})
                    except Exception as e:
                        print(f"Error processing {school} - {url}: {str(e)}")
                        all_failed_urls.append({"url": url, "reason": str(e), "school": school})
                        failed_rows += 1
                
                processed_rows += 1
                print(f"Progress: {processed_rows}/{total_rows} - Successful: {successful_rows} - Failed: {failed_rows}")

        await asyncio.gather(*[
            process_url_with_semaphore(row['2024 Coaches URL'], row['School'], row['Staff Directory'], row[[col for col in row.index if 'Coaches URL' in col][0]])
            for _, row in df.iterrows()
        ])

        await browser.close()

    # Save all coaches to Excel file
    coaches_df = pd.DataFrame(all_coaches)
    coaches_df.to_excel(f"scraped_coaches_data_{sheet_name}.xlsx", index=False)

    # Save all failed URLs to Excel file
    failed_urls_df = pd.DataFrame(all_failed_urls)
    failed_urls_df.to_excel(f"failed_urls_{sheet_name}.xlsx", index=False)

    print(f"Finished processing {sheet_name} - Successful: {successful_rows} - Failed: {failed_rows}")

async def main_async():
    excel_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    xls = pd.ExcelFile(excel_file)
    
    all_coaches = []
    all_failed_urls = []
    total_processed = 0
    total_successful = 0
    total_failed = 0
    
    for sheet_name in xls.sheet_names:
        if sheet_name != "NCAA D1": # process only sheet 2 for now
            continue
        print(f"Processing sheet: {sheet_name}")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
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

            semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

            async def process_url_with_semaphore(url, school, staff_directory_url, softball_coaches_url):
                nonlocal total_processed, total_successful, total_failed
                async with semaphore:
                    if pd.isna(url):
                        print(f"Skipped: {school} - Empty URL")
                        total_failed += 1
                        all_failed_urls.append({"url": "N/A", "reason": "Empty URL", "school": school})
                    else:
                        try:
                            failed_url, coaches = await process_url_async(url, school, context, staff_directory_url, softball_coaches_url)
                            if failed_url:
                                all_failed_urls.append(failed_url)
                                total_failed += 1
                            elif coaches:
                                all_coaches.extend(coaches)
                                total_successful += 1
                            else:
                                # This case handles when both failed_url and coaches are None
                                total_failed += 1
                                all_failed_urls.append({"url": url, "reason": "Unknown error", "school": school})
                        except Exception as e:
                            print(f"Error processing {school} - {url}: {str(e)}")
                            all_failed_urls.append({"url": url, "reason": str(e), "school": school})
                            total_failed += 1
                    
                    total_processed += 1
                    print(f"Progress: {total_processed}/{len(df)} - Successful: {total_successful} - Failed: {total_failed}")

            await asyncio.gather(*[
                process_url_with_semaphore(row['2024 Coaches URL'], row['School'], row['Staff Directory'], row[[col for col in row.index if 'Coaches URL' in col][0]])
                for _, row in df.iterrows()
            ])

            await browser.close()

        # Save results for the sheet
        save_coaches_data(all_coaches)
        save_failed_urls(all_failed_urls)
        
        print(f"Finished processing sheet: {sheet_name}")
        print(f"Coaches found: {total_successful}")
        print(f"Failed URLs: {total_failed}")
        print(f"Total processed: {total_processed}")
        print("=" * 50)
    
    print("Scraping completed!")
    print(f"Total rows processed: {total_processed}")
    print(f"Total coaches found: {total_successful}")
    print(f"Total failed URLs: {total_failed}")

if __name__ == "__main__":
    asyncio.run(main_async())