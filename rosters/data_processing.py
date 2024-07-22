# data_processing.py for scraping player info from college softball rosters

import asyncio
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from scraping_methods import html_based_scraping, genai_based_scraping, fallback_search

async def process_college(college_data, pass_number):
    url = college_data['2024 Roster URL']
    college_name = college_data['School']

    if pd.notna(url):
        print(f"\nProcessing {college_name} (URL: {url})")
        if pass_number == 1:
            result, success = await html_based_scraping(url, college_name)
            method = "HTML"
        elif pass_number == 2:
            result, success = await genai_based_scraping(url, college_name)
            method = "GenAI"
        else:  # pass_number == 3
            result, success = await fallback_search(college_name)
            method = "Fallback"

        if success:
            print(f"Successfully scraped data for {college_name} using {method}")
            return {
                'school': college_name,
                'url': url,
                'method': method,
                'success': True,
                'data': result
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

async def process_sheet(sheet_name, df):
    all_results = []

    for pass_number in range(1, 4):
        print(f"\nStarting Pass {pass_number} for sheet: {sheet_name}")
        
        pass_results = {
            'scraped_data': {},
            'failed_urls': [],
            'success_count': 0,
            'total_count': len(df)
        }
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_college, row, pass_number)  
                   for _, row in df.iterrows()]

            for future in as_completed(futures):
                result = future.result()
                if result['success']:
                    pass_results['scraped_data'][result['school']] = result['data'][result['school']]
                    pass_results['success_count'] += 1
                else:
                    pass_results['failed_urls'].append({
                        'school': result['school'],
                        'url': result['url'],
                        'reason': result.get('reason', 'Unknown')
                    })

        # Save results for this pass
        save_results(pass_results, sheet_name, pass_number)

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


def save_results(results, sheet_name, pass_number):
    # Save detailed results to JSON
    json_filename = f"{sheet_name}_Pass{pass_number}_results.json"
    with open(json_filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Save failed URLs to text file
    txt_filename = f"{sheet_name}_Pass{pass_number}_failed_urls.txt"
    with open(txt_filename, "w") as f:
        f.write(f"Failed URLs for {sheet_name} - Pass {pass_number}:\n\n")
        for failed in results['failed_urls']:
            f.write(f"School: {failed['school']}\nURL: {failed['url']}\nReason: {failed['reason']}\n\n")
    
    # Print summary statistics
    print(f"\nResults for {sheet_name} - Pass {pass_number}:")
    print(f"Successful scrapes: {results['success_count']}/{results['total_count']}")
    print(f"Failed scrapes: {len(results['failed_urls'])}")

def process_college(college_data, pass_number):
    url = college_data['2024 Roster URL']
    college_name = college_data['School']

    if pd.notna(url):
        print(f"\nProcessing {college_name} (URL: {url})")
        if pass_number == 1:
            result, success = html_based_scraping(url, college_name)
            method = "HTML"
        elif pass_number == 2:
            result, success = genai_based_scraping(url, college_name)
            method = "GenAI"
        else:  # pass_number == 3
            result, success = fallback_search(college_name)
            method = "Fallback"

        if success:
            print(f"Successfully scraped data for {college_name} using {method}")
            return {
                'school': college_name,
                'url': url,
                'method': method,
                'success': True,
                'data': result
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