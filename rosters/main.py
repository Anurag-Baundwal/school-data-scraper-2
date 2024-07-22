# main.py for scraping player info from college softball rosters
import asyncio
import pandas as pd
from data_processing import process_sheet

async def main():
    excel_file = r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx"
    xls = pd.ExcelFile(excel_file)
    
    for sheet_name in xls.sheet_names:
        print(f"\nProcessing sheet: {sheet_name}")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        await process_sheet(sheet_name, df)

if __name__ == "__main__":
    asyncio.run(main())