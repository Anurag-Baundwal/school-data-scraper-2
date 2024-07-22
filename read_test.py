import pandas as pd

try:
    df = pd.read_excel(r"C:\Users\dell3\source\repos\school-data-scraper-2\Freelancer_Data_Mining_Project.xlsx")
    print("Successfully opened the file")
    print(df.head())
except Exception as e:
    print(f"Error opening file: {str(e)}")