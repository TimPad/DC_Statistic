"""
Sample update script for Google Sheets - updates a small sample of data for testing
"""

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os

def authenticate_google_sheets(credentials_path):
    """Authenticate with Google Sheets"""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"Google Sheets authentication error: {str(e)}")
        return None

def load_consolidated_data(file_path):
    """Load consolidated data from CSV"""
    print(f"Loading consolidated data from {file_path}...")
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} records from consolidated data")
    return df

def upload_sample_data(client, data_df, sample_size=1000):
    """Upload a sample of data to Google Sheets"""
    try:
        # Take a sample of the data
        if len(data_df) > sample_size:
            sample_df = data_df.sample(n=sample_size, random_state=42)
            print(f"Taking sample of {sample_size} records from {len(data_df)} total records")
        else:
            sample_df = data_df
            print(f"Using all {len(data_df)} records (less than sample size)")
        
        # Open the Google Sheet with your specific settings
        spreadsheet = client.open('DC_stat')
        worksheet = spreadsheet.worksheet('Лист1')
        
        # Clear existing data
        print("Clearing existing data...")
        worksheet.clear()
        
        # Upload headers first
        headers = sample_df.columns.tolist()
        print("Uploading headers...")
        worksheet.update(values=[headers], range_name='A1')
        
        # Convert data to list format
        data_list = sample_df.replace({pd.NA: '', pd.NaT: '', None: ''}).values.tolist()
        
        # Upload data (starting at row 2)
        print(f"Uploading {len(data_list)} rows of sample data...")
        if data_list:  # Check if there's data to upload
            worksheet.update(values=data_list, range_name='A2')
        
        print("✅ Sample data successfully uploaded to Google Sheets")
        return True
    except Exception as e:
        print(f"Error uploading sample data: {str(e)}")
        return False

def main():
    # File paths
    consolidated_file = "consolidated_course_data.csv"
    credentials_file = "/Users/timofeynikulin/data-culture-12ca9f5d6c82.json"
    
    print("Sample Google Sheets Update")
    print("=" * 50)
    
    # Check if consolidated file exists
    if not os.path.exists(consolidated_file):
        print(f"❌ Consolidated data file {consolidated_file} not found")
        return
    
    # Load consolidated data
    consolidated_data = load_consolidated_data(consolidated_file)
    
    # Update Google Sheets with sample data
    print("\nUpdating Google Sheets with sample data...")
    client = authenticate_google_sheets(credentials_file)
    if client:
        success = upload_sample_data(client, consolidated_data, sample_size=1000)
        if success:
            print("✅ Google Sheets sample update completed successfully")
        else:
            print("❌ Failed to update Google Sheets with sample data")
    else:
        print("❌ Failed to authenticate with Google Sheets")

if __name__ == "__main__":
    main()