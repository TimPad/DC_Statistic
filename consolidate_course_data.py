"""
Consolidate course data from three textbooks into a single dataset with percentage columns
"""

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os

def load_student_list(file_path):
    """Load student list from Excel file"""
    print("Loading student list...")
    df = pd.read_excel(file_path)
    
    # Map columns to required format
    required_columns = {
        'ФИО': ['фио', 'фio', 'имя', 'name'],
        'Корпоративная почта': ['корпоративная почта', 'email', 'почта', 'e-mail'],
        'Филиал (кампус)': ['филиал', 'кампус', 'campus'],
        'Факультет': ['факультет', 'faculty'],
        'Образовательная программа': ['образовательная программа', 'программа', 'educational program'],
        'Группа': ['группа', 'group'],
        'Курс': ['курс', 'course']
    }
    
    # Find matching columns in the file
    found_columns = {}
    df_columns_lower = [str(col).lower().strip() for col in df.columns]
    
    for target_col, possible_names in required_columns.items():
        for col_idx, col_name in enumerate(df_columns_lower):
            if any(possible_name in col_name for possible_name in possible_names):
                found_columns[target_col] = df.columns[col_idx]
                break
    
    # Create new DataFrame with required columns
    result_df = pd.DataFrame()
    for target_col, source_col in found_columns.items():
        result_df[target_col] = df[source_col]
    
    # Add missing columns with empty values
    for required_col in required_columns.keys():
        if required_col not in result_df.columns:
            result_df[required_col] = ''
    
    # Filter only students with edu.hse.ru email
    result_df = result_df[result_df['Корпоративная почта'].astype(str).str.contains('@edu.hse.ru', na=False)]
    # Convert to string before using str accessor
    result_df['Корпоративная почта'] = pd.Series(result_df['Корпоративная почта']).astype(str).str.lower().str.strip()
    
    print(f"Loaded {len(result_df)} student records")
    return result_df

def extract_course_data(file_path, course_name):
    """Extract email and completion percentage from course analytics file"""
    print(f"Processing {course_name} course data...")
    df = pd.read_csv(file_path)
    
    # Extract the specific columns we need
    course_data = df[['Корпоративная почта', 'Процент завершения']].copy()
    course_data.columns = ['Корпоративная почта', f'Процент_{course_name}']
    # Convert to string before using str accessor
    course_data['Корпоративная почта'] = pd.Series(course_data['Корпоративная почта']).astype(str).str.lower().str.strip()
    
    print(f"Extracted {len(course_data)} records for {course_name}")
    return course_data

def consolidate_data(student_list, course_files):
    """Consolidate all course data with student list"""
    print("Consolidating data...")
    
    # Start with student list
    consolidated = student_list.copy()
    
    # Merge each course data
    course_names = ['ЦГ', 'Питон', 'Андан']
    for i, (file_path, course_name) in enumerate(zip(course_files, course_names)):
        course_data = extract_course_data(file_path, course_name)
        consolidated = pd.merge(consolidated, course_data, on='Корпоративная почта', how='left')
        # Fill NaN values with 0
        consolidated[f'Процент_{course_name}'] = consolidated[f'Процент_{course_name}'].fillna(0.0)
        print(f"Merged {course_name} data")
    
    print(f"Consolidation complete. Total records: {len(consolidated)}")
    return consolidated

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

def update_google_sheet_incremental(client, data_df):
    """Update Google Sheets incrementally - only add new data or update existing"""
    try:
        # Open the Google Sheet with your specific settings
        spreadsheet = client.open('DC_stat')
        worksheet = spreadsheet.worksheet('Лист1')
        
        # Get existing data
        try:
            existing_data = worksheet.get_all_records()
            existing_df = pd.DataFrame(existing_data)
            print(f"Found {len(existing_df)} existing records in Google Sheets")
        except:
            existing_df = pd.DataFrame()
            print("No existing data found in Google Sheets")
        
        if existing_df.empty:
            # If no existing data, upload all data
            print("Uploading all data to Google Sheets...")
            upload_data_in_batches(worksheet, data_df)
        else:
            # Merge with existing data
            print("Merging with existing data...")
            # Merge on email column
            if 'Корпоративная почта' in existing_df.columns:
                # Update existing records and add new ones
                merged_df = existing_df.merge(data_df, on='Корпоративная почта', how='outer', suffixes=('_old', ''))
                
                # For columns that exist in both, prefer the new data
                for col in ['ФИО', 'Филиал (кампус)', 'Факультет', 'Образовательная программа', 'Группа', 'Курс']:
                    if f'{col}_old' in merged_df.columns and col in merged_df.columns:
                        # Use new data if available, otherwise keep old
                        merged_df[col] = merged_df[col].fillna(merged_df[f'{col}_old'])
                        merged_df.drop(columns=[f'{col}_old'], inplace=True)
                
                # For percentage columns, use new data if available, otherwise keep old
                for col in ['Процент_ЦГ', 'Процент_Питон', 'Процент_Андан']:
                    if f'{col}_old' in merged_df.columns and col in merged_df.columns:
                        # Use new data if available and different from 0, otherwise keep old
                        merged_df[col] = merged_df.apply(
                            lambda row: row[col] if pd.notna(row[col]) and row[col] != 0 
                                       else (row[f'{col}_old'] if pd.notna(row[f'{col}_old']) else 0), 
                            axis=1
                        )
                        merged_df.drop(columns=[f'{col}_old'], inplace=True)
                
                # Fill any remaining NaN values with 0 for percentage columns
                for col in ['Процент_ЦГ', 'Процент_Питон', 'Процент_Андан']:
                    if col in merged_df.columns:
                        merged_df[col] = merged_df[col].fillna(0.0)
                
                print(f"Uploading merged data ({len(merged_df)} records) to Google Sheets...")
                # Upload merged data in batches
                upload_data_in_batches(worksheet, merged_df)
            else:
                # If no email column in existing data, treat as empty and upload all new data
                print("No email column found in existing data, uploading all new data...")
                upload_data_in_batches(worksheet, data_df)
        
        print("✅ Data successfully updated in Google Sheets")
        return True
    except Exception as e:
        print(f"Error updating Google Sheets: {str(e)}")
        return False

def upload_data_in_batches(worksheet, data_df, batch_size=1000):
    """Upload data to Google Sheets in batches"""
    try:
        # Clear existing data
        worksheet.clear()
        
        # Upload headers first
        headers = data_df.columns.tolist()
        worksheet.update('A1', [headers])
        
        # Upload data in batches
        total_rows = len(data_df)
        total_batches = ((total_rows-1) // batch_size) + 1
        print(f"Uploading {total_rows} rows in batches of {batch_size} ({total_batches} batches total)...")
        
        for i in range(0, total_rows, batch_size):
            batch_num = i // batch_size + 1
            batch_end = min(i + batch_size, total_rows)
            batch_data = data_df.iloc[i:batch_end]
            
            # Convert to list format
            data_batch = batch_data.replace({pd.NA: '', pd.NaT: '', None: ''}).values.tolist()
            
            # Calculate starting row for this batch (add 2 to account for 1-based indexing and headers)
            start_row = i + 2
            start_cell = f"A{start_row}"
            
            print(f"  Uploading batch {batch_num}/{total_batches}: rows {i+1}-{batch_end}")
            worksheet.update(start_cell, data_batch)
            print(f"    ✓ Batch {batch_num} uploaded successfully")
        
        return True
    except Exception as e:
        print(f"Error uploading data in batches: {str(e)}")
        return False

def main():
    # File paths
    student_list_file = "Список весь(1).xlsx"
    course_files = [
        "course_analytics_result_20250925_175703.csv",  # ЦГ (Digital Literacy)
        "course_analytics_result_20250925_175228.csv",  # Питон (Python)
        "course_analytics_result_20250925_174913.csv"   # Андан (Analysis)
    ]
    credentials_file = "/Users/timofeynikulin/data-culture-12ca9f5d6c82.json"
    
    print("Consolidating course data")
    print("=" * 50)
    
    # Load student list
    student_list = load_student_list(student_list_file)
    
    # Consolidate data
    consolidated_data = consolidate_data(student_list, course_files)
    
    # Save to CSV
    output_file = "consolidated_course_data.csv"
    consolidated_data.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Saved consolidated data to {output_file}")
    
    # Show some statistics
    print("\nCompletion Statistics:")
    for course in ['ЦГ', 'Питон', 'Андан']:
        col_name = f'Процент_{course}'
        if col_name in consolidated_data.columns:
            avg_completion = consolidated_data[col_name].mean()
            students_100 = len(consolidated_data[consolidated_data[col_name] == 100.0])
            students_0 = len(consolidated_data[consolidated_data[col_name] == 0.0])
            print(f"{course}: Average {avg_completion:.2f}%, 100%: {students_100}, 0%: {students_0}")
    
    # Update Google Sheets
    print("\nUpdating Google Sheets...")
    client = authenticate_google_sheets(credentials_file)
    if client:
        success = update_google_sheet_incremental(client, consolidated_data)
        if success:
            print("✅ Google Sheets updated successfully")
        else:
            print("❌ Failed to update Google Sheets")
    else:
        print("❌ Failed to authenticate with Google Sheets")

if __name__ == "__main__":
    main()