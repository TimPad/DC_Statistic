"""
Simple course processor for Google Sheets integration
"""

import pandas as pd
import io
import re
import sys
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

def load_excel_student_data(file_path):
    """Load student data from Excel file"""
    try:
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
        df_columns_lower = [col.lower().strip() for col in df.columns]
        
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
        result_df['Корпоративная почта'] = result_df['Корпоративная почта'].astype(str).str.lower().str.strip()
        
        print(f"Loaded {len(result_df)} records from Excel file")
        return result_df
    except Exception as e:
        print(f"Error loading Excel file: {str(e)}")
        return None

def load_csv_course_data(file_path):
    """Load and analyze course data from CSV file"""
    try:
        # Try different encodings
        try:
            # Try UTF-16 with tabulation (as in your files)
            with open(file_path, 'rb') as f:
                content = f.read()
            df = pd.read_csv(io.StringIO(content.decode('utf-16')), sep='\t', low_memory=False)
        except:
            try:
                # Try UTF-8 with comma
                df = pd.read_csv(file_path, sep=',', low_memory=False)
            except:
                # Try UTF-8 with semicolon
                df = pd.read_csv(file_path, sep=';', low_memory=False)
        
        print(f"Course CSV file loaded. Number of records: {len(df)}")
        return df
    except Exception as e:
        print(f"Error loading CSV file: {str(e)}")
        return None

def process_course_analytics(course_df):
    """Process course analytics from loaded data"""
    try:
        # Extract student information
        students_data = []
        
        # Find active students (exclude header rows)
        for idx, row in course_df.iterrows():
            # Get name from first column
            student_name = str(row[course_df.columns[0]]).strip()
            
            # Skip header rows
            if '-----' in student_name or student_name == 'nan' or not student_name:
                continue
                
            # Skip rows with positions
            if any(title in student_name.lower() for title in ['профессор', 'доцент', 'штатная', 'преподаватель', 'научный']):
                continue
            
            # Get user data
            user_data = str(row.get('Данные о пользователе', '')).strip()
            if not user_data or user_data == 'nan':
                continue
                
            # Get email
            email = str(row.get('Адрес электронной почты', '')).strip()
            
            # Add email domain check - keep only @edu.hse.ru records
            if not email or '@edu.hse.ru' not in email:
                continue
                
            # Also check that email doesn't contain words like "professor", "staff", etc.
            if any(title in email.lower() for title in ['professor', 'staff', 'admin', 'support']):
                continue
            
            # Check that email looks like a real student email
            if not re.match(r'^[a-zA-Z0-9._-]+@edu\.hse\.ru$', email):
                continue
            
            # Check that name doesn't contain service words
            if any(title in student_name.lower() for title in ['manager', 'admin', 'support', 'test']):
                continue
            
            # Check that user data doesn't contain service records
            if any(title in user_data.lower() for title in ['manager', 'admin', 'support', 'test']):
                continue
            
            # Check that user data contains faculty information
            if 'факультет' not in user_data.lower() and 'школа' not in user_data.lower() and 'институт' not in user_data.lower():
                continue
                
            # Split user data by semicolon
            parts = user_data.split(';')
            
            # Extract information
            faculty = ''
            program = ''
            course = ''
            group = ''
            
            if len(parts) > 0:
                faculty = parts[0].strip()
            
            if len(parts) > 1:
                program_part = parts[1].strip()
                # Clean program from codes
                program = re.sub(r'^[БМ]\s*\d+\.\d+\.\d+\s*[^;]*?\s*\d{4}\s*очная\s*', '', program_part).strip()
            
            if len(parts) > 2:
                course_part = parts[2].strip()
                # Extract course number
                course_match = re.search(r'(\d+)', course_part)
                if course_match:
                    course = course_match.group(1)
            
            if len(parts) > 3:
                group = parts[3].strip()
                # If fourth element is empty, try the last one
                if not group and len(parts) > 1:
                    group = parts[-1].strip()
            
            # Convert to lowercase for consistency
            faculty = faculty.lower()
            program = program.lower()
            group = group.lower()
            
            # Skip rows with positions in user data
            if any(title in faculty for title in ['профессор', 'доцент', 'штатная', 'преподаватель', 'научный', 'manager', 'admin']):
                continue
                
            # Check that faculty contains meaningful value
            if not faculty or len(faculty) < 5:
                continue
                
            students_data.append({
                'ФИО': student_name,
                'Email': email.lower().strip(),
                'Факультет': faculty,
                'Образовательная программа': program,
                'Курс': course,
                'Группа': group
            })
        
        # Create DataFrame
        students_df = pd.DataFrame(students_data)
        print(f"Extracted student data: {len(students_df)} records")
        
        # Identify real activities
        potential_activity_columns = []
        for col in course_df.columns:
            col_str = str(col).lower()
            if any(keyword in col_str for keyword in [
                'задач', 'тест', 'видео', 'блокнот', 'упражнен', 'практик'
            ]):
                potential_activity_columns.append(col)
        
        # Filter real activities (only with "Выполнено"/"Не выполнено")
        real_activity_columns = []
        for col in potential_activity_columns:
            try:
                unique_values = course_df[col].dropna().unique()
                # Real activity should have exactly 2 values: "Выполнено" and "Не выполнено"
                if len(unique_values) == 2:
                    values_str = [str(v).lower().strip() for v in unique_values]
                    if 'выполнено' in values_str and 'не выполнено' in values_str:
                        real_activity_columns.append(col)
            except Exception:
                continue
        
        print(f"Found real activities: {len(real_activity_columns)}")
        
        # Create student mapping dictionary
        student_mapping = {}
        for idx, row in students_df.iterrows():
            email = row['Email'].lower().strip()
            if email:
                student_mapping[email] = idx
        
        # Analyze progress for each student
        progress_data = []
        
        for idx, row in course_df.iterrows():
            # Get name from first column
            student_name = str(row[course_df.columns[0]]).strip()
            
            # Skip header rows
            if '-----' in student_name or student_name == 'nan' or not student_name:
                continue
                
            # Skip rows with positions
            if any(title in student_name.lower() for title in ['профессор', 'доцент', 'штатная', 'преподаватель', 'научный']):
                continue
            
            email = str(row.get('Адрес электронной почты', '')).strip().lower()
            
            # Add email domain check - keep only @edu.hse.ru records
            if not email or '@edu.hse.ru' not in email:
                continue
                
            # Also check that email doesn't contain words like "professor", "staff", etc.
            if any(title in email.lower() for title in ['professor', 'staff', 'admin', 'support']):
                continue
            
            # Check that email looks like a real student email
            if not re.match(r'^[a-zA-Z0-9._-]+@edu\.hse\.ru$', email):
                continue
            
            # Check if student exists in our data
            if email in student_mapping:
                student_idx = student_mapping[email]
                student_info = students_df.iloc[student_idx]
                
                # Count progress
                completed = 0
                attempted = 0
                not_started = 0
                
                for activity_col in real_activity_columns:
                    activity_value = row.get(activity_col, None)
                    
                    if pd.notna(activity_value) and str(activity_value).strip():
                        activity_str = str(activity_value).strip().lower()
                        
                        if activity_str == 'выполнено':
                            completed += 1
                            attempted += 1
                        elif activity_str == 'не выполнено':
                            attempted += 1
                        else:
                            not_started += 1
                    else:
                        not_started += 1
                
                total_activities = len(real_activity_columns)
                completion_rate = (completed / total_activities * 100) if total_activities > 0 else 0
                attempt_rate = (attempted / total_activities * 100) if total_activities > 0 else 0
                
                progress_data.append({
                    'Email': student_info['Email'],
                    'Завершено активностей': completed,
                    'Попыток выполнения': attempted,
                    'Не начато активностей': not_started,
                    'Всего активностей': total_activities,
                    'Процент завершения': round(completion_rate, 2),
                    'Процент попыток': round(attempt_rate, 2)
                })
        
        # Create DataFrame with progress
        progress_df = pd.DataFrame(progress_data)
        print(f"Analyzed student progress: {len(progress_df)} records")
        
        return progress_df
        
    except Exception as e:
        print(f"Error processing course analytics: {str(e)}")
        return None

def merge_student_and_course_data(student_df, progress_df):
    """Merge student data and course analytics"""
    try:
        # Merge by email
        if progress_df is not None and not progress_df.empty:
            result_df = pd.merge(student_df, progress_df, left_on='Корпоративная почта', right_on='Email', how='left')
            # Remove duplicate Email column
            result_df = result_df.drop('Email', axis=1)
            # Fill NaN values with zeros
            progress_columns = ['Завершено активностей', 'Попыток выполнения', 'Не начато активностей', 
                              'Всего активностей', 'Процент завершения', 'Процент попыток']
            for col in progress_columns:
                if col in result_df.columns:
                    result_df[col] = result_df[col].fillna(0)
        else:
            # If no progress data, add empty columns
            result_df = student_df.copy()
            result_df['Завершено активностей'] = 0
            result_df['Попыток выполнения'] = 0
            result_df['Не начато активностей'] = 0
            result_df['Всего активностей'] = 0
            result_df['Процент завершения'] = 0.0
            result_df['Процент попыток'] = 0.0
        
        print(f"Data merged. Total records: {len(result_df)}")
        return result_df
    except Exception as e:
        print(f"Error merging data: {str(e)}")
        return None

def authenticate_google_sheets(credentials_path):
    """Authenticate with Google Sheets"""
    try:
        # Use your specific Google Sheets configuration
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"Google Sheets authentication error: {str(e)}")
        return None

def update_google_sheet(client, data_df):
    """Update data in Google Sheets using your specific settings"""
    try:
        # Open the Google Sheet with your specific settings
        spreadsheet = client.open('DC_stat')
        worksheet = spreadsheet.worksheet('Лист1')
        
        # Clear existing data
        worksheet.clear()
        
        # Prepare data for upload
        # Headers
        headers = data_df.columns.tolist()
        
        # Data
        data = [headers] + data_df.replace({pd.NA: '', pd.NaT: '', None: ''}).values.tolist()
        
        # Upload data
        worksheet.update('A1', data)
        
        print(f"Data successfully updated in Google Sheets. Updated {len(data)-1} records.")
        return True
    except Exception as e:
        print(f"Error updating Google Sheets: {str(e)}")
        return False

def main():
    if len(sys.argv) != 4:
        print("Usage: python simple_course_processor.py <excel_file> <csv_file> <credentials_file>")
        print("Example: python simple_course_processor.py \"Список весь(1).xlsx\" \"Учебник_питон.csv\" \"/Users/timofeynikulin/data-culture-12ca9f5d6c82.json\"")
        sys.exit(1)
    
    excel_file = sys.argv[1]
    csv_file = sys.argv[2]
    credentials_file = sys.argv[3]
    
    print("Processing course data for Google Sheets integration...")
    print(f"Excel file: {excel_file}")
    print(f"CSV file: {csv_file}")
    print(f"Credentials file: {credentials_file}")
    
    # Load student data
    print("\n1. Loading student data...")
    student_df = load_excel_student_data(excel_file)
    if student_df is None:
        print("Failed to load student data")
        sys.exit(1)
    
    # Load course data
    print("\n2. Loading course data...")
    course_df = load_csv_course_data(csv_file)
    if course_df is None:
        print("Failed to load course data")
        sys.exit(1)
    
    # Process course analytics
    print("\n3. Processing course analytics...")
    progress_df = process_course_analytics(course_df)
    if progress_df is None:
        print("Failed to process course analytics")
        sys.exit(1)
    
    # Merge data
    print("\n4. Merging data...")
    result_df = merge_student_and_course_data(student_df, progress_df)
    if result_df is None:
        print("Failed to merge data")
        sys.exit(1)
    
    # Save to CSV
    output_file = f"course_analytics_result_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n5. Results saved to: {output_file}")
    
    # Update Google Sheets
    print("\n6. Updating Google Sheets...")
    client = authenticate_google_sheets(credentials_file)
    if client:
        success = update_google_sheet(client, result_df)
        if success:
            print("✅ Google Sheets successfully updated!")
        else:
            print("❌ Error updating Google Sheets")
    else:
        print("❌ Failed to authenticate with Google Sheets")
    
    print("\n✅ Processing completed successfully!")

if __name__ == "__main__":
    main()