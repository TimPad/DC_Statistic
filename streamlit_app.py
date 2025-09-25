"""
Streamlit Application for Course Analytics Processing
Upload files and process course data automatically
"""

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import tempfile
import time
from io import StringIO

# Page configuration
st.set_page_config(
    page_title="Обработка аналитики курсов",
    page_icon="📊",
    layout="wide"
)

def get_gcp_credentials():
    """
    Загружает GCP credentials только из secrets.toml
    """
    if "gcp_service_account" in st.secrets:
        credentials_info = st.secrets["gcp_service_account"]
        return service_account.Credentials.from_service_account_info(credentials_info)
    else:
        raise RuntimeError("❌ Нет секрета [gcp_service_account] в .streamlit/secrets.toml")

def authenticate_google_sheets():
    """
    Авторизация и возврат клиента gspread
    """
    try:
        creds = get_gcp_credentials()
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Ошибка аутентификации в Google Sheets: {str(e)}")
        return None

def load_student_list(uploaded_file):
    """Load student list from uploaded Excel or CSV file"""
    try:
        # Determine file type and load accordingly
        file_name = uploaded_file.name.lower()
        if file_name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(uploaded_file)
        elif file_name.endswith('.csv'):
            # Check encoding and try different options
            content = uploaded_file.getvalue()
            try:
                # Try UTF-16 with tab delimiter first (based on project memory)
                df = pd.read_csv(StringIO(content.decode('utf-16')), sep='\t')
            except (UnicodeDecodeError, pd.errors.ParserError):
                try:
                    # Try UTF-8 with comma delimiter
                    df = pd.read_csv(StringIO(content.decode('utf-8')))
                except UnicodeDecodeError:
                    # Try cp1251 encoding (common for Russian files)
                    df = pd.read_csv(StringIO(content.decode('cp1251')))
        else:
            st.error("Неподдерживаемый формат файла. Используйте Excel (.xlsx, .xls) или CSV (.csv)")
            return None
        
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
            if source_col in df.columns:
                result_df[target_col] = df[source_col]
        
        # Check for 'Данные о пользователе' column and parse it according to specification
        if 'Данные о пользователе' in df.columns:
            user_data = df['Данные о пользователе'].astype(str)
            parsed_data = user_data.str.split(';', expand=True)
            if len(parsed_data.columns) >= 4:
                result_df['Факультет'] = parsed_data[0]
                result_df['Образовательная программа'] = parsed_data[1] 
                result_df['Курс'] = parsed_data[2]
                result_df['Группа'] = parsed_data[3]
        
        # Add missing columns with empty values
        for required_col in required_columns.keys():
            if required_col not in result_df.columns:
                result_df[required_col] = ''
        
        # Filter only students with edu.hse.ru email
        if 'Корпоративная почта' in result_df.columns:
            result_df = result_df[result_df['Корпоративная почта'].astype(str).str.contains('@edu.hse.ru', na=False)]
            result_df['Корпоративная почта'] = pd.Series(result_df['Корпоративная почта']).astype(str).str.lower().str.strip()
        
        return result_df
    except Exception as e:
        st.error(f"Ошибка загрузки списка студентов: {str(e)}")
        return None

def extract_course_data(uploaded_file, course_name):
    """Extract email and completion percentage from uploaded course file (CSV or Excel)"""
    try:
        # Determine file type and load accordingly
        file_name = uploaded_file.name.lower()
        if file_name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(uploaded_file)
        elif file_name.endswith('.csv'):
            # Read the uploaded file with different encoding options
            content = uploaded_file.getvalue()
            try:
                # Try UTF-16 with tab delimiter first (based on project memory)
                df = pd.read_csv(StringIO(content.decode('utf-16')), sep='\t')
            except (UnicodeDecodeError, pd.errors.ParserError):
                try:
                    # Try UTF-8 with comma delimiter
                    df = pd.read_csv(StringIO(content.decode('utf-8')))
                except UnicodeDecodeError:
                    # Try cp1251 encoding (common for Russian files)
                    df = pd.read_csv(StringIO(content.decode('cp1251')))
        else:
            st.error(f"Неподдерживаемый формат файла для курса {course_name}. Используйте Excel (.xlsx, .xls) или CSV (.csv)")
            return None
        
        # Look for email column with different possible names
        email_column = None
        possible_email_names = ['Корпоративная почта', 'Адрес электронной почты', 'Email', 'Почта', 'E-mail']
        
        for col_name in possible_email_names:
            if col_name in df.columns:
                email_column = col_name
                break
        
        if email_column is None:
            st.error(f"Столбец с email не найден в файле {course_name}. Ожидаются столбцы: {', '.join(possible_email_names)}")
            return None
        
        # Look for completion percentage column
        completion_column = None
        possible_completion_names = ['Процент завершения', 'Completion', 'Progress', 'Прогресс', 'Завершение']
        
        # Also check if we need to calculate completion from multiple columns
        # Look for columns that might contain completion data
        completed_columns = []
        timestamp_columns = []
        
        for col in df.columns:
            if col not in ['Unnamed: 0', email_column, 'Данные о пользователе', 'User information', 'Страна']:
                # Check if this column contains completion data
                if not col.startswith('Unnamed:') and len(str(col).strip()) > 0:
                    # Sample some values to see if they contain "Выполнено" или "Не выполнено"
                    sample_values = df[col].dropna().astype(str).head(100)
                    if any('Выполнено' in str(val) or 'выполнено' in str(val).lower() for val in sample_values):
                        # Skip informational columns (based on experience memory)
                        if not all(str(val) == 'Не выполнено' for val in sample_values if pd.notna(val)):
                            completed_columns.append(col)
                elif col.startswith('Unnamed:') and col != 'Unnamed: 0':
                    # Check if this unnamed column contains timestamps (completion indicators)
                    sample_values = df[col].dropna().astype(str).head(20)
                    for val in sample_values:
                        val_str = str(val).strip()
                        # Check if it looks like a timestamp
                        if any(pattern in val_str for pattern in ['2020', '2021', '2022', '2023', '2024']) and ':' in val_str:
                            timestamp_columns.append(col)
                            break
        
        # If we found timestamp columns, use them for completion calculation
        if timestamp_columns:
            st.info(f"Найдено {len(timestamp_columns)} столбцов с временными метками для курса {course_name}")
            
            # Calculate completion percentage based on timestamps
            completion_data = []
            for idx, row in df.iterrows():
                email_val = row[email_column]
                if pd.isna(email_val) or '@edu.hse.ru' not in str(email_val).lower():
                    continue
                
                total_tasks = len(timestamp_columns)
                completed_tasks = 0
                
                for col in timestamp_columns:
                    cell_val = row[col]
                    val_str = str(cell_val).strip() if not pd.isna(cell_val) else ''
                    # Check if there's a valid timestamp (indicates completion)
                    if val_str and val_str != 'nan' and val_str != '':
                        # Verify it looks like a timestamp
                        if any(pattern in val_str for pattern in ['2020', '2021', '2022', '2023', '2024']) and ':' in val_str:
                            completed_tasks += 1
                
                # Calculate percentage
                percentage = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                completion_data.append({'email': str(email_val).lower().strip(), 'percentage': percentage})
            
            # Create result DataFrame
            if completion_data:
                result_df = pd.DataFrame(completion_data)
                result_df.columns = ['Корпоративная почта', f'Процент_{course_name}']
                st.success(f"✅ Рассчитан процент завершения для {len(result_df)} студентов курса {course_name} на основе {len(timestamp_columns)} заданий")
                return result_df
            else:
                st.warning(f"Не найдено данных о завершении для курса {course_name}")
                return None
        
        # If we found completion tracking columns, calculate percentage
        elif completed_columns:
            st.info(f"Найдено {len(completed_columns)} столбцов с данными о выполнении для курса {course_name}")
            
            # Calculate completion percentage
            completion_data = []
            for idx, row in df.iterrows():
                email_val = row[email_column]
                if pd.isna(email_val) or '@edu.hse.ru' not in str(email_val).lower():
                    continue
                
                total_tasks = 0
                completed_tasks = 0
                
                for col in completed_columns:
                    cell_val = row[col]
                    val = str(cell_val).strip() if not pd.isna(cell_val) else ''
                    if val and val != 'nan':
                        total_tasks += 1
                        if 'Выполнено' in val or 'выполнено' in val.lower():
                            completed_tasks += 1
                
                # Calculate percentage
                percentage = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                completion_data.append({'email': str(email_val).lower().strip(), 'percentage': percentage})
            
            # Create result DataFrame
            if completion_data:
                result_df = pd.DataFrame(completion_data)
                result_df.columns = ['Корпоративная почта', f'Процент_{course_name}']
                st.success(f"✅ Рассчитан процент завершения для {len(result_df)} студентов курса {course_name}")
                return result_df
            else:
                st.warning(f"Не найдено данных о завершении для курса {course_name}")
                return None
        
        # Fallback: look for direct completion percentage column
        for col_name in possible_completion_names:
            if col_name in df.columns:
                completion_column = col_name
                break
        
        if completion_column is None:
            st.error(f"Столбец с процентом завершения не найден в файле {course_name}. Ожидаются столбцы: {', '.join(possible_completion_names)}")
            st.info(f"Доступные столбцы: {', '.join([col for col in df.columns[:10]])}")
            return None
        
        # Extract the specific columns we need
        course_data = df[[email_column, completion_column]].copy()
        course_data.columns = ['Корпоративная почта', f'Процент_{course_name}']
        course_data['Корпоративная почта'] = pd.Series(course_data['Корпоративная почта']).astype(str).str.lower().str.strip()
        
        # Filter only edu.hse.ru emails
        email_series = pd.Series(course_data['Корпоративная почта'])
        course_data = course_data[email_series.str.contains('@edu.hse.ru', na=False)]
        
        return course_data
    except Exception as e:
        st.error(f"Ошибка обработки данных курса {course_name}: {str(e)}")
        return None

def consolidate_data(student_list, course_data_list, course_names):
    """Consolidate all course data with student list"""
    try:
        # Start with student list
        consolidated = student_list.copy()
        
        # Merge each course data
        for course_data, course_name in zip(course_data_list, course_names):
            if course_data is not None:
                consolidated = pd.merge(consolidated, course_data, on='Корпоративная почта', how='left')
                # Fill NaN values with 0
                consolidated[f'Процент_{course_name}'] = consolidated[f'Процент_{course_name}'].fillna(0.0)
        
        return consolidated
    except Exception as e:
        st.error(f"Error consolidating data: {str(e)}")
        return None

def upload_to_google_sheets(client, data_df, batch_size=200):
    """Upload data to Google Sheets with progress bar"""
    try:
        # Open the Google Sheet
        spreadsheet = client.open('DC_stat')
        worksheet = spreadsheet.worksheet('Лист1')
        
        # Clear existing data
        st.info("Очистка существующих данных в Google Sheets...")
        worksheet.clear()
        
        # Upload headers first
        headers = data_df.columns.tolist()
        st.info("Загрузка заголовков...")
        worksheet.update(values=[headers], range_name='A1')
        
        # Upload data in batches with progress bar
        total_rows = len(data_df)
        total_batches = ((total_rows-1) // batch_size) + 1
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        successful_batches = 0
        
        for i in range(0, total_rows, batch_size):
            batch_num = i // batch_size + 1
            batch_end = min(i + batch_size, total_rows)
            batch_data = data_df.iloc[i:batch_end]
            
            # Convert to list format
            data_batch = batch_data.replace({pd.NA: '', pd.NaT: '', None: ''}).values.tolist()
            
            # Calculate starting row for this batch
            start_row = i + 2
            start_cell = f"A{start_row}"
            
            try:
                status_text.text(f"Загрузка пакета {batch_num}/{total_batches}: строки {i+1}-{batch_end}")
                worksheet.update(values=data_batch, range_name=start_cell)
                successful_batches += 1
                
                # Update progress bar
                progress = batch_num / total_batches
                progress_bar.progress(progress)
                
                # Small delay to avoid rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                st.error(f"Не удалось загрузить пакет {batch_num}: {str(e)}")
                return False
        
        progress_bar.progress(1.0)
        status_text.text(f"✅ Загрузка завершена: {successful_batches} пакетов загружено успешно")
        return True
        
    except Exception as e:
        st.error(f"Ошибка загрузки в Google Sheets: {str(e)}")
        return False

def main():
    st.title("📊 Обработка аналитики курсов")
    st.markdown("Загрузите файлы и обработайте аналитику курсов автоматически")
    
    # Sidebar for file uploads
    st.sidebar.header("📁 Загрузка файлов")
    
    # File upload widgets
    student_file = st.sidebar.file_uploader(
        "Загрузить список студентов (Excel/CSV)",
        type=['xlsx', 'xls', 'csv'],
        help="Загрузите Excel или CSV файл с информацией о студентах"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Файлы курсов (CSV/Excel)")
    
    course_cg_file = st.sidebar.file_uploader(
        "Загрузить данные курса ЦГ",
        type=['csv', 'xlsx', 'xls'],
        help="Загрузите CSV или Excel файл для курса Цифровая грамотность"
    )
    
    course_python_file = st.sidebar.file_uploader(
        "Загрузить данные курса Python",
        type=['csv', 'xlsx', 'xls'],
        help="Загрузите CSV или Excel файл для курса Python"
    )
    
    course_analysis_file = st.sidebar.file_uploader(
        "Загрузить данные курса Анализ данных",
        type=['csv', 'xlsx', 'xls'],
        help="Загрузите CSV или Excel файл для курса Анализ данных"
    )
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📋 Статус обработки")
        
        # Check if all files are uploaded
        files_uploaded = all([
            student_file is not None,
            course_cg_file is not None,
            course_python_file is not None,
            course_analysis_file is not None
        ])
        
        if not files_uploaded:
            st.info("Пожалуйста, загрузите все необходимые файлы для начала обработки:")
            st.markdown("""
            - ✅ Список студентов (Excel или CSV файл)
            - ✅ Данные курса ЦГ (CSV или Excel файл)  
            - ✅ Данные курса Python (CSV или Excel файл)
            - ✅ Данные курса Анализ данных (CSV или Excel файл)
            """)
            
            # Show upload status
            file_status = {
                "Список студентов": "✅" if student_file else "❌",
                "Курс ЦГ": "✅" if course_cg_file else "❌",
                "Курс Python": "✅" if course_python_file else "❌",
                "Курс Анализ данных": "✅" if course_analysis_file else "❌"
            }
            
            status_df = pd.DataFrame([{"Файл": k, "Статус": v} for k, v in file_status.items()])
            st.table(status_df)
        
        else:
            st.success("Все файлы успешно загружены! Готово к обработке.")
            
            if st.button("🚀 Начать обработку", type="primary"):
                
                with st.spinner("Обработка данных..."):
                    
                    # Step 1: Load student list
                    st.info("📚 Загрузка списка студентов...")
                    student_list = load_student_list(student_file)
                    if student_list is None:
                        st.stop()
                    st.success(f"✅ Загружено {len(student_list)} записей студентов")
                    
                    # Step 2: Process course files
                    st.info("📊 Обработка файлов курсов...")
                    course_names = ['ЦГ', 'Питон', 'Андан']
                    course_files = [course_cg_file, course_python_file, course_analysis_file]
                    course_data_list = []
                    
                    for course_file, course_name in zip(course_files, course_names):
                        course_data = extract_course_data(course_file, course_name)
                        if course_data is None:
                            st.stop()
                        course_data_list.append(course_data)
                        st.success(f"✅ Обработан курс {course_name}: {len(course_data)} записей")
                    
                    # Step 3: Consolidate data
                    st.info("🔄 Консолидация данных...")
                    consolidated_data = consolidate_data(student_list, course_data_list, course_names)
                    if consolidated_data is None:
                        st.stop()
                    st.success(f"✅ Данные консолидированы: {len(consolidated_data)} всего записей")
                    
                    # Step 4: Show statistics
                    st.info("📈 Генерация статистики...")
                    stats_col1, stats_col2, stats_col3 = st.columns(3)
                    
                    for i, course_name in enumerate(course_names):
                        col_name = f'Процент_{course_name}'
                        if col_name in consolidated_data.columns:
                            avg_completion = consolidated_data[col_name].mean()
                            students_100 = len(consolidated_data[consolidated_data[col_name] == 100.0])
                            students_0 = len(consolidated_data[consolidated_data[col_name] == 0.0])
                            
                            with [stats_col1, stats_col2, stats_col3][i]:
                                st.metric(
                                    label=f"Курс {course_name}",
                                    value=f"{avg_completion:.2f}%",
                                    delta=f"100%: {students_100} | 0%: {students_0}"
                                )
                    
                    # Step 5: Update Google Sheets
                    st.info("☁️ Обновление Google Sheets...")
                    client = authenticate_google_sheets()
                    if client is None:
                        st.stop()
                    
                    success = upload_to_google_sheets(client, consolidated_data)
                    if success:
                        st.success("🎉 Вся обработка завершена успешно!")
                        st.balloons()
                    else:
                        st.error("❌ Не удалось загрузить в Google Sheets")
    
    with col2:
        st.header("ℹ️ Информация")
        st.markdown("""
        **Системные требования:**
        - Список студентов в формате Excel (.xlsx, .xls) или CSV (.csv)
        - Данные курсов в формате CSV (.csv) или Excel (.xlsx, .xls) со столбцами:
          - `Корпоративная почта`
          - `Процент завершения`
        - Настроенный доступ к Google Sheets
        
        **Результат:**
        - Консолидированные данные загружены в Google Sheet "DC_stat"
        - Информация о студентах с процентами завершения
        - Нулевые значения для студентов, не найденных в данных курсов
        
        **Этапы обработки:**
        1. Загрузка и валидация списка студентов
        2. Обработка трех файлов курсов
        3. Консолидация данных по email
        4. Генерация статистики завершения
        5. Загрузка в Google Sheets
        
        **Поддерживаемые кодировки:**
        - UTF-8 (по умолчанию)
        - UTF-16 с разделителем табуляции
        - CP1251 (для русских файлов)
        """)
        
        if files_uploaded:
            st.markdown("---")
            st.success("Готово к обработке!")

if __name__ == "__main__":
    main()
