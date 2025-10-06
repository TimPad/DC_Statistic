"""
Streamlit Application for Course Analytics Processing
Upload files and process course data automatically with Supabase database
"""

import streamlit as st
import pandas as pd
from supabase import create_client, Client
import os
import tempfile
import time
from io import StringIO
from datetime import datetime
from separated_db_functions import upload_students_to_supabase, upload_all_courses_to_supabase

# Page configuration
st.set_page_config(
    page_title="Обработка аналитики курсов - Supabase",
    page_icon="📊",
    layout="wide"
)

def authenticate_supabase():
    """Аутентификация с Supabase используя Streamlit secrets"""
    try:
        # Проверяем наличие секретов Supabase
        if not hasattr(st, 'secrets') or "supabase" not in st.secrets:
            st.error("❌ Секреты Supabase не найдены в конфигурации Streamlit")
            st.error("💡 Для развертывания настройте секреты SUPABASE_URL и SUPABASE_KEY")
            return None
        
        # Получаем данные подключения из секретов
        supabase_url = st.secrets["supabase"]["url"]
        supabase_key = st.secrets["supabase"]["key"]
        
        # Создаем клиента Supabase
        supabase: Client = create_client(supabase_url, supabase_key)
        
        st.success("✅ Аутентификация Supabase успешна")
        return supabase
        
    except Exception as e:
        st.error(f"❌ Ошибка аутентификации Supabase: {str(e)}")
        return None

def check_supabase_connection(supabase):
    """Проверка подключения к Supabase"""
    try:
        if supabase is None:
            st.error("❌ Клиент Supabase не инициализирован")
            return False
        
        st.info("🔍 Проверка подключения к Supabase...")
        
        # Проверяем доступ к таблице course_analytics
        try:
            # Попытка получить схему таблицы
            result = supabase.table('course_analytics').select('*').limit(1).execute()
            st.success("✅ Таблица 'course_analytics' доступна")
            
            # Проверяем, есть ли колонка "версия_образовательной_программы"
            if result.data:
                # Получаем первую запись для проверки структуры
                sample_record = result.data[0]
                if 'версия_образовательной_программы' not in sample_record:
                    st.warning("⚠️ Колонка 'версия_образовательной_программы' отсутствует. Добавляем...")
                    # Добавляем отсутствующую колонку
                    alter_sql = """
                    ALTER TABLE course_analytics 
                    ADD COLUMN IF NOT EXISTS версия_образовательной_программы TEXT;
                    """
                    try:
                        alter_result = supabase.rpc('exec_sql', {'sql': alter_sql}).execute()
                        st.success("✅ Колонка 'версия_образовательной_программы' добавлена")
                    except Exception as alter_error:
                        st.error(f"❌ Не удалось добавить колонку: {str(alter_error)}")
                        st.info("💡 Выполните в Supabase SQL Editor:")
                        st.code(alter_sql, language='sql')
                else:
                    st.success("✅ Колонка 'версия_образовательной_программы' присутствует")
            
            # Проверка прав на запись (тестовая запись)
            test_record = {
                'фио': f'Тест подключения {datetime.now().strftime("%H:%M:%S")}',
                'корпоративная_почта': 'test@connection.check',
                'филиал_кампус': 'Тест',
                'факультет': 'Тест',
                'образовательная_программа': 'Тест',
                'версия_образовательной_программы': 'Тест',
                'группа': 'Тест',
                'курс': 'Тест',
                'процент_цг': 0.0,
                'процент_питон': 0.0,
                'процент_андан': 0.0,
                'created_at': datetime.now().isoformat()
            }
            
            # Записываем тестовую запись
            insert_result = supabase.table('course_analytics').insert(test_record).execute()
            
            if insert_result.data:
                test_id = insert_result.data[0]['id']
                st.success("✅ Права на запись подтверждены")
                
                # Удаляем тестовую запись
                supabase.table('course_analytics').delete().eq('id', test_id).execute()
                st.success("✅ Права на удаление подтверждены")
            else:
                st.error("❌ Не удалось записать тестовые данные")
                return False
                
        except Exception as e:
            if "relation \"course_analytics\" does not exist" in str(e).lower():
                st.warning("⚠️ Таблица 'course_analytics' не существует. Будет создана автоматически.")
                # Создаем таблицу
                if not create_course_analytics_table(supabase):
                    return False
            elif "row-level security policy" in str(e).lower() or "42501" in str(e):
                st.error("❌ Ошибка Row Level Security (RLS): доступ к таблице заблокирован политиками безопасности")
                st.error("💡 Необходимо настроить RLS политики в Supabase Dashboard:")
                st.code("""
-- В Supabase SQL Editor выполните:

-- 1. Отключить RLS для таблицы (рекомендуется для внутренних приложений)
ALTER TABLE course_analytics DISABLE ROW LEVEL SECURITY;

-- ИЛИ

-- 2. Создать политику для разрешения всех операций (более безопасно)
CREATE POLICY "Enable all operations for service role" ON course_analytics
FOR ALL USING (true) WITH CHECK (true);

-- 3. Альтернативно: политика только для authenticated пользователей
CREATE POLICY "Enable operations for authenticated users" ON course_analytics
FOR ALL USING (auth.role() = 'authenticated') WITH CHECK (auth.role() = 'authenticated');
                """, language='sql')
                return False
            else:
                st.error(f"❌ Ошибка доступа к таблице: {str(e)}")
                return False
        
        st.success("🎉 Подключение к Supabase полностью работоспособно!")
        return True
        
    except Exception as e:
        st.error(f"❌ Общая ошибка проверки подключения: {str(e)}")
        return False

def create_course_analytics_table(supabase):
    """Создание таблицы course_analytics в Supabase"""
    try:
        st.info("🔧 Создание таблицы course_analytics...")
        
        # SQL для создания таблицы
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS course_analytics (
            id SERIAL PRIMARY KEY,
            фио TEXT NOT NULL,
            корпоративная_почта TEXT UNIQUE NOT NULL,
            филиал_кампус TEXT,
            факультет TEXT,
            образовательная_программа TEXT,
            версия_образовательной_программы TEXT,
            группа TEXT,
            курс TEXT,
            процент_цг REAL DEFAULT 0.0,
            процент_питон REAL DEFAULT 0.0,
            процент_андан REAL DEFAULT 0.0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Создаем индекс на email для быстрого поиска
        CREATE INDEX IF NOT EXISTS idx_course_analytics_email 
        ON course_analytics(корпоративная_почта);
        
        -- Функция для автоматического обновления updated_at
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        
        -- Триггер для автоматического обновления updated_at
        DROP TRIGGER IF EXISTS update_course_analytics_updated_at ON course_analytics;
        CREATE TRIGGER update_course_analytics_updated_at
            BEFORE UPDATE ON course_analytics
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """
        
        # Выполняем SQL через RPC (если поддерживается) или через прямой запрос
        try:
            result = supabase.rpc('exec_sql', {'sql': create_table_sql}).execute()
            st.success("✅ Таблица course_analytics создана успешно")
            return True
        except Exception as rpc_error:
            st.warning(f"⚠️ RPC недоступен: {str(rpc_error)}")
            st.info("💡 Создайте таблицу course_analytics вручную в Supabase Dashboard")
            st.code(create_table_sql, language='sql')
            return False
            
    except Exception as e:
        st.error(f"❌ Ошибка создания таблицы: {str(e)}")
        return False

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
            'Корпоративная почта': ['адрес электронной почты', 'корпоративная почта', 'email', 'почта', 'e-mail'],
            'Филиал (кампус)': ['филиал', 'кампус', 'campus'],
            'Факультет': ['факультет', 'faculty'],
            'Образовательная программа': ['образовательная программа', 'программа', 'educational program'],
            'Версия образовательной программы': ['версия образовательной программы', 'версия программы', 'program version', 'version'],
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
        possible_email_names = ['Адрес электронной почты', 'Корпоративная почта', 'Email', 'Почта', 'E-mail']
        
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
        
        # Колонки для исключения из анализа курса ЦГ (на основе анализа паттернов)
        # ВАЖНО: Исключаем справочные материалы, спецификации, промо-контент и оставляем только учебные задания
        cg_excluded_keywords = [
            # Справочные и информационные материалы
            'take away', 'шпаргалка', 'консультация', 'общая информация', 'промо-ролик',
            'поддержка студентов', 'пояснение', 'случайный вариант для студентов с овз',
            'материалы по модулю', 'копия',
            
            # Экзаменационные материалы и спецификации  
            'демонстрационный вариант', 'спецификация', 'демо-версия',
            'правила проведения независимого экзамена', 'порядок организации и проведения независимых экзаменов',
            'интерактивный тренажер правил нэ', 'пересдачи в сентябре', 'незрячих и слабовидящих',
            
            # Проектные работы (не входят в основную программу)
            'проекты с использование tei',
            
            # Тренировочные и обучающие материалы (не оцениваемые)
            'тренировочный тест', 'ключевые принципы tei', 'базовые возможности tie',
            'специальные модули tei', 'будут идентичными',
            
            # Опросы и анкеты (не оцениваемые)
            'опрос', 'тест по модулю', 'анкета',
            
            # Системные и служебные колонки
            'user information', 'страна', 'user_id', 'данные о пользователе'
        ]
        
        # Подсчет статистики фильтрации для курса ЦГ
        excluded_count = 0
        included_count = 0
        
        for col in df.columns:
            if col not in ['Unnamed: 0', email_column, 'Данные о пользователе', 'User information', 'Страна']:
                # Для курса ЦГ проверяем список исключений
                if course_name == 'ЦГ':
                    should_exclude = False
                    col_str = str(col).strip().lower()
                    
                    # Проверяем каждое ключевое слово для исключения
                    for excluded_keyword in cg_excluded_keywords:
                        if excluded_keyword.lower() in col_str:
                            should_exclude = True
                            excluded_count += 1
                            # Не выводим информацию о каждой исключенной колонке
                            break
                    
                    if should_exclude:
                        continue
                    # Не выводим информацию о каждой включенной колонке
                    included_count += 1
                
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
        
        # Сводная информация о фильтрации ЦГ
        if course_name == 'ЦГ':
            total_relevant_columns = excluded_count + included_count
            st.success(f"📊 Фильтрация ЦГ: исключено {excluded_count} колонок, включено {included_count} колонок из {total_relevant_columns} проанализированных")
        
        # If we found timestamp columns, use them for completion calculation
        if timestamp_columns:
            if course_name == 'ЦГ':
                st.success(f"✅ Курс ЦГ: найдено {len(timestamp_columns)} столбцов с временными метками (исключены справочные материалы)")
            else:
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
    """Consolidate all course data with student list and deduplication"""
    try:
        # Start with student list
        consolidated = student_list.copy()
        
        # Merge each course data
        for course_data, course_name in zip(course_data_list, course_names):
            if course_data is not None:
                consolidated = pd.merge(consolidated, course_data, on='Корпоративная почта', how='left')
                # Заполняем NULL значения (не 0.0)
                consolidated[f'Процент_{course_name}'] = consolidated[f'Процент_{course_name}'].where(pd.notna(consolidated[f'Процент_{course_name}']), None)
        
        # Критическая дедупликация по email
        st.info("🔍 Проверка и удаление дубликатов...")
        
        # Проверяем наличие дубликатов
        initial_count = len(consolidated)
        email_counts = consolidated['Корпоративная почта'].value_counts()
        duplicates = email_counts[email_counts > 1]
        
        if len(duplicates) > 0:
            st.warning(f"⚠️ Обнаружено {len(duplicates)} дубликатов email:")
            # Показываем первые несколько дубликатов
            duplicate_list = list(duplicates.index[:5])
            for email in duplicate_list:
                count = duplicates[email]
                st.text(f"  - {email}: {count} записей")
            if len(duplicates) > 5:
                st.text(f"  ... и ещё {len(duplicates) - 5} дубликатов")
        
        # Удаляем дубликаты, оставляя первое вхождение
        consolidated = consolidated.drop_duplicates(subset=['Корпоративная почта'], keep='first')
        
        final_count = len(consolidated)
        removed_count = initial_count - final_count
        
        if removed_count > 0:
            st.success(f"✅ Удалено {removed_count} дубликатов. Осталось {final_count} уникальных записей")
        else:
            st.success(f"✅ Дубликаты не обнаружены. Всего {final_count} уникальных записей")
        
        return consolidated
        
    except Exception as e:
        st.error(f"Error consolidating data: {str(e)}")
        return None

def upload_to_supabase(supabase, data_df, batch_size=200):
    """Инкрементальная загрузка данных в Supabase с прогресс-баром"""
    try:
        # Получаем существующие данные для сравнения
        existing_result = supabase.table('course_analytics').select('*').execute()
        existing_data = {}
        
        # Создаем словарь существующих записей по email
        if existing_result.data:
            for record in existing_result.data:
                email = record.get('корпоративная_почта', '').lower().strip()
                if email:
                    existing_data[email] = record
        
        st.success(f"✅ Найдено {len(existing_data)} существующих записей")
        
        # Подготавливаем данные для обновления
        records_to_insert = []
        records_to_update = []
        unchanged_count = 0
        processed_emails = set()  # Отслеживаем обработанные email для избежания дубликатов
        
        for _, row in data_df.iterrows():
            # Используем правильное название колонки email из памяти проекта
            email = str(row.get('Корпоративная почта', '')).strip().lower()
            if not email:  # Пробуем альтернативное название
                email = str(row.get('Адрес электронной почты', '')).strip().lower()
            
            # Пропускаем записи без email или с неправильным доменом
            if not email or '@edu.hse.ru' not in email:
                continue
            
            # КРИТИЧЕСКИ ВАЖНО: Пропускаем дубликаты в текущем наборе данных
            if email in processed_emails:
                st.warning(f"⚠️ Пропущен дубликат в текущих данных: {email}")
                continue
            processed_emails.add(email)
            
            # КРИТИЧЕСКИ ВАЖНО: Проверяем существование в базе по ТОЧНОМУ email
            email_exists_in_db = False
            for existing_email in existing_data.keys():
                if existing_email == email:
                    email_exists_in_db = True
                    break
            
            new_record = {
                'фио': str(row.get('ФИО', 'Неизвестно')).strip() if pd.notna(row.get('ФИО')) and str(row.get('ФИО', '')).strip() else 'Неизвестно',
                'корпоративная_почта': email if email else None,
                'филиал_кампус': str(row.get('Филиал (кампус)', '')) if pd.notna(row.get('Филиал (кампус)')) and str(row.get('Филиал (кампус)', '')).strip() else None,
                'факультет': str(row.get('Факультет', '')) if pd.notna(row.get('Факультет')) and str(row.get('Факультет', '')).strip() else None,
                'образовательная_программа': str(row.get('Образовательная программа', '')) if pd.notna(row.get('Образовательная программа')) and str(row.get('Образовательная программа', '')).strip() else None,
                'версия_образовательной_программы': str(row.get('Версия образовательной программы', '')) if pd.notna(row.get('Версия образовательной программы')) and str(row.get('Версия образовательной программы', '')).strip() else None,
                'группа': str(row.get('Группа', '')) if pd.notna(row.get('Группа')) and str(row.get('Группа', '')).strip() else None,
                'курс': str(row.get('Курс', '')) if pd.notna(row.get('Курс')) and str(row.get('Курс', '')).strip() else None,
                'процент_цг': float(row.get('Процент_ЦГ', 0.0)) if pd.notna(row.get('Процент_ЦГ')) and row.get('Процент_ЦГ') != '' else None,
                'процент_питон': float(row.get('Процент_Питон', 0.0)) if pd.notna(row.get('Процент_Питон')) and row.get('Процент_Питон') != '' else None,
                'процент_андан': float(row.get('Процент_Андан', 0.0)) if pd.notna(row.get('Процент_Андан')) and row.get('Процент_Андан') != '' else None
            }
            
            # Отладочная информация для версии программы (только в случае ошибок)
            version_value = new_record.get('версия_образовательной_программы')
            
            # Проверяем, есть ли этот email в базе данных (существующие записи)
            if email_exists_in_db:
                # Находим соответствующую запись в базе
                existing_record = None
                for existing_email, record in existing_data.items():
                    if existing_email == email:
                        existing_record = record
                        break
                
                if existing_record is None:
                    # Не нашли запись - рассматриваем как новую
                    new_record['created_at'] = datetime.now().isoformat()
                    records_to_insert.append(new_record)
                    continue
                
                # Проверяем, изменились ли данные
                needs_update = False
                
                # Сравниваем ключевые поля
                for key, value in new_record.items():
                    if key == 'корпоративная_почта':
                        continue  # Пропускаем ключевое поле
                    
                    existing_value = existing_record.get(key)
                    
                    # Для числовых полей сравниваем с толерантностью
                    if key.startswith('процент_'):
                        # Сравниваем NULL значения
                        if value is None and existing_value is None:
                            continue
                        if value is None or existing_value is None:
                            needs_update = True
                            break
                        if abs(float(existing_value) - float(value)) > 0.01:  # Толерантность 0.01%
                            needs_update = True
                            break
                    else:
                        # Для текстовых полей сравниваем NULL и строки
                        existing_str = str(existing_value).strip() if existing_value is not None else None
                        new_str = str(value).strip() if value is not None else None
                        
                        # Особое внимание к полю версия_образовательной_программы (отладка только при ошибках)
                        if key == 'версия_образовательной_программы':
                            # Если в базе NULL или пустая строка, а в новых данных есть значение - обновляем
                            if (existing_value is None or existing_str is None or existing_str == '') and new_str is not None and new_str != '':
                                needs_update = True
                                st.success(f"🔄 Обновление {email}: добавление версии программы '{new_str}'")
                                break
                            # Если значения разные - обновляем
                            elif existing_str != new_str:
                                needs_update = True
                                st.success(f"🔄 Обновление {email}: изменение версии с '{existing_str}' на '{new_str}'")
                                break
                        else:
                            if existing_str != new_str:
                                needs_update = True
                                break
                
                if needs_update:
                    new_record['id'] = existing_record['id']  # Добавляем ID для обновления
                    records_to_update.append(new_record)
                else:
                    unchanged_count += 1
            else:
                # Новая запись
                new_record['created_at'] = datetime.now().isoformat()
                records_to_insert.append(new_record)
        
        st.info(f"📋 Анализ изменений: {len(records_to_insert)} новых, {len(records_to_update)} обновлений, {unchanged_count} без изменений")
        
        if len(records_to_insert) == 0 and len(records_to_update) == 0:
            st.success("✅ Никаких изменений не обнаружено. База данных актуальна.")
            return True
        
        total_operations = len(records_to_insert) + len(records_to_update)
        total_batches = ((total_operations-1) // batch_size) + 1
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        successful_operations = 0
        current_operation = 0
        
        # Обрабатываем новые записи
        if records_to_insert:
            st.info(f"➕ Добавление {len(records_to_insert)} новых записей...")
            
            for i in range(0, len(records_to_insert), batch_size):
                batch_num = current_operation // batch_size + 1
                batch_end = min(i + batch_size, len(records_to_insert))
                batch_data = records_to_insert[i:batch_end]
                
                try:
                    status_text.text(f"Добавление пакета {batch_num}: записи {i+1}-{batch_end}")
                    
                    result = supabase.table('course_analytics').insert(batch_data).execute()
                    
                    if result.data:
                        successful_operations += len(result.data)
                    
                    current_operation += len(batch_data)
                    progress = current_operation / total_operations
                    progress_bar.progress(progress)
                    
                    time.sleep(0.1)
                    
                except Exception as e:
                    error_msg = str(e)
                    if "row-level security policy" in error_msg.lower() or "42501" in error_msg:
                        st.error(f"❌ Пакет {batch_num}: Ошибка Row Level Security")
                        st.error("💡 Необходимо настроить RLS политики в Supabase. Отключите RLS или создайте политику разрешения.")
                    elif "duplicate key value violates unique constraint" in error_msg.lower() or "23505" in error_msg:
                        st.error(f"❌ Пакет {batch_num}: Ошибка дубликата ключа")
                        st.error("💡 Обнаружены дубликаты email в базе. Проверьте исходные данные.")
                        # Попытаемся обработать каждую запись индивидуально
                        st.info("🔄 Попытка индивидуальной обработки записей...")
                        individual_success = 0
                        for record in batch_data:
                            try:
                                individual_result = supabase.table('course_analytics').insert([record]).execute()
                                if individual_result.data:
                                    individual_success += 1
                            except Exception as individual_error:
                                # Логируем ошибки для отдельных записей, но продолжаем
                                pass
                        if individual_success > 0:
                            successful_operations += individual_success
                            st.success(f"✅ Обработано индивидуально: {individual_success} записей")
                    else:
                        st.error(f"Не удалось добавить пакет {batch_num}: {error_msg}")
                        return False
        
        # Обрабатываем обновления
        if records_to_update:
            st.info(f"🔄 Обновление {len(records_to_update)} существующих записей...")
            
            for record in records_to_update:
                try:
                    record_id = record.pop('id')  # Удаляем ID из данных обновления
                    
                    result = supabase.table('course_analytics').update(record).eq('id', record_id).execute()
                    
                    if result.data:
                        successful_operations += 1
                    
                    current_operation += 1
                    progress = current_operation / total_operations
                    progress_bar.progress(progress)
                    
                    if current_operation % 10 == 0:  # Обновляем статус каждые 10 операций
                        status_text.text(f"Обновлено записей: {current_operation - len(records_to_insert)}/{len(records_to_update)}")
                    
                except Exception as e:
                    st.error(f"Не удалось обновить запись: {str(e)}")
                    return False
        
        progress_bar.progress(1.0)
        status_text.text(f"✅ Инкрементальное обновление завершено: {successful_operations} операций выполнено")
        return True
        
    except Exception as e:
        st.error(f"Ошибка инкрементального обновления Supabase: {str(e)}")
        return False

def main():
    st.title("📊 Обработка аналитики курсов")
    st.markdown("Загрузите файлы и обработайте аналитику курсов автоматически с сохранением в Supabase")
    
    # Sidebar for file uploads
    st.sidebar.header("📁 Загрузка файлов")
    
    # Опция выбора структуры БД
    st.sidebar.markdown("---")
    st.sidebar.subheader("💾 Структура базы данных")
    use_separated_tables = st.sidebar.radio(
        "Выберите структуру:",
        ["Объединенная таблица", "Разделенные таблицы"],
        index=1,  # По умолчанию разделенные
        help="Разделенные таблицы: студенты отдельно, прогресс по курсам отдельно"
    ) == "Разделенные таблицы"
    
    if use_separated_tables:
        st.sidebar.info("🔄 Используются разделенные таблицы")
    else:
        st.sidebar.info("🔗 Используется объединенная таблица")
    
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
                    
                    # Step 0: Проверка подключения к Supabase
                    st.info("🔍 Проверка подключения к Supabase...")
                    supabase = authenticate_supabase()
                    if supabase is None:
                        st.error("❌ Не удалось установить подключение к Supabase")
                        st.error("💡 Проверьте конфигурацию секретов и повторите попытку")
                        st.stop()
                    
                    # Проверяем работоспособность подключения
                    if not check_supabase_connection(supabase):
                        st.error("❌ Подключение к Supabase не работает")
                        st.error("💡 Устраните проблемы с доступом и повторите попытку")
                        st.stop()
                    
                    st.success("✅ Подключение к Supabase проверено и работает")
                    
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
                    
                    # Step 4: Show summary statistics table
                    st.info("📋 Генерация сводной статистики...")
                    
                    # Создаем сводную таблицу
                    summary_data = []
                    for course_name in course_names:
                        col_name = f'Процент_{course_name}'
                        if col_name in consolidated_data.columns:
                            course_data = consolidated_data[col_name].dropna()
                            if len(course_data) > 0:
                                avg_completion = course_data.mean()
                                students_100 = len(course_data[course_data == 100.0])
                                students_0 = len(course_data[course_data == 0.0])
                                students_partial = len(course_data[(course_data > 0.0) & (course_data < 100.0)])
                                total_students = len(course_data)
                                
                                # Добавляем разбивку по 10% диапазонам
                                students_90_99 = len(course_data[(course_data >= 90.0) & (course_data < 100.0)])
                                students_80_89 = len(course_data[(course_data >= 80.0) & (course_data < 90.0)])
                                students_70_79 = len(course_data[(course_data >= 70.0) & (course_data < 80.0)])
                                students_60_69 = len(course_data[(course_data >= 60.0) & (course_data < 70.0)])
                                students_50_59 = len(course_data[(course_data >= 50.0) & (course_data < 60.0)])
                                students_40_49 = len(course_data[(course_data >= 40.0) & (course_data < 50.0)])
                                students_30_39 = len(course_data[(course_data >= 30.0) & (course_data < 40.0)])
                                students_20_29 = len(course_data[(course_data >= 20.0) & (course_data < 30.0)])
                                students_10_19 = len(course_data[(course_data >= 10.0) & (course_data < 20.0)])
                                students_1_9 = len(course_data[(course_data > 0.0) & (course_data < 10.0)])
                                
                                summary_data.append({
                                    'Курс': course_name,
                                    'Студентов всего': total_students,
                                    'Средний %': f"{avg_completion:.1f}%",
                                    '100%': students_100,
                                    '90-99%': students_90_99,
                                    '80-89%': students_80_89,
                                    '70-79%': students_70_79,
                                    '60-69%': students_60_69,
                                    '50-59%': students_50_59,
                                    '40-49%': students_40_49,
                                    '30-39%': students_30_39,
                                    '20-29%': students_20_29,
                                    '10-19%': students_10_19,
                                    '1-9%': students_1_9,
                                    '0%': students_0
                                })
                    
                    if summary_data:
                        summary_df = pd.DataFrame(summary_data)
                        st.subheader("📋 Сводная таблица по курсам")
                        st.table(summary_df)
                        
                        # Общая статистика
                        total_students = len(consolidated_data)
                        students_with_data = len(consolidated_data.dropna(subset=[f'Процент_{course_names[0]}', f'Процент_{course_names[1]}', f'Процент_{course_names[2]}'], how='all'))
                        
                        st.info(f"📊 Общая статистика: {total_students} студентов в списке, {students_with_data} с данными о прогрессе")
                    
                    # Step 5: Обновление базы данных Supabase
                    st.info("💾 Обновление базы данных Supabase...")
                    # Используем уже проверенное подключение
                    
                    if use_separated_tables:
                        # Используем 4 отдельные таблицы
                        st.info("🔄 Загрузка в 4 отдельные таблицы...")
                        
                        # Загружаем студентов
                        if not upload_students_to_supabase(supabase, student_list):
                            st.error("❌ Не удалось загрузить студентов")
                            st.stop()
                        
                        # Загружаем курсы
                        if not upload_all_courses_to_supabase(supabase, course_data_list, course_names):
                            st.error("❌ Не удалось загрузить курсы")
                            st.stop()
                        
                        success = True
                    else:
                        # Используем объединенную таблицу
                        success = upload_to_supabase(supabase, consolidated_data)
                    
                    if success:
                        st.success("🎉 Вся обработка завершена успешно!")
                        st.balloons()
                    else:
                        st.error("❌ Не удалось загрузить в Supabase")
    
    with col2:
        st.header("ℹ️ Информация")
        
        # Кнопка проверки подключения
        st.subheader("🔍 Проверка подключения")
        if st.button("🔍 Проверить Supabase", type="secondary"):
            supabase = authenticate_supabase()
            if supabase:
                check_supabase_connection(supabase)
        
        st.markdown("---")
        st.markdown("""
        **Системные требования:**
        - Список студентов в формате Excel (.xlsx, .xls) или CSV (.csv)
        - Данные курсов в формате CSV (.csv) или Excel (.xlsx, .xls) со столбцами:
          - `Корпоративная почта`
          - `Процент завершения`
        - Настроенное подключение к Supabase
        
        **Результат:**
        - Консолидированные данные загружены в базу данных Supabase
        - Информация о студентах с процентами завершения
        - Нулевые значения для студентов, не найденных в данных курсов
        
        **Этапы обработки:**
        1. Загрузка и валидация списка студентов
        2. Обработка трех файлов курсов
        3. Консолидация данных по email
        4. Генерация статистики завершения
        5. Загрузка в базу данных Supabase
        
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

