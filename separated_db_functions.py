"""
Функции для работы с 4 отдельными таблицами в Supabase:
1. students - список студентов
2. course_cg - прогресс по курсу ЦГ 
3. course_python - прогресс по курсу Python
4. course_analysis - прогресс по курсу Анализ данных
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import time

def create_separated_tables(supabase):
    """Создание 4 отдельных таблиц: студенты + 3 курса"""
    try:
        st.info("🔧 Создание 4 отдельных таблиц...")
        
        # Таблица студентов
        students_sql = """
        CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            корпоративная_почта TEXT UNIQUE NOT NULL,
            фио TEXT NOT NULL,
            филиал_кампус TEXT,
            факультет TEXT,
            образовательная_программа TEXT,
            версия_образовательной_программы TEXT,
            группа TEXT,
            курс TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_students_email ON students(корпоративная_почта);
        """
        
        # Таблица курса ЦГ
        course_cg_sql = """
        CREATE TABLE IF NOT EXISTS course_cg (
            id SERIAL PRIMARY KEY,
            корпоративная_почта TEXT UNIQUE NOT NULL,
            процент_завершения REAL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_course_cg_email ON course_cg(корпоративная_почта);
        """
        
        # Таблица курса Python
        course_python_sql = """
        CREATE TABLE IF NOT EXISTS course_python (
            id SERIAL PRIMARY KEY,
            корпоративная_почта TEXT UNIQUE NOT NULL,
            процент_завершения REAL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_course_python_email ON course_python(корпоративная_почта);
        """
        
        # Таблица курса Анализ данных
        course_analysis_sql = """
        CREATE TABLE IF NOT EXISTS course_analysis (
            id SERIAL PRIMARY KEY,
            корпоративная_почта TEXT UNIQUE NOT NULL,
            процент_завершения REAL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_course_analysis_email ON course_analysis(корпоративная_почта);
        """
        
        # Представление для объединения всех данных
        analytics_view_sql = """
        CREATE OR REPLACE VIEW course_analytics_full AS
        SELECT 
            s.id as student_id,
            s.корпоративная_почта,
            s.фио,
            s.филиал_кампус,
            s.факультет,
            s.образовательная_программа,
            s.версия_образовательной_программы,
            s.группа,
            s.курс,
            cg.процент_завершения as процент_цг,
            cp.процент_завершения as процент_питон,
            ca.процент_завершения as процент_андан,
            s.created_at,
            s.updated_at
        FROM students s
        LEFT JOIN course_cg cg ON s.корпоративная_почта = cg.корпоративная_почта
        LEFT JOIN course_python cp ON s.корпоративная_почта = cp.корпоративная_почта
        LEFT JOIN course_analysis ca ON s.корпоративная_почта = ca.корпоративная_почта;
        """
        
        # Выполняем SQL команды
        tables = [
            ('students', students_sql),
            ('course_cg', course_cg_sql),
            ('course_python', course_python_sql),
            ('course_analysis', course_analysis_sql)
        ]
        
        for table_name, sql in tables:
            try:
                supabase.rpc('exec_sql', {'sql': sql}).execute()
                st.success(f"✅ Таблица '{table_name}' создана")
            except Exception as e:
                st.error(f"❌ Ошибка создания таблицы {table_name}: {str(e)}")
                st.code(sql, language='sql')
                return False
        
        # Создаем представление
        try:
            supabase.rpc('exec_sql', {'sql': analytics_view_sql}).execute()
            st.success("✅ Представление 'course_analytics_full' создано")
        except Exception as e:
            st.error(f"❌ Ошибка создания представления: {str(e)}")
            st.code(analytics_view_sql, language='sql')
            return False
        
        st.success("🎉 Все 4 отдельные таблицы созданы успешно!")
        return True
        
    except Exception as e:
        st.error(f"❌ Общая ошибка создания 4 отдельных таблиц: {str(e)}")
        return False

def upload_students_to_supabase(supabase, student_data):
    """
    Загрузка данных студентов в таблицу students с использованием оптимизированного UPSERT
    Основано на рекомендациях Supabase документации для максимальной производительности
    """
    try:
        st.info("👥 Загрузка данных студентов (UPSERT)...")
        
        # Подготовка данных для upsert
        records_for_upsert = []
        processed_emails = set()
        
        for _, row in student_data.iterrows():
            email = str(row.get('Корпоративная почта', '')).strip().lower()
            if not email or '@edu.hse.ru' not in email:
                continue
            
            # Пропускаем дубликаты в текущих данных
            if email in processed_emails:
                continue
            processed_emails.add(email)
                
            student_record = {
                'корпоративная_почта': email,
                'фио': str(row.get('ФИО', 'Неизвестно')).strip() or 'Неизвестно',
                'филиал_кампус': str(row.get('Филиал (кампус)', '')) if pd.notna(row.get('Филиал (кампус)')) and str(row.get('Филиал (кампус)', '')).strip() else None,
                'факультет': str(row.get('Факультет', '')) if pd.notna(row.get('Факультет')) and str(row.get('Факультет', '')).strip() else None,
                'образовательная_программа': str(row.get('Образовательная программа', '')) if pd.notna(row.get('Образовательная программа')) and str(row.get('Образовательная программа', '')).strip() else None,
                'версия_образовательной_программы': str(row.get('Версия образовательной программы', '')) if pd.notna(row.get('Версия образовательной программы')) and str(row.get('Версия образовательной программы', '')).strip() else None,
                'группа': str(row.get('Группа', '')) if pd.notna(row.get('Группа')) and str(row.get('Группа', '')).strip() else None,
                'курс': str(row.get('Курс', '')) if pd.notna(row.get('Курс')) and str(row.get('Курс', '')).strip() else None,
            }
            records_for_upsert.append(student_record)
        
        if not records_for_upsert:
            st.info("📋 Нет записей для обработки")
            return True
        
        st.info(f"📋 Подготовлено {len(records_for_upsert)} записей для UPSERT")
        
        # Оптимальная обработка батчами
        batch_size = 200  # Рекомендуемый размер согласно документации
        total_processed = 0
        
        for i in range(0, len(records_for_upsert), batch_size):
            batch = records_for_upsert[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = ((len(records_for_upsert) - 1) // batch_size) + 1
            
            try:
                # UPSERT с оптимальными параметрами
                result = supabase.table('students').upsert(
                    batch,
                    on_conflict='корпоративная_почта',  # Конфликт по email
                    ignore_duplicates=False,             # Обновлять при конфликтах
                    returning='minimal'                  # Минимальный ответ
                ).execute()
                
                batch_processed = len(batch)
                total_processed += batch_processed
                
                st.success(f"✅ Батч {batch_num}/{total_batches}: обработано {batch_processed} записей")
                
            except Exception as e:
                error_str = str(e)
                
                # Обработка сетевых ошибок с повторами
                if any(error_pattern in error_str.lower() for error_pattern in [
                    "connectionterminated", "connection", "eof occurred in violation of protocol", 
                    "ssl", "timeout"
                ]):
                    st.warning(f"⚠️ Сетевая ошибка в батче {batch_num}, повторяем...")
                    
                    # Повторная попытка с задержкой
                    import time
                    time.sleep(2)
                    
                    try:
                        result = supabase.table('students').upsert(
                            batch,
                            on_conflict='корпоративная_почта',
                            ignore_duplicates=False,
                            returning='minimal'
                        ).execute()
                        
                        total_processed += len(batch)
                        st.success(f"✅ Батч {batch_num}/{total_batches}: обработано {len(batch)} записей (после повтора)")
                        
                    except Exception as retry_error:
                        st.error(f"❌ Батч {batch_num} не удался после повтора: {str(retry_error)}")
                        return False
                else:
                    st.error(f"❌ Ошибка в батче {batch_num}: {error_str}")
                    return False
        
        st.success(f"🎉 UPSERT завершен успешно! Обработано {total_processed} записей из {len(records_for_upsert)}")
        return True
        
    except Exception as e:
        st.error(f"❌ Критическая ошибка UPSERT: {str(e)}")
def upload_course_data_to_supabase(supabase, course_data, course_name):
    """
    Загрузка данных одного курса в соответствующую таблицу с использованием UPSERT
    Оптимизировано для максимальной производительности по аналогии с upload_students_to_supabase
    """
    try:
        # Определяем таблицу для курса
        table_mapping = {
            'ЦГ': 'course_cg',
            'Питон': 'course_python', 
            'Андан': 'course_analysis'
        }
        
        table_name = table_mapping.get(course_name)
        if not table_name:
            st.error(f"❌ Неизвестный курс: {course_name}")
            return False
            
        st.info(f"📈 Загрузка данных курса {course_name} в таблицу {table_name} (UPSERT)...")
        
        if course_data is None or course_data.empty:
            st.warning(f"⚠️ Нет данных для курса {course_name}")
            return True
        
        # Подготовка данных для upsert
        records_for_upsert = []
        processed_emails = set()
        
        for _, row in course_data.iterrows():
            email = str(row.get('Корпоративная почта', '')).strip().lower()
            if not email or '@edu.hse.ru' not in email:
                continue
                
            # Пропускаем дубликаты в текущих данных
            if email in processed_emails:
                continue
            processed_emails.add(email)
            
            # Ищем колонку с процентом для данного курса
            percent_col = f'Процент_{course_name}'
            progress_value = None
            
            if percent_col in row and pd.notna(row[percent_col]) and row[percent_col] != '':
                try:
                    progress_value = float(row[percent_col])
                except (ValueError, TypeError):
                    progress_value = None
            
            course_record = {
                'корпоративная_почта': email,
                'процент_завершения': progress_value
            }
            records_for_upsert.append(course_record)
        
        if not records_for_upsert:
            st.info(f"📋 Нет записей для обработки в курсе {course_name}")
            return True
        
        st.info(f"📋 Подготовлено {len(records_for_upsert)} записей для UPSERT в курсе {course_name}")
        
        # Оптимальная обработка батчами
        batch_size = 200  # Рекомендуемый размер согласно документации Supabase
        total_processed = 0
        
        for i in range(0, len(records_for_upsert), batch_size):
            batch = records_for_upsert[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = ((len(records_for_upsert) - 1) // batch_size) + 1
            
            try:
                # UPSERT с оптимальными параметрами
                result = supabase.table(table_name).upsert(
                    batch,
                    on_conflict='корпоративная_почта',  # Конфликт по email
                    ignore_duplicates=False,             # Обновлять при конфликтах
                    returning='minimal'                  # Минимальный ответ
                ).execute()
                
                batch_processed = len(batch)
                total_processed += batch_processed
                
                st.success(f"✅ Курс {course_name} - Батч {batch_num}/{total_batches}: обработано {batch_processed} записей")
                
            except Exception as e:
                error_str = str(e)
                
                # Обработка сетевых ошибок с повторами
                if any(error_pattern in error_str.lower() for error_pattern in [
                    'connection', 'timeout', 'network', 'ssl', 'eof occurred in violation of protocol'
                ]):
                    retry_success = False
                    for retry_attempt in range(3):
                        try:
                            time.sleep(2 ** retry_attempt)  # Экспоненциальная задержка: 1s, 2s, 4s
                            result = supabase.table(table_name).upsert(
                                batch,
                                on_conflict='корпоративная_почта',
                                ignore_duplicates=False,
                                returning='minimal'
                            ).execute()
                            
                            batch_processed = len(batch)
                            total_processed += batch_processed
                            st.success(f"✅ Курс {course_name} - Батч {batch_num}/{total_batches}: обработано {batch_processed} записей (после повтора)")
                            retry_success = True
                            break
                            
                        except Exception as retry_error:
                            if retry_attempt == 2:  # Последняя попытка
                                st.error(f"❌ Курс {course_name} - Батч {batch_num}: сетевая ошибка после 3 попыток: {str(retry_error)}")
                            continue
                    
                    if not retry_success:
                        return False
                else:
                    st.error(f"❌ Курс {course_name} - Батч {batch_num}: {str(e)}")
                    return False
        
        st.success(f"🎉 Курс {course_name}: успешно обработано {total_processed} записей с использованием UPSERT")
        return True
        
    except Exception as e:
        st.error(f"❌ Общая ошибка загрузки курса {course_name}: {str(e)}")
        return False

def upload_all_courses_to_supabase(supabase, course_data_list, course_names):
    """Загрузка всех курсов в отдельные таблицы"""
    try:
        st.info("📚 Загрузка всех курсов в отдельные таблицы...")
        
        success_count = 0
        for course_data, course_name in zip(course_data_list, course_names):
            if upload_course_data_to_supabase(supabase, course_data, course_name):
                success_count += 1
        
        if success_count == len(course_names):
            st.success(f"🎉 Все {success_count} курса загружены успешно!")
            return True
        else:
            st.error(f"❌ Загружено только {success_count} из {len(course_names)} курсов")
            return False
        
    except Exception as e:
        st.error(f"❌ Ошибка загрузки всех курсов: {str(e)}")
        return False