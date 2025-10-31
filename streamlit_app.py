"""
Streamlit Application for Course Analytics Processing
Upload files and process course data automatically with Supabase database
"""
import streamlit as st
import pandas as pd
from supabase import create_client, Client
from io import StringIO
import time
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Обработка аналитики курсов - Supabase",
    page_icon="📊",
    layout="wide"
)

# ==============================
# ФУНКЦИИ ДЛЯ РАБОТЫ С РАЗДЕЛЁННЫМИ ТАБЛИЦАМИ (встроены)
# ==============================

def upload_students_to_supabase(supabase, student_data):
    """
    Загрузка данных студентов в таблицу students с использованием оптимизированного UPSERT
    """
    try:
        st.info("👥 Загрузка данных студентов (UPSERT)...")
        records_for_upsert = []
        processed_emails = set()
        
        for _, row in student_data.iterrows():
            email = str(row.get('Корпоративная почта', '')).strip().lower()
            if not email or '@edu.hse.ru' not in email:
                continue
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
        batch_size = 200
        total_processed = 0
        
        for i in range(0, len(records_for_upsert), batch_size):
            batch = records_for_upsert[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = ((len(records_for_upsert) - 1) // batch_size) + 1
            
            try:
                result = supabase.table('students').upsert(
                    batch,
                    on_conflict='корпоративная_почта',
                    ignore_duplicates=False,
                    returning='minimal'
                ).execute()
                total_processed += len(batch)
                st.success(f"✅ Батч {batch_num}/{total_batches}: обработано {len(batch)} записей")
            except Exception as e:
                error_str = str(e)
                if any(pat in error_str.lower() for pat in ["connection", "timeout", "ssl", "eof"]):
                    st.warning(f"⚠️ Сетевая ошибка в батче {batch_num}, повтор...")
                    time.sleep(2)
                    try:
                        result = supabase.table('students').upsert(batch, on_conflict='корпоративная_почта').execute()
                        total_processed += len(batch)
                        st.success(f"✅ Батч {batch_num} (после повтора)")
                    except Exception as retry_error:
                        st.error(f"❌ Батч {batch_num} не удался после повтора: {retry_error}")
                        return False
                else:
                    st.error(f"❌ Ошибка в батче {batch_num}: {e}")
                    return False
        
        st.success(f"🎉 UPSERT завершён! Обработано {total_processed} записей")
        return True
    except Exception as e:
        st.error(f"❌ Критическая ошибка UPSERT студентов: {e}")
        return False


def upload_course_data_to_supabase(supabase, course_data, course_name):
    """Загрузка данных одного курса в соответствующую таблицу"""
    try:
        table_mapping = {'ЦГ': 'course_cg', 'Питон': 'course_python', 'Андан': 'course_analysis'}
        table_name = table_mapping.get(course_name)
        if not table_name:
            st.error(f"❌ Неизвестный курс: {course_name}")
            return False
            
        st.info(f"📈 Загрузка курса {course_name} в {table_name}...")
        if course_data is None or course_data.empty:
            st.warning(f"⚠️ Нет данных для курса {course_name}")
            return True

        records_for_upsert = []
        processed_emails = set()
        for _, row in course_data.iterrows():
            email = str(row.get('Корпоративная почта', '')).strip().lower()
            if not email or '@edu.hse.ru' not in email:
                continue
            if email in processed_emails:
                continue
            processed_emails.add(email)
            
            percent_col = f'Процент_{course_name}'
            progress_value = None
            if percent_col in row and pd.notna(row[percent_col]) and row[percent_col] != '':
                try:
                    progress_value = float(row[percent_col])
                except (ValueError, TypeError):
                    progress_value = None
            
            records_for_upsert.append({
                'корпоративная_почта': email,
                'процент_завершения': progress_value
            })
        
        if not records_for_upsert:
            st.info(f"📋 Нет записей для курса {course_name}")
            return True

        batch_size = 200
        total_processed = 0
        for i in range(0, len(records_for_upsert), batch_size):
            batch = records_for_upsert[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            try:
                supabase.table(table_name).upsert(batch, on_conflict='корпоративная_почта').execute()
                total_processed += len(batch)
                st.success(f"✅ Курс {course_name} - батч {batch_num}: {len(batch)} записей")
            except Exception as e:
                st.error(f"❌ Ошибка загрузки курса {course_name}, батч {batch_num}: {e}")
                return False

        st.success(f"🎉 Курс {course_name}: {total_processed} записей загружено")
        return True
    except Exception as e:
        st.error(f"❌ Ошибка загрузки курса {course_name}: {e}")
        return False


def upload_all_courses_to_supabase(supabase, course_data_list, course_names):
    """Загрузка всех курсов в отдельные таблицы"""
    try:
        st.info("📚 Загрузка всех курсов...")
        success_count = 0
        for course_data, course_name in zip(course_data_list, course_names):
            if upload_course_data_to_supabase(supabase, course_data, course_name):
                success_count += 1
        if success_count == len(course_names):
            st.success(f"🎉 Все {success_count} курса загружены!")
            return True
        else:
            st.error(f"❌ Загружено только {success_count} из {len(course_names)}")
            return False
    except Exception as e:
        st.error(f"❌ Ошибка загрузки всех курсов: {e}")
        return False


# ==============================
# ОСТАЛЬНЫЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==============================

def authenticate_supabase():
    try:
        if not hasattr(st, 'secrets') or "supabase" not in st.secrets:
            st.error("❌ Секреты Supabase не найдены")
            return None
        supabase_url = st.secrets["supabase"]["url"]
        supabase_key = st.secrets["supabase"]["key"]
        supabase: Client = create_client(supabase_url, supabase_key)
        st.success("✅ Аутентификация Supabase успешна")
        return supabase
    except Exception as e:
        st.error(f"❌ Ошибка аутентификации: {e}")
        return None


def load_student_list(uploaded_file):
    try:
        file_name = uploaded_file.name.lower()
        if file_name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(uploaded_file)
        elif file_name.endswith('.csv'):
            content = uploaded_file.getvalue()
            try:
                df = pd.read_csv(StringIO(content.decode('utf-16')), sep='\t')
            except (UnicodeDecodeError, pd.errors.ParserError):
                try:
                    df = pd.read_csv(StringIO(content.decode('utf-8')))
                except UnicodeDecodeError:
                    df = pd.read_csv(StringIO(content.decode('cp1251')))
        else:
            st.error("Неподдерживаемый формат файла")
            return None

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

        found_columns = {}
        df_columns_lower = [str(col).lower().strip() for col in df.columns]
        for target_col, possible_names in required_columns.items():
            for col_idx, col_name in enumerate(df_columns_lower):
                if any(possible_name in col_name for possible_name in possible_names):
                    found_columns[target_col] = df.columns[col_idx]
                    break

        result_df = pd.DataFrame()
        for target_col, source_col in found_columns.items():
            if source_col in df.columns:
                result_df[target_col] = df[source_col]

        if 'Данные о пользователе' in df.columns:
            user_data = df['Данные о пользователе'].astype(str)
            parsed_data = user_data.str.split(';', expand=True)
            if len(parsed_data.columns) >= 4:
                result_df['Факультет'] = parsed_data[0]
                result_df['Образовательная программа'] = parsed_data[1] 
                result_df['Курс'] = parsed_data[2]
                result_df['Группа'] = parsed_data[3]

        for required_col in required_columns.keys():
            if required_col not in result_df.columns:
                if required_col == 'ФИО':
                    result_df[required_col] = None
                else:
                    result_df[required_col] = ''

        if 'Корпоративная почта' in result_df.columns:
            result_df = result_df[result_df['Корпоративная почта'].astype(str).str.contains('@edu.hse.ru', na=False)]
            result_df['Корпоративная почта'] = pd.Series(result_df['Корпоративная почта']).astype(str).str.lower().str.strip()
        return result_df
    except Exception as e:
        st.error(f"Ошибка загрузки списка студентов: {e}")
        return None


def extract_course_data(uploaded_file, course_name):
    try:
        file_name = uploaded_file.name.lower()
        if file_name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(uploaded_file)
        elif file_name.endswith('.csv'):
            content = uploaded_file.getvalue()
            try:
                df = pd.read_csv(StringIO(content.decode('utf-16')), sep='\t')
            except (UnicodeDecodeError, pd.errors.ParserError):
                try:
                    df = pd.read_csv(StringIO(content.decode('utf-8')))
                except UnicodeDecodeError:
                    df = pd.read_csv(StringIO(content.decode('cp1251')))
        else:
            st.error(f"Неподдерживаемый формат файла для курса {course_name}")
            return None

        email_column = None
        possible_email_names = ['Адрес электронной почты', 'Корпоративная почта', 'Email', 'Почта', 'E-mail']
        for col_name in possible_email_names:
            if col_name in df.columns:
                email_column = col_name
                break
        if email_column is None:
            st.error(f"Столбец с email не найден в файле {course_name}")
            return None

        completion_column = None
        possible_completion_names = ['Процент завершения', 'Completion', 'Progress', 'Прогресс', 'Завершение']
        cg_excluded_keywords = [
            'take away', 'шпаргалка', 'консультация', 'общая информация', 'промо-ролик',
            'поддержка студентов', 'пояснение', 'случайный вариант для студентов с овз',
            'материалы по модулю', 'копия', 'демонстрационный вариант', 'спецификация',
            'демо-версия', 'правила проведения независимого экзамена',
            'порядок организации и проведения независимых экзаменов',
            'интерактивный тренажер правил нэ', 'пересдачи в сентябре', 'незрячих и слабовидящих',
            'проекты с использование tei', 'тренировочный тест', 'ключевые принципы tei',
            'базовые возможности tie', 'специальные модули tei', 'будут идентичными',
            'опрос', 'тест по модулю', 'анкета', 'user information', 'страна', 'user_id', 'данные о пользователе'
        ]

        excluded_count = 0
        included_count = 0
        completed_columns = []
        timestamp_columns = []

        for col in df.columns:
            if col not in ['Unnamed: 0', email_column, 'Данные о пользователе', 'User information', 'Страна']:
                if course_name == 'ЦГ':
                    should_exclude = False
                    col_str = str(col).strip().lower()
                    for excluded_keyword in cg_excluded_keywords:
                        if excluded_keyword.lower() in col_str:
                            should_exclude = True
                            excluded_count += 1
                            break
                    if should_exclude:
                        continue
                    included_count += 1

                if not col.startswith('Unnamed:') and len(str(col).strip()) > 0:
                    sample_values = df[col].dropna().astype(str).head(100)
                    if any('Выполнено' in str(val) or 'выполнено' in str(val).lower() for val in sample_values):
                        if not all(str(val) == 'Не выполнено' for val in sample_values if pd.notna(val)):
                            completed_columns.append(col)
                elif col.startswith('Unnamed:') and col != 'Unnamed: 0':
                    sample_values = df[col].dropna().astype(str).head(20)
                    for val in sample_values:
                        val_str = str(val).strip()
                        if any(pattern in val_str for pattern in ['2020', '2021', '2022', '2023', '2024']) and ':' in val_str:
                            timestamp_columns.append(col)
                            break

        if course_name == 'ЦГ':
            total_relevant_columns = excluded_count + included_count
            st.success(f"📊 Фильтрация ЦГ: исключено {excluded_count}, включено {included_count}")

        if timestamp_columns:
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
                    if val_str and val_str != 'nan' and val_str != '':
                        if any(pattern in val_str for pattern in ['2020', '2021', '2022', '2023', '2024']) and ':' in val_str:
                            completed_tasks += 1
                percentage = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                completion_data.append({'email': str(email_val).lower().strip(), 'percentage': percentage})
            if completion_data:
                result_df = pd.DataFrame(completion_data)
                result_df.columns = ['Корпоративная почта', f'Процент_{course_name}']
                st.success(f"✅ Рассчитан процент завершения для {len(result_df)} студентов курса {course_name}")
                return result_df
            else:
                st.warning(f"Не найдено данных о завершении для курса {course_name}")
                return None

        elif completed_columns:
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
                percentage = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                completion_data.append({'email': str(email_val).lower().strip(), 'percentage': percentage})
            if completion_data:
                result_df = pd.DataFrame(completion_data)
                result_df.columns = ['Корпоративная почта', f'Процент_{course_name}']
                st.success(f"✅ Рассчитан процент завершения для {len(result_df)} студентов курса {course_name}")
                return result_df
            else:
                st.warning(f"Не найдено данных о завершении для курса {course_name}")
                return None

        for col_name in possible_completion_names:
            if col_name in df.columns:
                completion_column = col_name
                break
        if completion_column is None:
            st.error(f"Столбец с процентом завершения не найден в файле {course_name}")
            return None

        course_data = df[[email_column, completion_column]].copy()
        course_data.columns = ['Корпоративная почта', f'Процент_{course_name}']
        course_data['Корпоративная почта'] = pd.Series(course_data['Корпоративная почта']).astype(str).str.lower().str.strip()
        email_series = pd.Series(course_data['Корпоративная почта'])
        course_data = course_data[email_series.str.contains('@edu.hse.ru', na=False)]
        return course_data
    except Exception as e:
        st.error(f"Ошибка обработки данных курса {course_name}: {e}")
        return None


def consolidate_data(student_list, course_data_list, course_names):
    try:
        consolidated = student_list.copy()
        for course_data, course_name in zip(course_data_list, course_names):
            if course_data is not None:
                consolidated = pd.merge(consolidated, course_data, on='Корпоративная почта', how='left')
                consolidated[f'Процент_{course_name}'] = consolidated[f'Процент_{course_name}'].where(pd.notna(consolidated[f'Процент_{course_name}']), None)

        st.info("🔍 Проверка и удаление дубликатов...")
        initial_count = len(consolidated)
        email_counts = consolidated['Корпоративная почта'].value_counts()
        duplicates = email_counts[email_counts > 1]
        if len(duplicates) > 0:
            st.warning(f"⚠️ Обнаружено {len(duplicates)} дубликатов email")
            duplicate_list = list(duplicates.index[:5])
            for email in duplicate_list:
                count = duplicates[email]
                st.text(f"  - {email}: {count} записей")
            if len(duplicates) > 5:
                st.text(f"  ... и ещё {len(duplicates) - 5} дубликатов")

        consolidated = consolidated.drop_duplicates(subset=['Корпоративная почта'], keep='first')
        final_count = len(consolidated)
        removed_count = initial_count - final_count
        if removed_count > 0:
            st.success(f"✅ Удалено {removed_count} дубликатов. Осталось {final_count} записей")
        else:
            st.success(f"✅ Дубликаты не обнаружены. Всего {final_count} записей")
        return consolidated
    except Exception as e:
        st.error(f"Error consolidating data: {e}")
        return None


# ==============================
# ОСНОВНАЯ ФУНКЦИЯ
# ==============================
def main():
    st.title("📊 Обработка аналитики курсов")
    st.markdown("Загрузите файлы и обработайте аналитику курсов автоматически с сохранением в Supabase")

    # Sidebar
    st.sidebar.header("📁 Загрузка файлов")
    st.sidebar.info("🔄 Используются разделённые таблицы")

    student_file = st.sidebar.file_uploader(
        "Загрузить список студентов (Excel/CSV)",
        type=['xlsx', 'xls', 'csv']
    )
    st.sidebar.markdown("---")
    st.sidebar.subheader("Файлы курсов")
    course_cg_file = st.sidebar.file_uploader("Курс ЦГ", type=['csv', 'xlsx', 'xls'])
    course_python_file = st.sidebar.file_uploader("Курс Python", type=['csv', 'xlsx', 'xls'])
    course_analysis_file = st.sidebar.file_uploader("Курс Анализ данных", type=['csv', 'xlsx', 'xls'])

    col1, col2 = st.columns([2, 1])
    with col1:
        st.header("📋 Статус обработки")
        files_uploaded = all([
            student_file is not None,
            course_cg_file is not None,
            course_python_file is not None,
            course_analysis_file is not None
        ])
        if not files_uploaded:
            st.info("Пожалуйста, загрузите все файлы:")
            file_status = {
                "Список студентов": "✅" if student_file else "❌",
                "Курс ЦГ": "✅" if course_cg_file else "❌",
                "Курс Python": "✅" if course_python_file else "❌",
                "Курс Анализ данных": "✅" if course_analysis_file else "❌"
            }
            status_df = pd.DataFrame([{"Файл": k, "Статус": v} for k, v in file_status.items()])
            st.table(status_df)
        else:
            st.success("Все файлы загружены! Готово к обработке.")
            if st.button("🚀 Начать обработку", type="primary"):
                with st.spinner("Обработка данных..."):
                    supabase = authenticate_supabase()
                    if supabase is None:
                        st.error("❌ Не удалось подключиться к Supabase")
                        st.stop()

                    st.info("📚 Загрузка списка студентов...")
                    student_list = load_student_list(student_file)
                    if student_list is None:
                        st.stop()
                    st.success(f"✅ Загружено {len(student_list)} записей")

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

                    st.info("🔄 Консолидация данных...")
                    consolidated_data = consolidate_data(student_list, course_data_list, course_names)
                    if consolidated_data is None:
                        st.stop()
                    st.success(f"✅ Консолидировано: {len(consolidated_data)} записей")

                    # Сводная статистика
                    st.info("📋 Генерация сводной статистики...")
                    summary_data = []
                    for course_name in course_names:
                        col_name = f'Процент_{course_name}'
                        if col_name in consolidated_data.columns:
                            course_data = consolidated_data[col_name].dropna()
                            if len(course_data) > 0:
                                avg_completion = course_data.mean()
                                students_100 = len(course_data[course_data == 100.0])
                                students_0 = len(course_data[course_data == 0.0])
                                total_students = len(course_data)
                                summary_data.append({
                                    'Курс': course_name,
                                    'Студентов всего': total_students,
                                    'Средний %': f"{avg_completion:.1f}%",
                                    '100%': students_100,
                                    '0%': students_0
                                })
                    if summary_data:
                        summary_df = pd.DataFrame(summary_data)
                        st.subheader("📋 Сводная таблица по курсам")
                        st.table(summary_df)

                    # 🔥 КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: ТОЛЬКО КУРСЫ!
                    st.info("💾 Обновление данных курсов в Supabase...")
                    if not upload_all_courses_to_supabase(supabase, course_data_list, course_names):
                        st.error("❌ Не удалось загрузить курсы")
                        st.stop()

                    st.success("🎉 Обработка завершена успешно!")
                    st.balloons()

    with col2:
        st.header("ℹ️ Информация")
        if st.button("🔍 Проверить Supabase", type="secondary"):
            supabase = authenticate_supabase()
            if supabase:
                st.success("✅ Подключение работает")

        # 🔁 ОТДЕЛЬНАЯ КНОПКА ДЛЯ СТУДЕНТОВ
        if student_file is not None:
            if st.button("🔄 Обновить список студентов", type="secondary"):
                with st.spinner("Обновление списка студентов..."):
                    supabase = authenticate_supabase()
                    if supabase is None:
                        st.error("❌ Не удалось подключиться к Supabase")
                    else:
                        student_list = load_student_list(student_file)
                        if student_list is not None:
                            if upload_students_to_supabase(supabase, student_list):
                                st.success("✅ Список студентов обновлён!")
                            else:
                                st.error("❌ Не удалось обновить список студентов")
                        else:
                            st.error("❌ Не удалось загрузить список студентов")

        st.markdown("---")
        st.markdown("""
        **Режим работы:**
        - 🚀 **Начать обработку** → обновляет **только курсы**
        - 🔄 **Обновить список студентов** → обновляет **только таблицу `students`**
        - Нет объединённой таблицы
        """)

if __name__ == "__main__":
    main()
