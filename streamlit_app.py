"""
Streamlit Application for Course Analytics Processing
Upload files and process course data automatically with Supabase database
"""
import streamlit as st
import pandas as pd
from supabase import create_client, Client
from io import StringIO

# Page configuration
st.set_page_config(
    page_title="Обработка аналитики курсов - Supabase",
    page_icon="📊",
    layout="wide"
)

# ==============================
# НОВАЯ НЕЗАВИСИМАЯ ФУНКЦИЯ ОБНОВЛЕНИЯ ТАБЛИЦЫ STUDENTS
# ==============================
def update_students_table_in_supabase(supabase: Client, student_ pd.DataFrame):
    """
    Обновляет только таблицу 'students' в Supabase.
    Не связано с загрузкой данных курсов.
    """
    if student_data is None or student_data.empty:
        st.warning("⚠️ Нет данных для обновления таблицы студентов.")
        return False

    st.info("👥 Обновление таблицы 'students' в Supabase...")
    records = []
    seen_emails = set()

    for _, row in student_data.iterrows():
        email = str(row.get('Корпоративная почта', '')).strip().lower()
        if not email or '@edu.hse.ru' not in email:
            continue
        if email in seen_emails:
            continue
        seen_emails.add(email)

        record = {
            'корпоративная_почта': email,
            'фио': str(row.get('ФИО', '')).strip() or None,
            'филиал_кампус': str(row.get('Филиал (кампус)', '')).strip() or None,
            'факультет': str(row.get('Факультет', '')).strip() or None,
            'образовательная_программа': str(row.get('Образовательная программа', '')).strip() or None,
            'версия_образовательной_программы': str(row.get('Версия образовательной программы', '')).strip() or None,
            'группа': str(row.get('Группа', '')).strip() or None,
            'курс': str(row.get('Курс', '')).strip() or None,
        }
        records.append(record)

    if not records:
        st.warning("⚠️ После фильтрации не осталось валидных записей студентов.")
        return False

    # Получаем существующие email из таблицы students
    try:
        existing = supabase.table('students').select('корпоративная_почта').execute()
        existing_emails = {r['корпоративная_почта'].lower() for r in existing.data} if existing.data else set()
    except Exception as e:
        st.error(f"❌ Ошибка при чтении таблицы 'students': {e}")
        return False

    to_insert = []
    to_update = []

    for rec in records:
        email = rec['корпоративная_почта']
        if email in existing_emails:
            to_update.append(rec)
        else:
            to_insert.append(rec)

    success = True
    batch_size = 200

    if to_insert:
        st.info(f"➕ Вставка {len(to_insert)} новых студентов...")
        for i in range(0, len(to_insert), batch_size):
            batch = to_insert[i:i + batch_size]
            try:
                supabase.table('students').upsert(batch, on_conflict='корпоративная_почта').execute()
            except Exception as e:
                st.error(f"❌ Ошибка вставки студентов: {e}")
                success = False

    if to_update:
        st.info(f"🔄 Обновление {len(to_update)} студентов...")
        for i in range(0, len(to_update), batch_size):
            batch = to_update[i:i + batch_size]
            try:
                supabase.table('students').upsert(batch, on_conflict='корпоративная_почта').execute()
            except Exception as e:
                st.error(f"❌ Ошибка обновления студентов: {e}")
                success = False

    if success:
        st.success(f"✅ Таблица 'students' обновлена: {len(records)} записей")
    return success


# ==============================
# ФУНКЦИЯ ЗАГРУЗКИ КУРСОВ (БЕЗ СТУДЕНТОВ)
# ==============================
def upload_all_courses_to_supabase(supabase: Client, course_data_list, course_names):
    """
    Загружает данные курсов в соответствующие таблицы (course_cg, course_python, course_analysis).
    Не затрагивает таблицу students.
    """
    table_mapping = {
        'ЦГ': 'course_cg',
        'Питон': 'course_python',
        'Андан': 'course_analysis'
    }

    for course_data, course_name in zip(course_data_list, course_names):
        table_name = table_mapping.get(course_name)
        if not table_name:
            st.error(f"❌ Неизвестный курс: {course_name}")
            return False

        if course_data is None or course_data.empty:
            st.warning(f"⚠️ Нет данных для курса {course_name}")
            continue

        st.info(f"📈 Загрузка данных курса {course_name} в таблицу {table_name}...")

        records = []
        seen_emails = set()
        for _, row in course_data.iterrows():
            email = str(row.get('Корпоративная почта', '')).strip().lower()
            if not email or '@edu.hse.ru' not in email:
                continue
            if email in seen_emails:
                continue
            seen_emails.add(email)

            percent_col = f'Процент_{course_name}'
            progress = row.get(percent_col)
            if pd.isna(progress):
                progress = None
            else:
                try:
                    progress = float(progress)
                except (ValueError, TypeError):
                    progress = None

            records.append({
                'корпоративная_почта': email,
                'процент_завершения': progress
            })

        if not records:
            st.info(f"📋 Нет валидных записей для курса {course_name}")
            continue

        batch_size = 200
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                supabase.table(table_name).upsert(batch, on_conflict='корпоративная_почта').execute()
            except Exception as e:
                st.error(f"❌ Ошибка загрузки курса {course_name}, пакет {i//batch_size + 1}: {e}")
                return False

        st.success(f"✅ Курс {course_name} загружен: {len(records)} записей")

    return True


# ==============================
# АУТЕНТИФИКАЦИЯ SUPABASE
# ==============================
def authenticate_supabase():
    """Аутентификация с Supabase используя Streamlit secrets"""
    try:
        if not hasattr(st, 'secrets') or "supabase" not in st.secrets:
            st.error("❌ Секреты Supabase не найдены в конфигурации Streamlit")
            st.error("💡 Для развертывания настройте секреты SUPABASE_URL и SUPABASE_KEY")
            return None
        supabase_url = st.secrets["supabase"]["url"]
        supabase_key = st.secrets["supabase"]["key"]
        supabase: Client = create_client(supabase_url, supabase_key)
        st.success("✅ Аутентификация Supabase успешна")
        return supabase
    except Exception as e:
        st.error(f"❌ Ошибка аутентификации Supabase: {str(e)}")
        return None


# ==============================
# ЗАГРУЗКА И ОБРАБОТКА ДАННЫХ
# ==============================
def load_student_list(uploaded_file):
    """Load student list from uploaded Excel or CSV file"""
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
            st.error("Неподдерживаемый формат файла. Используйте Excel (.xlsx, .xls) или CSV (.csv)")
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
        st.error(f"Ошибка загрузки списка студентов: {str(e)}")
        return None


def extract_course_data(uploaded_file, course_name):
    """Extract email and completion percentage from uploaded course file"""
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
            st.success(f"📊 Фильтрация ЦГ: исключено {excluded_count} колонок, включено {included_count} колонок из {total_relevant_columns} проанализированных")

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
            if completion_
                result_df = pd.DataFrame(completion_data)
                result_df.columns = ['Корпоративная почта', f'Процент_{course_name}']
                st.success(f"✅ Рассчитан процент завершения для {len(result_df)} студентов курса {course_name} на основе {len(timestamp_columns)} заданий")
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
            if completion_
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
        st.error(f"Ошибка обработки данных курса {course_name}: {str(e)}")
        return None


def consolidate_data(student_list, course_data_list, course_names):
    """Consolidate all course data with student list and deduplication"""
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
            st.success(f"✅ Удалено {removed_count} дубликатов. Осталось {final_count} уникальных записей")
        else:
            st.success(f"✅ Дубликаты не обнаружены. Всего {final_count} уникальных записей")
        return consolidated
    except Exception as e:
        st.error(f"Error consolidating data: {str(e)}")
        return None


# ==============================
# ОСНОВНАЯ ФУНКЦИЯ
# ==============================
def main():
    st.title("📊 Обработка аналитики курсов")
    st.markdown("Загрузите файлы и обработайте аналитику курсов автоматически с сохранением в Supabase")

    # Sidebar for file uploads
    st.sidebar.header("📁 Загрузка файлов")
    st.sidebar.info("🔄 Используются разделенные таблицы")

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
            st.info("Пожалуйста, загрузите все необходимые файлы:")
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
                    supabase = authenticate_supabase()
                    if supabase is None:
                        st.error("❌ Не удалось подключиться к Supabase")
                        st.stop()

                    st.info("📚 Загрузка списка студентов...")
                    student_list = load_student_list(student_file)
                    if student_list is None:
                        st.stop()
                    st.success(f"✅ Загружено {len(student_list)} записей студентов")

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
                    st.success(f"✅ Данные консолидированы: {len(consolidated_data)} записей")

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
                    if summary_
                        summary_df = pd.DataFrame(summary_data)
                        st.subheader("📋 Сводная таблица по курсам")
                        st.table(summary_df)

                    # ЗАГРУЗКА В SUPABASE — ТОЛЬКО РАЗДЕЛЁННЫЕ ТАБЛИЦЫ
                    st.info("💾 Обновление базы данных Supabase...")
                    # 🔹 Сначала — только студенты
                    if not update_students_table_in_supabase(supabase, student_list):
                        st.error("❌ Не удалось обновить таблицу 'students'")
                        st.stop()
                    # 🔹 Затем — только курсы
                    if not upload_all_courses_to_supabase(supabase, course_data_list, course_names):
                        st.error("❌ Не удалось загрузить данные курсов")
                        st.stop()

                    st.success("🎉 Вся обработка завершена успешно!")
                    st.balloons()

    with col2:
        st.header("ℹ️ Информация")
        if st.button("🔍 Проверить Supabase", type="secondary"):
            supabase = authenticate_supabase()
            if supabase:
                st.success("✅ Подключение работает")

        st.markdown("---")
        st.markdown("""
        **Системные требования:**
        - Список студентов в формате Excel (.xlsx, .xls) или CSV (.csv)
        - Данные курсов в формате CSV (.csv) или Excel (.xlsx, .xls)
        - Настроенное подключение к Supabase
        **Результат:**
        - Таблица `students` обновляется отдельно
        - Прогресс по курсам — в отдельных таблицах
        - Нет дублирования логики
        """)

if __name__ == "__main__":
    main()
