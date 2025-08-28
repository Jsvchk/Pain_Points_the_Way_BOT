import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =======================
# ==== 0. Утиліти ======
# =======================
def normalize_value(val):
    if val is None:
        return ""
    return str(val).strip().lower()

# =======================
# ==== 1. Авторизація ===
# =======================
def connect_to_sheet():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive.file',
             'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("Telegram_bot(Pain_Points_the_Way)_ANSWERS")
    return spreadsheet

# =======================
# ==== 2. Завантаження даних із Google Sheets ====
# =======================
def load_all_from_sheets():
    spreadsheet = connect_to_sheet()
    answers_ws = spreadsheet.worksheet("answers")

    final_questions = {}

# =======================
# ==== 3. Збереження відповіді в таблицю ====
# =======================
df_columns = [
    "id", "ник", "дата_время", "зона_боли",
    "Какой характер боли?",
    "Как долго длится боль?",
    "Когда появилась боль?",
    "Были ли травмы или операции?",
    "Есть ли боль или отдача в другую часть тела?",
    "Усиливается ли боль при движении? Если да — при каком?",
    "Что провоцирует боль?",
    "Есть ли напряжение, онемение или покалывание?",
    "Есть ли отёчность? Когда она появляется?",
    "Есть ли проблемы с дыханием или грудной клеткой?",
    "Есть ли связь боли с менструацией или родами?",
    "Есть ли связь между эмоциями и телом (напряжение при стрессе)?",
    "Есть ли утренняя скованность?",
    "Есть ли хронические заболевания?",
    "Как вы дышите чаще всего?",
    "Как вы обычно сидите, стоите, двигаетесь в течение дня?",
    "Какая у вас работа? Много ли сидите или стоите?",
    "Как работает кишечник?",
    "Принимаете ли вы лекарства? Какие именно?",
    "Чувствуется ли перекос в теле или асимметрия?",
    "video_file_id"
]

def save_answer(answers_ws, user_id, username, zone, question_text, answer):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = answers_ws.get_all_values()

    if not data:
        # Додаємо заголовки, якщо аркуш порожній
        answers_ws.append_row(df_columns)
        data = [df_columns]

    # Індекс стовпця
    try:
        col_index = data[0].index(question_text) + 1  # 1-based
    except ValueError:
        answers_ws.update_cell(1, len(data[0]) + 1, question_text)
        col_index = len(data[0]) + 1

    # Пошук рядка
    row_index = None
    for i, row in enumerate(data[1:], start=2):
        if row[0] == str(user_id) and row[3] == zone:
            row_index = i
            break

    if row_index:
        answers_ws.update_cell(row_index, col_index, answer)
    else:
        new_row = [""] * len(df_columns)
        new_row[0] = user_id
        new_row[1] = username
        new_row[2] = now
        new_row[3] = zone
        new_row[col_index - 1] = answer
        answers_ws.append_row(new_row)

def save_video_link(answers_ws, user_id, username, zone, video_file_id):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = answers_ws.get_all_values()
    if not data:
        answers_ws.append_row(df_columns)
        data = [df_columns]
    try:
        col_index = data[0].index("video_file_id") + 1
    except ValueError:
        answers_ws.update_cell(1, len(data[0]) + 1, "video_file_id")
        col_index = len(data[0]) + 1
    row_index = None
    for i, row in enumerate(data[1:], start=2):
        if row[0] == str(user_id) and row[3] == zone:
            row_index = i
            break
    if row_index:
        answers_ws.update_cell(row_index, col_index, video_file_id)
    else:
        new_row = [""] * len(df_columns)
        new_row[0] = user_id
        new_row[1] = username
        new_row[2] = now
        new_row[3] = zone
        # place video id into its column
        new_row[col_index - 1] = video_file_id
        answers_ws.append_row(new_row)
