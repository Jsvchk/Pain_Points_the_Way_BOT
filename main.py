import logging
import asyncio
import json
from typing import List, Dict, Any

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ContentType,
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from write_answer_google_exel import connect_to_sheet, save_answer, save_video_link
from aiogram.enums import ChatAction

# =========================
# Logging
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = "8425701456:AAH1lA7aO96lav1ckThQN6RNJE4j0r518LY"
JSON_PATH_RU = "data.json"          # RU dataset (current file)
JSON_PATH_EN = "data_en.json"       # EN dataset (add this file with English translation)

# =========================
# Data utils
# =========================

async def send_with_typing(
    message: Message,
    text: str,
    pause: float | None = None,
    reply_markup: InlineKeyboardMarkup | None = None,
    *,
    min_pause: float = 0.4,
    max_pause: float = 2.0,
    chars_per_second: float = 40.0,
):
    """Simulate human typing with an adaptive pause.

    If `pause` is None, it is computed as:
        clamp(len(text) / chars_per_second, min_pause, max_pause)
    You can override pacing per message by passing a custom `pause`.
    """
    try:
        await message.bot.send_chat_action(chat_id=message.chat.id, action=ChatAction.TYPING)
    except Exception:
        pass

    # Auto-calc pause if not provided
    if pause is None:
        try:
            calc = len(text) / chars_per_second if text else min_pause
        except Exception:
            calc = min_pause
        pause = max(min_pause, min(max_pause, calc))

    await asyncio.sleep(pause)
    return await message.answer(text, reply_markup=reply_markup)

def load_data(file_path: str) -> Dict[str, Any]:
    """Завантажує дані з JSON файлу."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Файл {file_path} не знайдено!")
        return {}
    except json.JSONDecodeError:
        logger.error(f"Помилка парсингу JSON у файлі {file_path}!")
        return {}

# load both datasets once
DATA = {
    "ru": load_data(JSON_PATH_RU),
    "en": load_data(JSON_PATH_EN),
}

# i18n strings
STR: Dict[str, Dict[str, str]] = {
    "ru": {
        "start_prompt": "Здравствуйте! Please choose your language / Пожалуйста, выберите язык:",
        "hello": (
            "Здравствуйте! 👋😊\n"
            "Я бот 🤖, который поможет вам пройти небольшой опросное тестирование 📝 "
            "и подобрать наилучшие упражнения 🏋️‍♂️🧘‍♀️ для улучшения вашего состояния 💪🌿.\n\n"
            "В процессе опроса мы узнаем 🔍, где и при каких условиях возникает боль 😣, "
            "а затем я предложу подборку видео 🎥 с упражнениями 🏃‍♀️.\n\n"
            "Также вы сможете прислать мне видео 📹 с выполнением упражнений для обратной связи 💬✅."
        ),
        "choose_zone": "Пожалуйста, выберите область, где вы чувствуете боль:",
        "ask_trigger": "Когда возникает боль? Выберите подходящие варианты:",
        "survey_done": (
            "🎉 Отлично! Вы прошли опрос!\n\n"
            "Я не врач, но могу предложить упражнения, которые, исходя из ваших ответов, "
            "могут помочь чувствовать себя лучше и заботиться о своем теле."
        ),
        "forming_plan": "⏳ Формирую ваш персональный план: {p}%",
        "got_answers": "Получил ваши ответы и могу дать небольшие рекомендации:\n\n",
        "no_recs": "Пока без конкретных рекомендаций.",
        "plan_and_video": "Рекомендую комплекс упражнений и видео:\n\n",
        "video_label": "🎥 **Видео с упражнениями:** {url}\n\n",
        "plan_foot": "Желательно выполнять их регулярно, и помни — это только рекомендации, чтобы тебе было комфортнее и приятнее двигаться! 😉",
        "pay_prompt": "💬 Ты можешь получить разбор твоего видео от специалиста:\n\nВыбери, что интересно:",
        "pay_v1": "Разбор одного видео — 149 kr",
        "pay_v2": "Повторный разбор — 199 kr",
        "pay_sub": "Подписка на архив — 49 kr/мес",
        "pay_mar": "Участие в марафоне — 99 kr",
        "pay_thanks": "Спасибо! 🧾 Вы выбрали: {choice}\n\nОплата пока не подключена — кнопки носят информационный характер. Можете отправить видео для разбора прямо здесь.",
        "send_video_q": "Хотите прислать видео своих упражнений, чтобы я мог дать ещё более точные рекомендации?",
        "send_video_yes": "Да, хочу 🎥",
        "send_video_no": "Нет, спасибо ✨",
        "video_ok": "Принял видео, спасибо! Сохранил ссылку у себя (file_id).",
        "send_video_prompt": "Отлично! Отправьте видео, где вы выполняете упражнения, и я дам рекомендации.",
        "bye": "Хорошо! Если возникнут вопросы - обращайтесь! 😊",
    },
    "en": {
        "start_prompt": "Hello! Please choose your language:",
        "hello": (
            "Hello! 👋😊\n"
            "I will help you take a short survey 📝 and select suitable exercises 🏋️‍♂️🧘‍♀️ to improve your condition 💪🌿.\n\n"
            "We will find out 🔍 where and under what circumstances you experience pain 😣, and then I will provide exercise videos 🎥🏃‍♀️.\n\n"
            "You can also send me a video 📹 of you doing the exercises for feedback 💬✅."
        ),
        "choose_zone": "Please choose the area where you feel pain:",
        "ask_trigger": "When does the pain occur? Choose the appropriate options:",
        "survey_done": (
            "🎉 Great! You’ve completed the survey!\n\n"
            "I’m not a doctor, but I can suggest exercises that — based on your answers — may help you feel better and take care of your body."
        ),
        "forming_plan": "⏳ Creating your personal plan: {p}%",
        "got_answers": "I’ve received your answers and can give you a few brief recommendations:\n\n",
        "no_recs": "No specific recommendations yet.",
        "plan_and_video": "I recommend a set of exercises and a video:\n\n",
        "video_label": "🎥 **Exercise video:** {url}\n\n",
        "plan_foot": "Do them regularly; these are suggestions to keep you comfortable and moving! 😉",
        "pay_prompt": "💬 You can get a specialist’s review of your video:\n\nChoose what’s interesting:",
        "pay_v1": "Single video review — 149 kr",
        "pay_v2": "Repeat review — 199 kr",
        "pay_sub": "Exercise archive subscription — 49 kr/mo",
        "pay_mar": "Marathon participation — 99 kr",
        "pay_thanks": "Thanks! 🧾 You chose: {choice}\n\nPayments aren’t connected yet — buttons are informational. You can send a video for review right here.",
        "send_video_q": "Want to send your exercise video so I can give more precise recommendations?",
        "send_video_yes": "Yes, send 🎥",
        "send_video_no": "No, thanks ✨",
        "video_ok": "Got your video, thanks! Saved its file_id.",
        "send_video_prompt": "Great! Send the video of you doing the exercises and I’ll give you feedback.",
        "bye": "Alright! If you have questions — just ask! 😊",
    },
}

# helpers to pick dataset/strings by state language
def get_dataset_from_state_data(state_data: Dict[str, Any]) -> Dict[str, Any]:
    lang = state_data.get("language", "ru")
    return DATA.get(lang) or DATA.get("ru") or {}


# --- Helper: persist all answers to Sheets at once ---
async def persist_survey_to_sheets(state: FSMContext, user_id: int, username: str):
    """Save all collected answers for the current user in one go to avoid UI lags.
    Runs blocking gspread I/O in a background thread without nested event loops.
    """
    sd = await state.get_data()
    zone = sd.get("selected_zone", "")
    answers: list[dict[str, str]] = sd.get("answers", []) or []

    def _sync_write():
        spreadsheet = connect_to_sheet()
        answers_ws = spreadsheet.worksheet("answers")
        # write each Q/A once
        for item in answers:
            for q, a in item.items():
                save_answer(answers_ws, user_id, username or "", zone, q, a)

    try:
        # Perform blocking I/O off the event loop (no asyncio.run inside!)
        await asyncio.to_thread(_sync_write)
    except Exception as e:
        logger.exception(f"[SHEETS] persist_survey_to_sheets failed: {e}")


def get_unique_zones(d: Dict[str, Any]) -> List[str]:
    return list(d.get("zones", {}).keys())


def get_pain_options(d: Dict[str, Any]) -> List[str]:
    return list(d.get("triggers", {}).keys())


def build_zones_keyboard(d: Dict[str, Any], page: int = 0) -> InlineKeyboardMarkup:
    zones = get_unique_zones(d)
    ZONES_PER_PAGE = 8
    start_index = page * ZONES_PER_PAGE
    end_index = start_index + ZONES_PER_PAGE

    builder = InlineKeyboardBuilder()
    for i, zone in enumerate(zones[start_index:end_index], start=start_index):
        builder.button(text=zone, callback_data=f"zone_idx:{i}")
    builder.adjust(2)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"zones_page:{page-1}"))
    if end_index < len(zones):
        nav.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"zones_page:{page+1}"))
    if nav:
        builder.row(*nav)

    return builder.as_markup()


def build_trigger_keyboard(d: Dict[str, Any]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    triggers = get_pain_options(d)
    for i, trigger in enumerate(triggers):
        builder.button(text=trigger, callback_data=f"trg_idx:{i}")
    builder.adjust(2)
    return builder.as_markup()



# New helper: build_answers_keyboard
def build_answers_keyboard(options: List[Dict[str, str]], question_index: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for i, answer_dict in enumerate(options):
        answer_text = list(answer_dict.keys())[0]
        builder.button(text=answer_text, callback_data=f"answer:{question_index}:{i}")
    builder.adjust(2)
    return builder.as_markup()

def build_payment_keyboard(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=STR[lang]["pay_v1"], callback_data="pay:v1")
    builder.button(text=STR[lang]["pay_v2"], callback_data="pay:v2")
    builder.button(text=STR[lang]["pay_sub"], callback_data="pay:sub")
    builder.button(text=STR[lang]["pay_mar"], callback_data="pay:mar")
    builder.adjust(1)
    return builder.as_markup()

# --- Video decision keyboard builder ---
def build_video_decision_keyboard(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=STR[lang]["send_video_yes"], callback_data="send_video_yes")
    builder.button(text=STR[lang]["send_video_no"], callback_data="send_video_no")
    builder.adjust(2)
    return builder.as_markup()

# =========================
# FSM
# =========================
class LanguageForm(StatesGroup):
    choose_language = State()


class PainForm(StatesGroup):
    choose_zone = State()
    ask_trigger = State()
    ask_questions = State()


# =========================
# Router
# =========================
router = Router()


# =========================
# Handlers
# =========================

@router.message(Command("start"))
async def choose_language(message: Message, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="Русский 🇷🇺", callback_data="lang:ru")
    builder.button(text="English 🇬🇧", callback_data="lang:en")
    builder.adjust(2)
    await message.answer(
        STR["ru"]["start_prompt"],
        reply_markup=builder.as_markup(),
    )
    await state.set_state(LanguageForm.choose_language)

# --- Restart handler ---
@router.message(Command("restart"))
async def restart_bot(message: Message, state: FSMContext):
    # Clear any conversation state and start from language selection
    await state.clear()
    await message.answer("🔁 Перезапуск бота...")
    await choose_language(message, state)


@router.callback_query(LanguageForm.choose_language, F.data.startswith("lang:"))
async def set_language(call: CallbackQuery, state: FSMContext):
    lang = call.data.split(":", 1)[1]
    await state.update_data(language=lang)

    await call.message.answer(STR.get(lang, STR["ru"])["hello"])

    await start_zone_selection(call.message, state)
    await call.answer()


async def start_zone_selection(message: Message, state: FSMContext):
    logger.info("👉 Запуск вибору зони")
    await state.update_data(zones_page=0, answers=[])
    sd = await state.get_data()
    d = get_dataset_from_state_data(sd)
    zones = get_unique_zones(d)
    if not zones:
        # fallback to RU dataset if EN is empty/missing
        d = DATA.get("ru") or {}
        await state.update_data(language=sd.get("language", "ru"))
        logger.warning("[i18n] Dataset for selected language is empty, fallback to RU")
    lang = sd.get("language", "ru")
    kb = build_zones_keyboard(d, page=0)
    await send_with_typing(message, STR[lang]["choose_zone"], reply_markup=kb)
    await state.set_state(PainForm.choose_zone)


@router.callback_query(PainForm.choose_zone, F.data.startswith("zones_page:"))
async def on_zones_page(call: CallbackQuery, state: FSMContext):
    page = int(call.data.split(":", 1)[1])
    await state.update_data(zones_page=page)
    sd = await state.get_data()
    d = get_dataset_from_state_data(sd)
    if not get_unique_zones(d):
        d = DATA.get("ru") or {}
    kb = build_zones_keyboard(d, page=page)
    await call.message.edit_reply_markup(reply_markup=kb)
    await call.answer()


@router.callback_query(PainForm.choose_zone, F.data.startswith("zone_idx:"))
async def zone_selected(call: CallbackQuery, state: FSMContext):
    idx = int(call.data.split(":", 1)[1])
    sd = await state.get_data()
    d = get_dataset_from_state_data(sd)
    if not get_unique_zones(d):
        d = DATA.get("ru") or {}
    zones = get_unique_zones(d)
    zone = zones[idx] if 0 <= idx < len(zones) else ""
    await state.update_data(selected_zone=zone, answers=[])
    await ask_trigger_selection(call.message, state)
    await call.answer()


async def ask_trigger_selection(message: Message, state: FSMContext):
    sd = await state.get_data()
    d = get_dataset_from_state_data(sd)
    lang = sd.get("language", "ru")
    kb = build_trigger_keyboard(d)
    await send_with_typing(message, STR[lang]["ask_trigger"], reply_markup=kb)
    await state.set_state(PainForm.ask_trigger)


@router.callback_query(PainForm.ask_trigger, F.data.startswith("trg_idx:"))
async def trigger_selected(call: CallbackQuery, state: FSMContext):
    idx = int(call.data.split(":", 1)[1])
    sd = await state.get_data()
    d = get_dataset_from_state_data(sd)
    triggers = get_pain_options(d)
    trigger = triggers[idx] if 0 <= idx < len(triggers) else ""

    user_data = await state.get_data()
    answers = user_data.get("answers", [])
    answers.append({"trigger": trigger})
    await state.update_data(answers=answers)

    questions = list(d.get("questions", {}).keys())
    await state.update_data(questions_list=questions, current_question_index=0)

    await ask_next_question(call.message, state)
    await call.answer()


async def ask_next_question(message: Message, state: FSMContext):
    user_data = await state.get_data()
    current_index = user_data.get("current_question_index", 0)
    questions = user_data.get("questions_list", [])
    sd = await state.get_data()
    d = get_dataset_from_state_data(sd)

    if current_index >= len(questions):
        await finish_survey(message, state)
        return

    current_question = questions[current_index]
    q_value = d.get("questions", {}).get(current_question)

    # Determine if the question has a condition (object) or is a simple list of options
    if isinstance(q_value, dict):
        condition = q_value.get("condition", {})
        options = q_value.get("options", [])

        # Check gender condition if present
        required_gender = condition.get("gender")
        if required_gender:
            # gender can be stored explicitly or inferred from answers
            gender = (user_data.get("gender")
                      or next((v for a in user_data.get("answers", []) for k, v in a.items() if k == "Яка у вас стать?"), None))
            if gender != required_gender:
                # Skip this question, advance to the next one
                await state.update_data(current_question_index=current_index + 1)
                await ask_next_question(message, state)
                return
    else:
        options = q_value or []

    # Build and send keyboard for the current question
    kb = build_answers_keyboard(options, current_index)
    await send_with_typing(message, current_question, reply_markup=kb)
    await state.set_state(PainForm.ask_questions)


@router.callback_query(PainForm.ask_questions, F.data.startswith("answer:"))
async def answer_selected(call: CallbackQuery, state: FSMContext):
    _, q_idx, a_idx = call.data.split(":", 2)
    q_i = int(q_idx)
    a_i = int(a_idx)

    user_data = await state.get_data()
    questions = user_data.get("questions_list", [])
    answers = user_data.get("answers", [])
    sd = await state.get_data()
    d = get_dataset_from_state_data(sd)

    current_question = questions[q_i]
    q_value = d["questions"][current_question]
    if isinstance(q_value, dict):
        options = q_value.get("options", [])
    else:
        options = q_value
    answer_data = options[a_i]
    answer_text = list(answer_data.keys())[0]

    # Persist explicit gender for conditional logic
    if current_question == "Яка у вас стать?":
        await state.update_data(gender=answer_text)

    answers.append({current_question: answer_text})
    await state.update_data(answers=answers, current_question_index=q_i + 1)

    await call.answer()
    await ask_next_question(call.message, state)


async def finish_survey(message: Message, state: FSMContext):
    sd = await state.get_data()
    d = get_dataset_from_state_data(sd)
    lang = sd.get("language", "ru")
    user_data = await state.get_data()
    answers = user_data.get("answers", [])
    selected_zone = user_data.get("selected_zone", "")

    await send_with_typing(
        message,
        STR[lang]["survey_done"],
    )

    loading_msg = await message.answer(STR[lang]["forming_plan"].format(p=0))
    for i in range(0, 101, 20):
        await asyncio.sleep(0.3)
        try:
            await loading_msg.edit_text(STR[lang]["forming_plan"].format(p=i))
        except Exception:
            pass

    await asyncio.sleep(4.3)

    zone_info = d.get("zones", {}).get(selected_zone, {})
    video_url = zone_info.get("video", "")

    # 1) РЕКОМЕНДАЦИИ ПО ОТВЕТАМ
    rec_lines = []
    for answer in answers:
        for question, answer_text in answer.items():
            if question in d.get("questions", {}):
                q_value = d["questions"][question]
                options = q_value.get("options", []) if isinstance(q_value, dict) else q_value
                for answer_dict in options or []:
                    if answer_text in answer_dict:
                        recommendation = answer_dict[answer_text]
                        rec_lines.append(f"💡 **{question}** — {answer_text}: {recommendation}")

    rec_text = STR[lang]["got_answers"] + ("\n".join(rec_lines) if rec_lines else STR[lang]["no_recs"])
    await send_with_typing(message, rec_text)

    await asyncio.sleep(3.2)

    # 2) ПЛАН И ВИДЕО
    plan_text = (
        STR[lang]["plan_and_video"]
        + f"{zone_info.get('text', '')}\n\n"
    )
    if video_url:
        plan_text += STR[lang]["video_label"].format(url=video_url)
    plan_text += STR[lang]["plan_foot"]

    await send_with_typing(message, plan_text)

    await asyncio.sleep(0.8)

    # Persist all answers once (non-blocking)
    try:
        user = message.from_user
        await persist_survey_to_sheets(state, user.id, user.username or "")
    except Exception as e:
        logger.exception(f"[SHEETS] batch save failed: {e}")

    # 3) ПРЕДЛОЖЕНИЕ ПРИСЛАТЬ ВИДЕО (сразу после результатов)
    await asyncio.sleep(0.6)
    await send_with_typing(
        message,
        STR[lang]["send_video_q"],
        reply_markup=build_video_decision_keyboard(lang),
    )

    # 4) ОПЛАТНЫЕ ОПЦИИ (после предложения прислать видео)
    await asyncio.sleep(1.0)
    await send_with_typing(
        message,
        STR[lang]["pay_prompt"],
        reply_markup=build_payment_keyboard(lang),
    )


@router.callback_query(F.data.in_({"send_video_yes", "send_video_no"}))
async def handle_video_decision(call: CallbackQuery, state: FSMContext):
    sd = await state.get_data()
    lang = sd.get("language", "ru")
    if call.data == "send_video_yes":
        await send_with_typing(call.message, STR[lang]["send_video_prompt"]) 
        await state.update_data(waiting_for_video=True)
    else:
        await send_with_typing(call.message, STR[lang]["bye"]) 
        await state.clear()
    await call.answer()


@router.message(F.content_type == ContentType.VIDEO)
async def handle_video(message: Message, state: FSMContext):
    user_data = await state.get_data()
    lang = user_data.get("language", "ru")
    if not user_data.get("waiting_for_video"):
        return

    file_id = message.video.file_id if message.video else None

    # Save video link (file_id) to Google Sheets
    try:
        spreadsheet = connect_to_sheet()
        answers_ws = spreadsheet.worksheet("answers")
        user = message.from_user
        user_id = user.id
        username = user.username or ""
        zone_for_sheet = user_data.get("selected_zone", "")
        if file_id:
            save_video_link(answers_ws, user_id, username, zone_for_sheet, file_id)
    except Exception as e:
        logger.exception(f"[SHEETS] save_video_link failed: {e}")

    await send_with_typing(message, STR[lang]["video_ok"]) 
    await state.clear()


@router.callback_query(F.data.startswith("pay:"))
async def handle_payment_choice(call: CallbackQuery, state: FSMContext):
    sd = await state.get_data()
    lang = sd.get("language", "ru")
    mapping = {
        "pay:v1": STR[lang]["pay_v1"],
        "pay:v2": STR[lang]["pay_v2"],
        "pay:sub": STR[lang]["pay_sub"],
        "pay:mar": STR[lang]["pay_mar"],
    }
    kind = call.data
    chosen = mapping.get(kind, "вариант")
    await state.update_data(selected_offer=kind)

    # Сообщаем, что оплата пока не подключена, и предлагаем перейти к отправке видео
    await send_with_typing(call.message, STR[lang]["pay_thanks"].format(choice=chosen))

    # показать клавиатуру с предложением отправить видео (те же кнопки, что были до этого)
    await send_with_typing(
        call.message,
        STR[lang]["send_video_q"],
        reply_markup=build_video_decision_keyboard(lang),
    )
    await call.answer()


# Optional: catch-all callback debugger
@router.callback_query()
async def debug_any_callback(call: CallbackQuery, state: FSMContext):
    # Якщо сюди потрапив колбек — значить жоден із специфічних фільтрів не спрацював
    try:
        current_state = await state.get_state()
    except Exception:
        current_state = None
    logger.info(f"[DEBUG CATCH-ALL v3] data={call.data} | state={current_state}")
    await call.answer()


# =========================
# Entrypoint
# =========================
async def main():
    base = DATA.get("ru") or {}
    if not base:
        logger.error("Не вдалося завантажити дані з JSON файлу!")
    else:
        logger.info("Дані успішно завантажені")
        logger.info(f"Знайдено зон: {len(base.get('zones', {}))}")
        logger.info(f"Знайдено питань: {len(base.get('questions', {}))}")

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())