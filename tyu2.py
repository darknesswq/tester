import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime, timezone, timedelta
import re
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes, Application, \
    MessageHandler, filters, PreCheckoutQueryHandler
import asyncio
import multiprocessing
import os
import random
import html
import traceback
from selenium.common.exceptions import NoSuchElementException
from concurrent.futures import ThreadPoolExecutor
from yookassa import Configuration, Payment
import uuid
from yookassa import Payment as YooPayment
import openai
import tempfile
import glob
from filelock import FileLock
import threading

# Настройка API ГПТ
USER_STATES_FILE = "user_states.json"
openai.api_key = "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"

# Настройки прокси
# proxy_url = "http://modeler_16nxtr:GZeYVyEQP0LH@45.128.156.22:10934"

# Устанавливаем прокси только для OpenAI
# openai.proxy = {
#     "http": proxy_url,
#     "https": proxy_url,
# }

# shop_id и secret_key для ЮKassa
YOOKASSA_SHOP_ID = '1077915'
YOOKASSA_SECRET_KEY = 'live_Tgu9MW5f2A33-THVKpNAtJRkES0Et7t8_lNmYqd2W8k'

# Инициализация yookassa
Configuration.account_id = '1077915'
Configuration.secret_key = 'live_Tgu9MW5f2A33-THVKpNAtJRkES0Et7t8_lNmYqd2W8k'

# Конфигурационные переменные
TELEGRAM_TOKEN = '8156529655:AAE8wBoWK_qzl5FIErKg5LTSk1ItoZHKAB8'
ADMIN_ID = ['holdstater', 'darkness3625']  # usernames Telegram для админки
COMMENTS_CHAT_ID = -1002575677046  # <-- id чата комментариев

USERS_FILE = 'users.json'
PROMO_FILE = 'promo_codes.json'

# === ДОБАВЛЯЕМ ПЕРЕМЕННЫЕ ДЛЯ CRYPTO PAY ===
CRYPTO_PAY_API_TOKEN = '374172:AA1k9OnA9xF6X5OSoFGT8B9f1RkyhyGmOod'  # Токен Crypto Pay API
CRYPTO_PAY_API_URL = 'https://pay.crypt.bot/api/'  # Для mainnet, для теста: https://testnet-pay.crypt.bot/api/

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tennis_parser.log'),
        logging.StreamHandler()
    ]
)

def parsing_worker(chat_id, context, loop=None):
    parser = TennisParser()
    try:
        # Получаем токен из контекста
        token = TELEGRAM_TOKEN
        
        # Функция для отправки сообщений через прямой HTTP запрос
        def send_message(text, parse_mode=None, reply_markup=None):
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            data = {
                "chat_id": chat_id,
                "text": text
            }
            if parse_mode:
                data["parse_mode"] = parse_mode
            if reply_markup:
                data["reply_markup"] = json.dumps(reply_markup)
            response = requests.post(url, data=data)
            return response.json()
            
        # Функция для отправки файла через прямой HTTP запрос
        def send_document(file_path):
            url = f"https://api.telegram.org/bot{token}/sendDocument"
            with open(file_path, 'rb') as f:
                files = {'document': f}
                data = {'chat_id': chat_id}
                response = requests.post(url, data=data, files=files)
                return response.json()
        
        # Стартовое сообщение
        send_message("Начинаем парсинг Live матчей...")
        
        live_matches = []
        total = 0
        try:
            match_links = parser.get_match_links()
            total = len(match_links)
            send_message(f"Найдено live-матчей: {total}")
            
            for idx, match in enumerate(match_links, 1):
                urls = match['urls'] if 'urls' in match else [match['url']]
                container_home = match.get('home_player', '')
                container_away = match.get('away_player', '')
                found = False
                for link in urls:
                    try:
                        match_info = parser.parse_match_details(link)
                        if match_info:
                            parsed_home = match_info.get('home_player', '').strip().lower()
                            parsed_away = match_info.get('away_player', '').strip().lower()
                            cont_home = container_home.strip().lower()
                            cont_away = container_away.strip().lower()
                            if parsed_home == cont_home and parsed_away == cont_away:
                                match_info['home_player'] = container_home
                                match_info['away_player'] = container_away
                                match_info['source_url'] = link
                                match_info['url'] = link
                                match_info['container_home'] = container_home
                                match_info['container_away'] = container_away
                                live_matches.append(match_info)
                                found = True
                                break
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"Ошибка при парсинге матча {link}: {e}")
                if not found and urls:
                    match_info = parser.parse_match_details(urls[0])
                    if match_info:
                        match_info['home_player'] = container_home
                        match_info['away_player'] = container_away
                        match_info['source_url'] = urls[0]
                        match_info['url'] = urls[0]
                        match_info['container_home'] = container_home
                        match_info['container_away'] = container_away
                        match_info['fallback_link'] = True
                        live_matches.append(match_info)
                if idx % 3 == 0 or idx == total:
                    send_message(f"Обработано {idx}/{total} матчей. Найдено {len(live_matches)} с подходящими данными.")
        except Exception as e:
            send_message(f"Ошибка при получении списка матчей: {e}")
            
        if not live_matches:
            send_message("К сожалению, не удалось найти ни одного live-матча с подходящими данными.")
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_file = f"live_matches_{timestamp}.json"
        parser.save_to_json(live_matches, filename=json_file)
        update_user_state(chat_id, data=live_matches)
        
        send_message(f"Информация о Live матчах сохранена в файл: {json_file}")
        
        try:
            send_document(json_file)
        except Exception as e:
            send_message(f"Не удалось отправить файл: {e}")
            
        msg = (
            'Анализ завершен:\n\n'
            'Рекомендуется ставить <b>ПО СИСТЕМЕ</b>👈🏻\n'
            'Но каждые 20-30 минут обновляйте подбор и статистику.\n'
            'Цифры меняются\n<b>Можешь задать вопрос в чат и помощник ответит</b>\n'
        )
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "ПО СИСТЕМЕ", "url": "https://t.me/holdstat/14858"}],
                [{"text": "ЕЩЁ ОДИН ПОДБОР", "callback_data": "pay_crypto"}]
            ]
        }
        
        send_message(msg, parse_mode="HTML", reply_markup=keyboard)
        
        # Запускаем анализ фаворитов
        try:
            # Создаем простой объект для совместимости с filter_and_send_favorites
            class SimpleTelegramBot:
                def __init__(self, token):
                    self.token = token
                    
                async def send_message(self, chat_id, text, parse_mode=None, disable_web_page_preview=None, reply_markup=None):
                    url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                    data = {
                        "chat_id": chat_id,
                        "text": text
                    }
                    if parse_mode:
                        data["parse_mode"] = parse_mode
                    if disable_web_page_preview is not None:
                        data["disable_web_page_preview"] = disable_web_page_preview
                    if reply_markup:
                        data["reply_markup"] = json.dumps(reply_markup)
                    response = requests.post(url, data=data)
                    return response.json()
            
            # Запускаем фильтрацию фаворитов в отдельном потоке
            def run_filter():
                try:
                    simple_bot = SimpleTelegramBot(token)
                    asyncio.run(parser.filter_and_send_favorites(live_matches, simple_bot, chat_id))
                except Exception as e:
                    print(f"Ошибка при фильтрации фаворитов: {e}")
                    send_message(f"Ошибка при фильтрации фаворитов: {e}")
            
            threading.Thread(target=run_filter).start()
        except Exception as e:
            print(f"Ошибка при запуске фильтрации фаворитов: {e}")
            send_message(f"Ошибка при запуске фильтрации фаворитов: {e}")
            
    finally:
        parser.close()



def clean_html_for_telegram(text):
    import re
    import html as html_lib
    # Оставляем только <b> и </b>, все остальные <...> экранируем
    # Экранируем все < кроме <b> и </b>
    text = re.sub(r'<(?!/?b>)', '&lt;', text)
    text = re.sub(r'(?<!<)/b>', '&gt;/b&gt;', text)
    # Проверяем парность тегов
    if text.count('<b>') != text.count('</b>'):
        text = text.replace('<b>', '**').replace('</b>', '**')
    # Экранируем все оставшиеся < и >
    text = html_lib.escape(text)
    # Возвращаем обратно разрешённые теги
    text = text.replace('&lt;b&gt;', '<b>').replace('&lt;/b&gt;', '</b>')
    return text


# Функция для создания платежа через ЮKassa и отправки ссылки на оплату (СБП и др.)
async def send_sbp_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    amount = '299.00'
    description = 'Подбор лучших 3 матчей на основе статистики в реальном времени'
    return_url = 'https://t.me/'  # Можно указать ссылку на бота или канал
    email = 'user@example.com'
    try:
        payment = Payment.create({
            "amount": {
                "value": amount,
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": return_url
            },
            "capture": True,
            "description": description,
            "receipt": {
                "items": [
                    {
                        "description": "Подбор лучших 3 матчей",
                        "quantity": "1.00",
                        "amount": {
                            "value": amount,
                            "currency": "RUB"
                        },
                        "vat_code": 1,
                        "payment_subject": "service",
                        "payment_mode": "full_prepayment"
                    }
                ],
                "email": email
            }
        }, uuid.uuid4())
        confirmation_url = payment.confirmation.confirmation_url
        msg = (
            '💸 <b>Оплата через ЮKassa (СБП, карты и др.)</b>\n\n'
            'Перейдите по ссылке для оплаты:\n'
            f'<a href="{confirmation_url}">Оплатить</a>'
        )
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_web_page_preview=False)
        # Запускаем polling статуса платежа
        await poll_yookassa_payment_status(context, chat_id, payment.id)
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f'Ошибка при создании платежа через ЮKassa: {e}')


async def poll_yookassa_payment_status(context, chat_id, payment_id, max_attempts=40, delay=15):
    for attempt in range(max_attempts):
        payment = YooPayment.find_one(payment_id)
        if payment.status == 'succeeded':
            await context.bot.send_message(chat_id=chat_id, text='✅ Оплата получена! Запускаю подбор...')
            return
        await asyncio.sleep(delay)
    await context.bot.send_message(chat_id=chat_id,
                                   text='❌ Время ожидания оплаты истекло. Если вы оплатили — напишите в поддержку.')


class TennisParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://www.flashscorekz.com"
        self.url = f"{self.base_url}/tennis/"
        self.live_url = f"{self.base_url}/live/tennis/"
        self.setup_driver()

    def setup_driver(self):
        """Настройка Chrome WebDriver"""
        try:
            start_time = time.time()
            self.logger.info("Начало инициализации WebDriver...")
            chrome_options = ChromeOptions()
            # user-data-dir больше не используется
            chrome_options.add_argument('--headless')  # Временно убираем headless для диагностики
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--remote-debugging-port=0')
            service = ChromeService(executable_path='/usr/local/bin/chromedriver')
            self.logger.info("Создаю экземпляр Chrome WebDriver...")
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.logger.info("WebDriver создан, устанавливаю таймаут...")
            self.driver.set_page_load_timeout(40)
            end_time = time.time()
            self.logger.info(f"WebDriver успешно инициализирован за {end_time - start_time:.2f} секунд")
        except Exception as e:
            self.logger.error(f"Ошибка при инициализации WebDriver: {str(e)}\n{traceback.format_exc()}")
            raise

    def get_participant_name(self, elem):
        """Универсально собирает имя игрока или команды (включая парные/командные матчи), разделяет длинным тире"""
        try:
            parts = []
            for child in elem.find_elements(By.XPATH, ".//*"):
                txt = child.text.strip()
                if txt:
                    parts.append(txt)
            if not parts:
                return elem.text.strip()
            return " — ".join(parts)
        except Exception as e:
            self.logger.error(f"Ошибка при извлечении имени игрока: {e}")
            return elem.text.strip()

    def get_match_links(self):
        """Получение ссылок и игроков для live матчей (универсально для одиночных, парных, командных, поддержка нескольких ссылок в контейнере)"""
        try:
            self.logger.info(f"Загрузка специальной страницы LIVE для тенниса: {self.live_url}")
            try:
                self.logger.info(f"Пробую self.driver.get({self.live_url})")
                self.driver.get(self.live_url)
                self.logger.info(f"Страница {self.live_url} загружена, жду 5 секунд для рендера")
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Ошибка загрузки страницы {self.live_url}: {e}\n{traceback.format_exc()}")
                return []
            try:
                self.logger.info("Ожидание элемента sportName (WebDriverWait 3 сек)")
                WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "sportName"))
                )
                self.logger.info("Специальная страница LIVE для тенниса загружена")
            except Exception as e:
                self.logger.warning(f"Не удалось загрузить специальную страницу LIVE: {str(e)}\n{traceback.format_exc()}")
                alt_live_url = f"{self.url}?type=live"
                self.logger.info(f"Пробуем альтернативную страницу LIVE: {alt_live_url}")
                try:
                    self.logger.info(f"Пробую self.driver.get({alt_live_url})")
                    self.driver.get(alt_live_url)
                    self.logger.info(f"Страница {alt_live_url} загружена, жду 5 секунд для рендера")
                    time.sleep(5)
                except Exception as e:
                    self.logger.error(f"Ошибка загрузки страницы {alt_live_url}: {e}\n{traceback.format_exc()}")
                    return []
                try:
                    self.logger.info("Ожидание элемента sportName (WebDriverWait 3 сек) для альтернативной страницы")
                    WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "sportName"))
                    )
                    self.logger.info("Альтернативная страница LIVE загружена")
                except Exception as e:
                    self.logger.error(f"Не удалось загрузить альтернативную страницу LIVE: {str(e)}\n{traceback.format_exc()}")
                    return []
            self.logger.info("Собираю контейнеры live-матчей...")
            matches = []
            try:
                match_elements = self.driver.find_elements(By.CSS_SELECTOR, ".event__match.event__match--live")
                self.logger.info(f"Найдено {len(match_elements)} live-контейнеров матчей")
            except Exception as e:
                self.logger.error(f"Ошибка при поиске live-контейнеров: {e}\n{traceback.format_exc()}")
                return []
            for match in match_elements:
                try:
                    home_elem = match.find_element(By.CSS_SELECTOR, ".event__participant--home")
                    away_elem = match.find_element(By.CSS_SELECTOR, ".event__participant--away")
                    home = self.get_participant_name(home_elem)
                    away = self.get_participant_name(away_elem)
                    link_elems = match.find_elements(By.CSS_SELECTOR, "a[href*='match/']")
                    links = [a.get_attribute("href") for a in link_elems if a.get_attribute("href")]
                    if links and home and away:
                        matches.append({"urls": links, "home_player": home, "away_player": away})
                except Exception as e:
                    self.logger.error(f"Ошибка при сборе данных матча: {e}\n{traceback.format_exc()}")
                    continue
            self.logger.info(f"Всего собрано {len(matches)} live-матчей с игроками и ссылками (возможно несколько ссылок на матч)")
            return matches
        except Exception as e:
            self.logger.error(f"Ошибка при получении списка матчей: {str(e)}\n{traceback.format_exc()}")
            return []

    def parse_serve_stats(self):
        """Парсинг статистики подачи"""
        start_time = time.time()
        self.logger.info("Начало парсинга статистики подачи...")
        try:
            serve_stats = {}
            # Сначала попробуем найти и открыть вкладку "Статистика"
            try:
                self.logger.info("Пробуем найти и открыть вкладку статистики")

                # Используем JavaScript для поиска и клика по вкладке статистики
                stats_tab_clicked = self.driver.execute_script("""
                    // Список селекторов для вкладки со статистикой
                    const statsTabs = [
                        'a[href*="#/match-statistics"]',
                        'a[data-tab-id*="statistics"]',
                        'a[data-tab="statistics"]',
                        'button[data-testid="wcl-tab"]',
                        'div[class*="tab"][class*="statistics"]',
                        'a[href*="statistics"]',
                        'li[data-tab="statistics"]',
                        'button[data-tab="statistics"]',
                        'a:contains("Статистика")',
                        'a:contains("Statistics")',
                        'div[class*="tab"]:contains("Статистика")',
                        'div[class*="tab"]:contains("Statistics")'
                    ];

                    for (const selector of statsTabs) {
                        try {
                            const tabs = document.querySelectorAll(selector);
                            for (const tab of tabs) {
                                // Проверяем, что текст вкладки содержит ключевые слова, связанные со статистикой
                                const text = tab.textContent.toLowerCase();
                                if (text.includes('stat') || text.includes('стат')) {
                                    tab.click();
                                    return true;
                                }
                            }
                        } catch (e) {
                            // Продолжаем перебор, если селектор не сработал
                            continue;
                        }
                    }

                    // Ищем конкретный элемент из примера
                    const specificButton = document.querySelector('button[data-testid="wcl-tab"][role="tab"]');
                    if (specificButton && specificButton.textContent.includes('Статистика')) {
                        specificButton.click();
                        return true;
                    }

                    // Если не нашли по селекторам, пробуем найти по тексту
                    const allLinks = document.querySelectorAll('a, div[role="tab"], li[role="tab"], button[role="tab"]');
                    for (const link of allLinks) {
                        const text = link.textContent.toLowerCase();
                        if (text.includes('stat') || text.includes('стат')) {
                            link.click();
                            return true;
                        }
                    }

                    return false;
                """)

                if stats_tab_clicked:
                    self.logger.info("Успешно открыта вкладка со статистикой")
                    # Ждем загрузки содержимого вкладки
                    time.sleep(2)
                else:
                    self.logger.info("Вкладка со статистикой не найдена, продолжаем парсинг текущей страницы")

                    # Попробуем открыть статистику по URL
                    current_url = self.driver.current_url
                    if "#/match-statistics" not in current_url:
                        stats_url = current_url.split("#")[0] + "#/match-summary/match-statistics"
                        self.logger.info(f"Переходим на страницу статистики по URL: {stats_url}")
                        self.driver.get(stats_url)
                        time.sleep(2)
            except Exception as e:
                self.logger.warning(f"Ошибка при попытке открыть вкладку статистики: {str(e)}")

            # Ждем загрузки секции статистики
            self.logger.info("Ожидание загрузки секции статистики...")
            try:
                WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div[data-analytics-context='tab-match-statistics']"))
                )
                self.logger.info("Секция статистики загружена")
            except Exception as e:
                self.logger.error(f"Не удалось дождаться секции статистики: {str(e)}")
                return {}

            # Находим секцию подачи - сначала пробуем новый селектор
            self.logger.info("Поиск секции подачи...")
            try:
                serve_section = None
                # Пробуем найти секцию по новому селектору
                try:
                    serve_section = self.driver.find_element(By.XPATH,
                                                             "//div[@data-analytics-context='tab-match-statistics']//div[contains(@class, 'section')][.//div[contains(@class, 'sectionHeader') and contains(text(), 'Подача')]]")
                    self.logger.info("Секция подачи найдена по новому селектору")
                except Exception as e:
                    self.logger.warning(f"Не удалось найти секцию подачи по новому селектору: {str(e)}")

                # Если не нашли по новому, пробуем альтернативные селекторы
                if not serve_section:
                    try:
                        serve_section = self.driver.find_element(By.XPATH,
                                                                 "//div[contains(@class, 'section')][.//div[contains(@class, 'section__title') and contains(text(), 'Подача')]]")
                        self.logger.info("Секция подачи найдена по альтернативному селектору")
                    except Exception as e:
                        self.logger.warning(f"Не удалось найти секцию подачи по альтернативному селектору: {str(e)}")

                # Если все еще не нашли, попробуем более общий селектор
                if not serve_section:
                    try:
                        sections = self.driver.find_elements(By.XPATH,
                                                             "//div[contains(@class, 'section')]")
                        for section in sections:
                            try:
                                header = section.find_element(By.XPATH,
                                                              ".//div[contains(@class, 'sectionHeader') or contains(@class, 'section__title')]")
                                if 'подача' in header.text.lower():
                                    serve_section = section
                                    self.logger.info("Секция подачи найдена по общему селектору и тексту заголовка")
                                    break
                            except:
                                continue
                    except Exception as e:
                        self.logger.warning(f"Не удалось найти секцию подачи по общему селектору: {str(e)}")

                # Еще один метод - ищем новый формат статистики
                if not serve_section:
                    try:
                        all_stats = self.driver.find_elements(By.CSS_SELECTOR, ".statName_Kj5Sw, .statCategory_y0tVC")
                        for stat in all_stats:
                            if any(keyword in stat.text.lower() for keyword in
                                   ['перв', 'подач', 'подачи навылет', 'эйс', 'двойн']):
                                # Нашли статистику подачи в новом формате
                                self.logger.info("Найдена статистика подачи в новом формате")
                                serve_section = stat.find_element(By.XPATH,
                                                                  "./ancestor::div[contains(@class, 'section') or contains(@class, 'wcl-category')]")
                                break
                    except Exception as e:
                        self.logger.warning(f"Не удалось найти статистику подачи в новом формате: {str(e)}")

                # Если нашли секцию подачи, извлекаем данные
                if serve_section:
                    # Пробуем найти все строки статистики в этой секции
                    try:
                        stat_rows = serve_section.find_elements(By.CSS_SELECTOR,
                                                                "[class*='statisticsRow'], [class*='statRow'], .wcl-row_OFViZ")
                        self.logger.info(f"Найдено {len(stat_rows)} строк статистики подачи")

                        for row in stat_rows:
                            try:
                                # Находим категорию (тип статистики)
                                category_elem = row.find_element(By.CSS_SELECTOR,
                                                                 "[class*='category'], [class*='statCategory'], .wcl-category_7qsgP")
                                category = category_elem.text.strip()

                                if not category or category.lower() == 'подача':
                                    continue  # Пропускаем заголовок секции

                                # Находим значения для домашнего и гостевого игрока
                                home_elem = row.find_element(By.CSS_SELECTOR,
                                                             "[class*='home'], [class*='player1'], .wcl-homePlayer_HRiEa")
                                away_elem = row.find_element(By.CSS_SELECTOR,
                                                             "[class*='away'], [class*='player2'], .wcl-awayPlayer_CZE9L")

                                home_value = home_elem.text.strip()
                                away_value = away_elem.text.strip()

                                # Проверяем, содержат ли значения проценты и дополнительные данные
                                if '(' in home_value and ')' in home_value:
                                    # Формат: 70% (14/20)
                                    home_percent = re.search(r'(\d+)%', home_value)
                                    home_details = re.search(r'\((.*?)\)', home_value)

                                    if home_percent and home_details:
                                        home_data = {
                                            'value': f"{home_percent.group(1)}%",
                                            'details': home_details.group(1)
                                        }
                                    else:
                                        home_data = home_value
                                else:
                                    home_data = home_value

                                if '(' in away_value and ')' in away_value:
                                    # Формат: 70% (14/20)
                                    away_percent = re.search(r'(\d+)%', away_value)
                                    away_details = re.search(r'\((.*?)\)', away_value)

                                    if away_percent and away_details:
                                        away_data = {
                                            'value': f"{away_percent.group(1)}%",
                                            'details': away_details.group(1)
                                        }
                                    else:
                                        away_data = away_value
                                else:
                                    away_data = away_value

                                # Сохраняем данные
                                serve_stats[category] = {
                                    'home': home_data,
                                    'away': away_data
                                }

                                self.logger.info(f"Найдена статистика подачи: {category} - {home_data} | {away_data}")
                            except Exception as e:
                                self.logger.debug(f"Ошибка при обработке строки статистики подачи: {str(e)}")
                                continue
                    except Exception as e:
                        self.logger.error(f"Ошибка при поиске строк статистики подачи: {str(e)}")
                else:
                    self.logger.warning("Секция подачи не найдена")

                    # Пробуем еще один способ - ищем статистику в новом формате напрямую
                    try:
                        all_stat_rows = self.driver.find_elements(By.CSS_SELECTOR,
                                                                  "[data-testid='wcl-statistics'], div[class*='statisticsRow'], div[class*='statRow']")

                        for row in all_stat_rows:
                            try:
                                row_text = row.text.lower()
                                if any(keyword in row_text for keyword in ['эйс', 'подач', 'двойн', 'перв']):
                                    # Получаем категорию и значения
                                    try:
                                        category_text = re.search(r'^(.*?)(?:\d|\s\d)', row_text)
                                        category = category_text.group(1).strip() if category_text else "Неизвестно"

                                        # Извлекаем числовые значения
                                        values = re.findall(r'(\d+%|\d+/\d+|\d+)', row_text)
                                        if len(values) >= 2:
                                            home_value = values[0]
                                            away_value = values[-1]

                                            serve_stats[category] = {
                                                'home': home_value,
                                                'away': away_value
                                            }

                                            self.logger.info(
                                                f"Найдена статистика подачи: {category} - {home_value} | {away_value}")
                                    except Exception as e:
                                        self.logger.debug(f"Ошибка при извлечении данных из строки: {str(e)}")
                                        continue
                            except Exception as e:
                                self.logger.debug(f"Ошибка при проверке строки статистики: {str(e)}")
                                continue
                    except Exception as e:
                        self.logger.error(f"Ошибка при прямом поиске статистики подачи: {str(e)}")

            except Exception as e:
                self.logger.error(f"Ошибка при поиске секции подачи: {str(e)}")

            total_time = time.time() - start_time
            self.logger.info(
                f"Парсинг статистики подачи завершен за {total_time:.2f} секунд. Найдено {len(serve_stats)} параметров")

            return serve_stats

        except Exception as e:
            self.logger.error(f"Критическая ошибка при парсинге статистики подачи: {str(e)}")
            traceback_str = traceback.format_exc()
            self.logger.error(f"Стек вызовов: {traceback_str}")
            return {}

    def parse_game_stats(self):
        """Парсинг статистики возврата и очков"""
        start_time = time.time()
        self.logger.info("Начало парсинга статистики возврата и очков...")
        try:
            # Мы не вызываем здесь переход на вкладку статистики,
            # так как предполагается, что это уже было сделано в методе parse_serve_stats

            self.logger.info("Поиск секций возврата и очков...")
            game_stats = {}

            # Ищем все секции статистики
            try:
                sections = self.driver.find_elements(By.CSS_SELECTOR,
                                                     "div[class*='section'], div[class*='wcl-category'], div[data-testid='wcl-category']")
                self.logger.info(f"Найдено {len(sections)} секций статистики")

                for section in sections:
                    try:
                        # Получаем заголовок секции
                        header_elem = section.find_element(By.CSS_SELECTOR,
                                                           "div[class*='header'], div[class*='Header'], div[class*='title'], div[class*='wcl-category']")
                        header_text = header_elem.text.strip().lower()

                        # Игнорируем секцию подачи, так как она уже обработана в parse_serve_stats
                        if 'подача' in header_text or 'serve' in header_text:
                            continue

                        # Извлекаем название секции
                        section_name = header_elem.text.strip()

                        # Ищем все строки статистики в этой секции
                        stat_rows = section.find_elements(By.CSS_SELECTOR,
                                                          "[class*='statisticsRow'], [class*='statRow'], [data-testid='wcl-statistics'], .wcl-row_OFViZ")

                        # Парсим каждую строку
                        for row in stat_rows:
                            try:
                                # Находим категорию (тип статистики)
                                category_elem = row.find_element(By.CSS_SELECTOR,
                                                                 "[class*='category'], [class*='statCategory'], .wcl-category_7qsgP")
                                category = category_elem.text.strip()

                                # Пропускаем заголовки
                                if not category or category.lower() == section_name.lower():
                                    continue

                                # Находим значения для домашнего и гостевого игрока
                                home_elem = row.find_element(By.CSS_SELECTOR,
                                                             "[class*='home'], [class*='player1'], .wcl-homePlayer_HRiEa")
                                away_elem = row.find_element(By.CSS_SELECTOR,
                                                             "[class*='away'], [class*='player2'], .wcl-awayPlayer_CZE9L")

                                home_value = home_elem.text.strip()
                                away_value = away_elem.text.strip()

                                # Обрабатываем значения так же, как в parse_serve_stats
                                # Проверяем, содержат ли значения проценты и дополнительные данные
                                if '(' in home_value and ')' in home_value:
                                    # Формат: 70% (14/20)
                                    home_percent = re.search(r'(\d+)%', home_value)
                                    home_details = re.search(r'\((.*?)\)', home_value)

                                    if home_percent and home_details:
                                        home_data = {
                                            'value': f"{home_percent.group(1)}%",
                                            'details': home_details.group(1)
                                        }
                                    else:
                                        home_data = home_value
                                else:
                                    home_data = home_value

                                if '(' in away_value and ')' in away_value:
                                    # Формат: 70% (14/20)
                                    away_percent = re.search(r'(\d+)%', away_value)
                                    away_details = re.search(r'\((.*?)\)', away_value)

                                    if away_percent and away_details:
                                        away_data = {
                                            'value': f"{away_percent.group(1)}%",
                                            'details': away_details.group(1)
                                        }
                                    else:
                                        away_data = away_value
                                else:
                                    away_data = away_value

                                # Создаем полное имя для категории с указанием секции
                                full_category = f"{section_name} - {category}" if section_name != category else category

                                # Сохраняем данные
                                game_stats[full_category] = {
                                    'home': home_data,
                                    'away': away_data
                                }

                                self.logger.info(f"Найдена статистика: {full_category} - {home_data} | {away_data}")
                            except Exception as e:
                                self.logger.debug(f"Ошибка при обработке строки статистики: {str(e)}")
                                continue
                    except Exception as e:
                        self.logger.debug(f"Ошибка при обработке секции статистики: {str(e)}")
                        continue
            except Exception as e:
                self.logger.error(f"Ошибка при поиске секций статистики: {str(e)}")

            # Ищем статистику в альтернативном формате, если мы не нашли её ранее
            if not game_stats:
                try:
                    all_stat_rows = self.driver.find_elements(By.CSS_SELECTOR,
                                                              "[data-testid='wcl-statistics'], div[class*='statisticsRow'], div[class*='statRow']")

                    for row in all_stat_rows:
                        try:
                            row_text = row.text.lower()

                            # Игнорируем строки подачи, так как они уже обработаны
                            if any(keyword in row_text for keyword in ['эйс', 'подач', 'двойн', 'перв']):
                                continue

                            # Получаем категорию и значения
                            try:
                                category_text = re.search(r'^(.*?)(?:\d|\s\d)', row_text)
                                category = category_text.group(1).strip() if category_text else "Неизвестно"

                                # Извлекаем числовые значения
                                values = re.findall(r'(\d+%|\d+/\d+|\d+)', row_text)
                                if len(values) >= 2:
                                    home_value = values[0]
                                    away_value = values[-1]

                                    game_stats[category] = {
                                        'home': home_value,
                                        'away': away_value
                                    }

                                    self.logger.info(f"Найдена статистика: {category} - {home_value} | {away_value}")
                            except Exception as e:
                                self.logger.debug(f"Ошибка при извлечении данных из строки: {str(e)}")
                                continue
                        except Exception as e:
                            self.logger.debug(f"Ошибка при проверке строки статистики: {str(e)}")
                            continue
                except Exception as e:
                    self.logger.error(f"Ошибка при прямом поиске статистики: {str(e)}")

            total_time = time.time() - start_time
            self.logger.info(f"Парсинг статистики возврата и очков завершен за {total_time:.2f} секунд")

            return game_stats

        except Exception as e:
            self.logger.error(f"Критическая ошибка при парсинге статистики возврата и очков: {str(e)}")
            traceback_str = traceback.format_exc()
            self.logger.error(f"Стек вызовов: {traceback_str}")
            return {}

    def parse_games_stats(self):
        """Парсинг статистики по геймам"""
        start_time = time.time()
        self.logger.info("Начало парсинга статистики по геймам...")
        try:
            # Мы не вызываем здесь переход на вкладку статистики,
            # так как предполагается, что это уже было сделано в предыдущих методах

            self.logger.info("Поиск данных о геймах...")
            games_stats = {}

            # Ищем информацию о счете по сетам и геймам
            try:
                # Пытаемся найти элемент с информацией о матче (счет, сеты и т.д.)
                score_elements = self.driver.find_elements(By.CSS_SELECTOR,
                                                           "[class*='score'], [class*='Score'], [data-testid='wcl-score'], .wcl-score")

                if score_elements:
                    self.logger.info(f"Найдено {len(score_elements)} элементов с информацией о счете")

                    # Ищем информацию о текущем счете
                    try:
                        current_score = self.driver.find_element(By.CSS_SELECTOR,
                                                                 "[class*='current-score'], [class*='currentScore'], [data-testid='wcl-current-score']")
                        if current_score:
                            current_score_text = current_score.text.strip()
                            games_stats['Текущий счет'] = current_score_text
                            self.logger.info(f"Текущий счет: {current_score_text}")
                    except Exception as e:
                        self.logger.debug(f"Не удалось найти текущий счет: {str(e)}")

                    # Парсим информацию о счете по сетам
                    for score_element in score_elements:
                        try:
                            # Ищем информацию о сетах
                            set_info = score_element.find_elements(By.CSS_SELECTOR,
                                                                   "[class*='set'], [class*='Set'], [data-testid='wcl-set']")

                            if set_info:
                                for i, set_elem in enumerate(set_info, 1):
                                    try:
                                        set_text = set_elem.text.strip()
                                        # Извлекаем счет по геймам из текста сета
                                        scores = re.findall(r'\d+', set_text)
                                        if len(scores) >= 2:
                                            home_score = scores[0]
                                            away_score = scores[1]
                                            games_stats[f'Сет {i}'] = {
                                                'home': home_score,
                                                'away': away_score
                                            }
                                            self.logger.info(f"Сет {i}: {home_score} - {away_score}")
                                    except Exception as e:
                                        self.logger.debug(f"Ошибка при обработке информации о сете {i}: {str(e)}")
                        except Exception as e:
                            self.logger.debug(f"Ошибка при поиске информации о сетах: {str(e)}")
            except Exception as e:
                self.logger.error(f"Ошибка при поиске элементов счета: {str(e)}")

            # Ищем детальную информацию о геймах в альтернативном формате
            if not games_stats:
                try:
                    # Проверяем наличие вкладки с деталями матча
                    tab_selectors = [
                        "a[href*='#/match-summary']",
                        "button[data-testid='wcl-tab']:not([data-selected='true'])",
                        "button.wcl-tab_y-fEC:not(.wcl-tabSelected_T--kd)"
                    ]

                    # Пытаемся найти и кликнуть на вкладку "Обзор матча"
                    tab_found = False
                    for selector in tab_selectors:
                        try:
                            tabs = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            if tabs:
                                for tab in tabs:
                                    try:
                                        tab_text = tab.text.strip().lower()
                                        if 'обзор' in tab_text or 'матч' in tab_text or 'summary' in tab_text or 'match' in tab_text:
                                            # Кликаем на вкладку с обзором матча
                                            self.logger.info(f"Найдена вкладка обзора матча: {tab_text}")
                                            self.driver.execute_script("arguments[0].click();", tab)
                                            time.sleep(1)  # Ждем загрузки содержимого
                                            tab_found = True
                                            break
                                    except Exception as e:
                                        self.logger.debug(f"Ошибка при проверке текста вкладки: {str(e)}")
                                if tab_found:
                                    break
                        except Exception as e:
                            self.logger.debug(f"Ошибка при поиске вкладки по селектору {selector}: {str(e)}")

                    if tab_found:
                        self.logger.info("Успешно перешли на вкладку обзора матча")

                        # Ищем таблицу матча или подобную информацию
                        try:
                            match_period_elements = self.driver.find_elements(By.CSS_SELECTOR,
                                                                              "[class*='period'], [class*='Period'], [data-testid='wcl-period']")

                            if match_period_elements:
                                self.logger.info(f"Найдено {len(match_period_elements)} периодов матча")

                                for i, period in enumerate(match_period_elements, 1):
                                    try:
                                        period_text = period.text.strip()
                                        # Ищем счет типа "6-4" или "7-6"
                                        scores = re.search(r'(\d+)[^\d]+(\d+)', period_text)
                                        if scores:
                                            home_score = scores.group(1)
                                            away_score = scores.group(2)
                                            games_stats[f'Сет {i}'] = {
                                                'home': home_score,
                                                'away': away_score
                                            }
                                            self.logger.info(f"Сет {i}: {home_score} - {away_score}")
                                    except Exception as e:
                                        self.logger.debug(f"Ошибка при обработке периода {i}: {str(e)}")
                        except Exception as e:
                            self.logger.error(f"Ошибка при поиске периодов матча: {str(e)}")
                except Exception as e:
                    self.logger.error(f"Ошибка при попытке перехода на вкладку обзора матча: {str(e)}")

            total_time = time.time() - start_time
            self.logger.info(f"Парсинг статистики по геймам завершен за {total_time:.2f} секунд")

            return games_stats

        except Exception as e:
            self.logger.error(f"Критическая ошибка при парсинге статистики по геймам: {str(e)}")
            traceback_str = traceback.format_exc()
            self.logger.error(f"Стек вызовов: {traceback_str}")
            return {}

    def parse_odds(self):
        """Парсинг коэффициентов матча - улучшенный метод"""
        try:
            start_time = time.time()
            self.logger.info("Начинаю парсинг коэффициентов")

            # Улучшенная обработка cookie
            self.close_cookies_popup()

            # Попробуем найти и открыть вкладку с коэффициентами, если она существует
            try:
                self.logger.info("Пробуем найти и открыть вкладку с коэффициентами")

                # Сначала пробуем найти и кликнуть на вкладку "Коэффициенты" с помощью JavaScript
                odds_tab_clicked = self.driver.execute_script("""
                    // Список селекторов для вкладки с коэффициентами
                    const oddsTabs = [
                        'a[href*="#/odds"]',
                        'a[data-tab-id*="odds"]',
                        'a[data-tab="odds"]',
                        'a[class*="oddsTab"]',
                        'div[class*="tab"][class*="odds"]',
                        'a[href*="odds"]',
                        'li[data-tab="odds"]',
                        'button[data-tab="odds"]',
                        'a:contains("Коэффициенты")',
                        'a:contains("Odds")',
                        'div[class*="tab"]:contains("Коэффициенты")',
                        'div[class*="tab"]:contains("Odds")'
                    ];

                    for (const selector of oddsTabs) {
                        try {
                            const tabs = document.querySelectorAll(selector);
                            for (const tab of tabs) {
                                // Проверяем, что текст вкладки содержит ключевые слова, связанные с коэффициентами
                                const text = tab.textContent.toLowerCase();
                                if (text.includes('odds') || text.includes('коэфф') || text.includes('ставк')) {
                                    tab.click();
                                    return true;
                                }
                            }
                        } catch (e) {
                            // Продолжаем перебор, если селектор не сработал
                            continue;
                        }
                    }

                    // Если не нашли по селекторам, пробуем найти по тексту
                    const allLinks = document.querySelectorAll('a, div[role="tab"], li[role="tab"], button[role="tab"]');
                    for (const link of allLinks) {
                        const text = link.textContent.toLowerCase();
                        if (text.includes('odds') || text.includes('коэфф') || text.includes('ставк')) {
                            link.click();
                            return true;
                        }
                    }

                    return false;
                """)

                if odds_tab_clicked:
                    self.logger.info("Успешно открыта вкладка с коэффициентами")
                    # Ждем загрузки содержимого вкладки
                    time.sleep(2)
                else:
                    self.logger.info("Вкладка с коэффициентами не найдена, продолжаем парсинг текущей страницы")
            except Exception as e:
                self.logger.warning(f"Ошибка при попытке открыть вкладку с коэффициентами: {str(e)}")

            odds_data = {}

            # НОВЫЙ МЕТОД: Парсинг через специализированную функцию parse_odds_cell
            try:
                # Получаем HTML страницы
                html_content = self.driver.page_source
                # Парсим коэффициенты из oddsCell__odd с помощью нового метода
                odds_cell_data = self.parse_odds_cell(html_content)

                # Если нашли данные с помощью нового метода, используем их
                if odds_cell_data and (odds_cell_data.get('home_odds') or odds_cell_data.get('away_odds')):
                    self.logger.info(
                        f"Коэффициенты успешно получены с помощью метода parse_odds_cell: {odds_cell_data}")
                    return odds_cell_data
                else:
                    self.logger.info("Метод parse_odds_cell не нашел коэффициентов")
            except Exception as e:
                self.logger.debug(f"Ошибка при использовании метода parse_odds_cell: {str(e)}")

            # Если новый метод не сработал, используем старый подход
            # Ожидание загрузки коэффициентов
            try:
                self.logger.info("Ожидание загрузки элементов с коэффициентами с увеличенным таймаутом")
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR,
                                                    '.oddsValueInner, .oddsCell__odd, .oddsCell, div[class*="odds"], span[class*="odds"]'))
                )
                self.logger.info("Элементы с коэффициентами найдены после ожидания")
            except Exception as e:
                self.logger.warning(f"Timeout при ожидании загрузки коэффициентов даже с увеличенным таймаутом: {str(e)}")

            # Делаем скриншот для отладки
            try:
                screenshot_path = f"odds_parsing_debug_{int(time.time())}.png"
                self.driver.save_screenshot(screenshot_path)
                self.logger.info(f"Скриншот страницы сохранен для отладки: {screenshot_path}")
            except Exception as e:
                self.logger.warning(f"Ошибка при сохранении скриншота: {str(e)}")

            # Более точный JavaScript для поиска коэффициентов
            js_odds = self.driver.execute_script("""
                var odds = {};

                function isValidOdds(value) {
                    var num = parseFloat(value);
                    return !isNaN(num) && num > 1.0 && num < 50.0 && value.length <= 6;
                }

                // Поиск по специфичным селекторам
                var selectors = [
                    '.oddsValueInner',
                    '.oddsCell__odd',
                    '.oddsCell',
                    'div[class*="odds"][class*="value"]',
                    'span[class*="odds"][class*="value"]',
                    'div[class*="coefficient"]',
                    'span[class*="coefficient"]'
                ];

                var foundOdds = [];

                for (var i = 0; i < selectors.length && foundOdds.length < 2; i++) {
                    var elements = document.querySelectorAll(selectors[i]);
                    console.log('Проверка селектора ' + selectors[i] + ': найдено ' + elements.length + ' элементов');
                    for (var j = 0; j < elements.length; j++) {
                        var text = elements[j].textContent.trim().replace(/[^0-9.]/g, '');
                        if (isValidOdds(text)) {
                            foundOdds.push(text);
                            if (foundOdds.length >= 2) break;
                        }
                    }
                    if (foundOdds.length >= 2) break;
                }

                if (foundOdds.length >= 1) odds.home_odds = foundOdds[0];
                if (foundOdds.length >= 2) odds.away_odds = foundOdds[1];

                // Поиск направления изменения коэффициентов
                var directionSelectors = [
                    'div[class*="direction"]',
                    'span[class*="arrow"]',
                    'div[class*="trend"]',
                    'span[class*="trend"]',
                    '.arrowUp-ico',
                    '.arrowDown-ico'
                ];

                for (var i = 0; i < directionSelectors.length; i++) {
                    var dirElements = document.querySelectorAll(directionSelectors[i]);
                    var dir = null;

                    for (var j = 0; j < Math.min(dirElements.length, 2); j++) {
                        var dirClass = dirElements[j].getAttribute('class') || '';

                        if (dirClass.toLowerCase().includes('up')) {
                            dir = 'up';
                        } else if (dirClass.toLowerCase().includes('down')) {
                            dir = 'down';
                        }

                        if (dir && j === 0 && odds.home_odds) {
                            odds.home_odds_direction = dir;
                        } else if (dir && j === 1 && odds.away_odds) {
                            odds.away_odds_direction = dir;
                        }
                    }
                }

                // Извлечение предыдущих коэффициентов из title
                var oddsElements = document.querySelectorAll('.oddsCell__odd');
                for (var i = 0; i < Math.min(oddsElements.length, 2); i++) {
                    var title = oddsElements[i].getAttribute('title') || '';
                    if (title && '»' in title) {
                        var parts = title.split('»');
                        if (parts.length === 2) {
                            var oldOdds = parts[0].trim().replace(/[^0-9.]/g, '');
                            if (i === 0 && oldOdds && isValidOdds(oldOdds)) {
                                odds.home_odds_old = oldOdds;
                            } else if (i === 1 && oldOdds && isValidOdds(oldOdds)) {
                                odds.away_odds_old = oldOdds;
                            }
                        }
                    }
                }

                odds.selector_info = 'Использовано селекторов: ' + selectors.length;
                return odds;
            """)

            if js_odds and (js_odds.get('home_odds') or js_odds.get('away_odds')):
                odds_data = js_odds
                self.logger.info(f"Коэффициенты получены через JS: {odds_data}")
            else:
                self.logger.info("JavaScript не нашел коэффициентов, пробуем резервный метод")
                # Резервный метод с улучшенными селекторами
                try:
                    selectors = [
                        '.oddsValueInner',
                        '.oddsCell__odd',
                        '.oddsCell',
                        'div[class*="odds"][class*="value"]',
                        'span[class*="odds"][class*="value"]',
                        'div[class*="coefficient"]',
                        'span[class*="coefficient"]'
                    ]

                    for selector in selectors:
                        odds_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        self.logger.info(f"Проверка селектора {selector}: найдено {len(odds_elements)} элементов")
                        if len(odds_elements) >= 2:
                            home_text = re.sub(r'[^0-9.]', '', odds_elements[0].text)
                            away_text = re.sub(r'[^0-9.]', '', odds_elements[1].text)

                            if home_text and self._is_valid_odds(home_text):
                                odds_data["home_odds"] = home_text
                                self.logger.info(f"Найден коэффициент на домашнего игрока: {home_text}")

                            if away_text and self._is_valid_odds(away_text):
                                odds_data["away_odds"] = away_text
                                self.logger.info(f"Найден коэффициент на гостевого игрока: {away_text}")

                            if odds_data.get("home_odds") and odds_data.get("away_odds"):
                                break
                        else:
                            self.logger.info(f"Селектор {selector} не дал результата")

                except Exception as e:
                    self.logger.debug(f"Ошибка при поиске коэффициентов методом 1: {str(e)}")

            # Улучшенная валидация найденных коэффициентов
            if odds_data.get('home_odds') and odds_data.get('away_odds'):
                home_odds = float(odds_data['home_odds'])
                away_odds = float(odds_data['away_odds'])

                # Проверка на правдоподобность коэффициентов
                if abs(home_odds - away_odds) > 20:
                    self.logger.warning("Подозрительная разница между коэффициентами")
                    if not (1.01 <= home_odds <= 30 and 1.01 <= away_odds <= 30):
                        self.logger.error("Коэффициенты вне допустимого диапазона")
                        odds_data = {}

            total_time = time.time() - start_time
            odds_data["parse_time"] = total_time
            self.logger.info(f"Парсинг коэффициентов завершен за {total_time:.2f} секунд")

            return odds_data

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге коэффициентов: {str(e)}")
            return {}

    def _is_valid_odds(self, odds_str):
        """Проверка валидности коэффициента"""
        try:
            odds = float(odds_str)
            # Валидный коэффициент обычно больше 1.0 и меньше 50.0
            return 1.0 < odds < 50.0
        except (ValueError, TypeError):
            return False

    def close_cookies_popup(self):
        """Метод для закрытия различных видов cookie-баннеров и всплывающих окон, включая попап подтверждения возраста (JA/NEE)"""
        try:
            self.logger.info("Пытаюсь найти и закрыть cookie-баннеры и всплывающие окна")

            # --- Новый блок для попапа совершеннолетия (JA/NEE) ---
            try:
                # Ищем кнопку с текстом 'JA' (Да)
                ja_buttons = self.driver.find_elements(By.XPATH, "//button[normalize-space(text())='JA']")
                for btn in ja_buttons:
                    if btn.is_displayed():
                        btn.click()
                        self.logger.info("Кликнул по кнопке 'JA' для подтверждения возраста")
                        time.sleep(0.5)
                        break
            except Exception as e:
                self.logger.warning(f'Ошибка при попытке закрыть попап возраста: {e}')

            # Список общих селекторов для cookie-баннеров и всплывающих окон
            common_selectors = [
                # Cookie баннеры - кнопки закрытия/согласия
                "button[data-testid='cookie-policy-dialog-accept']",
                "button[data-testid='banner-accept']",
                "button.cookie-consent__agree",
                "button.consent-give",
                "button#accept-cookies",
                "button#onetrust-accept-btn-handler",
                "a#cookie_action_close_header",
                ".cookie-banner .close-button",
                ".cookie-banner .accept-button",
                ".cookie-consent .accept-cookies",
                ".cookie-popup .accept-all",
                "div[class*='cookie'] button[class*='accept']",
                "div[class*='cookie'] button[class*='close']",
                "div[class*='cookie'] a[class*='accept']",

                # Всплывающие окна - кнопки закрытия
                "button.popup-close",
                "button.modal-close",
                "button.close-modal",
                "a.popup-close",
                ".modal .close",
                ".modal .close-btn",
                ".popup .close-button",
                "[data-dismiss='modal']",
                "[aria-label='Close']",
                ".modal-header .close",

                # Иконки закрытия
                ".fa-times",
                ".fa-close",
                ".close-icon",
                ".modal-close-icon"
            ]

            # Поиск и клик по всем возможным элементам закрытия
            for selector in common_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        if element.is_displayed():
                            self.logger.info(f"Найден видимый элемент для закрытия: {selector}")
                            try:
                                element.click()
                                self.logger.info(f"Успешно закрыт элемент: {selector}")
                                time.sleep(0.3)  # Даем время на обработку клика
                            except Exception as click_error:
                                self.logger.debug(f"Не удалось кликнуть по элементу {selector}: {str(click_error)}")
                                try:
                                    # Пробуем через JavaScript
                                    self.driver.execute_script("arguments[0].click();", element)
                                    self.logger.info(f"Успешно закрыт элемент через JavaScript: {selector}")
                                    time.sleep(0.3)
                                except Exception as js_error:
                                    self.logger.debug(
                                        f"Не удалось закрыть элемент через JavaScript {selector}: {str(js_error)}")
                except Exception as selector_error:
                    self.logger.debug(f"Ошибка при поиске элемента {selector}: {str(selector_error)}")

            # Удаление cookie-баннеров через JavaScript (для случаев, когда нет доступной кнопки закрытия)
            self.driver.execute_script("""
                // Находим все элементы, которые могут быть cookie-баннерами
                var elements = document.querySelectorAll('div[class*="cookie"], div[class*="Cookie"], div[id*="cookie"], div[id*="Cookie"], div[class*="popup"], div[class*="modal"], div[aria-label*="cookie"], div[aria-label*="Cookie"]');

                elements.forEach(function(element) {
                    // Если элемент виден и имеет фиксированное позиционирование или абсолютное
                    var style = window.getComputedStyle(element);
                    if ((style.position === 'fixed' || style.position === 'absolute') && 
                        style.display !== 'none' && 
                        style.visibility !== 'hidden' &&
                        element.offsetWidth > 0 && 
                        element.offsetHeight > 0) {

                        // Скрываем элемент
                        element.style.display = 'none';
                        element.style.opacity = '0';
                        element.style.visibility = 'hidden';
                        element.style.pointerEvents = 'none';

                        console.log('Скрыт потенциальный cookie-баннер:', element);
                    }
                });

                // Восстановление прокрутки страницы, которая могла быть заблокирована баннерами
                document.body.style.overflow = 'auto';
                document.documentElement.style.overflow = 'auto';
            """)

            self.logger.info("Завершена обработка потенциальных cookie-баннеров и всплывающих окон")

        except Exception as e:
            self.logger.warning(f"Ошибка при попытке закрыть cookie-баннеры: {str(e)}")

    def _find_bootstrap_buttons(self, driver):
        """Ищет кнопки с Bootstrap-классами

        Args:
            driver: WebDriver для поиска элементов

        Returns:
            list: Список найденных элементов кнопок
        """
        bootstrap_selectors = [
            ".btn-primary", ".btn-secondary", ".btn-success", ".btn-info",
            ".btn-warning", ".btn-danger", ".btn-light", ".btn-dark",
            ".btn-outline-primary", ".btn-outline-secondary", ".btn-outline-success",
            ".btn-outline-info", ".btn-outline-warning", ".btn-outline-danger",
            ".btn-outline-light", ".btn-outline-dark",
            ".btn-lg", ".btn-sm", ".btn-block"
        ]

        result = []
        for selector in bootstrap_selectors:
            try:
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                if buttons:
                    result.extend(buttons)
            except Exception as e:
                self.logger.debug(f"Ошибка при поиске кнопок с Bootstrap селектором '{selector}': {str(e)}")

        return result

    def safe_click(self, element, max_attempts=3):
        """Безопасный клик по элементу с обработкой ошибок перехвата клика

        Args:
            element: Элемент, по которому нужно кликнуть
            max_attempts: Максимальное количество попыток клика

        Returns:
            bool: True если клик успешен, False в противном случае
        """
        for attempt in range(max_attempts):
            try:
                self.logger.debug(f"Попытка клика {attempt + 1}/{max_attempts}")

                # Прокручиваем к элементу для большей надежности клика
                try:
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                    time.sleep(0.2)  # Короткая пауза после прокрутки
                except Exception as e:
                    self.logger.debug(f"Ошибка при прокрутке к элементу: {str(e)}")

                # Проверяем, виден ли элемент и можно ли по нему кликнуть
                if not element.is_displayed():
                    self.logger.warning("Элемент не отображается на странице")
                    time.sleep(0.3)
                    continue

                # Проверяем, не перекрыт ли элемент другими элементами
                is_clickable = self.driver.execute_script("""
                    var elem = arguments[0];
                    var rect = elem.getBoundingClientRect();
                    var cx = rect.left + rect.width / 2;
                    var cy = rect.top + rect.height / 2;
                    var elemAtPoint = document.elementFromPoint(cx, cy);
                    return elem.contains(elemAtPoint) || elemAtPoint.contains(elem);
                """, element)

                if not is_clickable:
                    self.logger.warning("Элемент перекрыт другими элементами, пробую удалить перекрытие")
                    self.driver.execute_script("""
                        var elem = arguments[0];
                        var rect = elem.getBoundingClientRect();
                        var cx = rect.left + rect.width / 2;
                        var cy = rect.top + rect.height / 2;
                        var elemAtPoint = document.elementFromPoint(cx, cy);
                        if (elemAtPoint && elemAtPoint !== elem) {
                            try {
                                elemAtPoint.style.pointerEvents = 'none';
                                elemAtPoint.style.opacity = '0.5';
                            } catch(e) {}
                        }
                    """, element)
                    time.sleep(0.3)

                # Пробуем кликнуть
                element.click()
                self.logger.info("Клик выполнен успешно")
                return True

            except Exception as e:
                error_text = str(e).lower()
                self.logger.warning(f"Ошибка при клике (попытка {attempt + 1}/{max_attempts}): {str(e)}")

                # Проверяем различные типы ошибок
                if any(keyword in error_text for keyword in
                       ["intercepted", "другой элемент", "перехвачен", "element click", "would receive the click",
                        "intercept", "element in the way"]):
                    self.logger.info("Обнаружен перехват клика, пытаюсь обработать")

                    # 1. Закрытие cookie-баннеров
                    try:
                        self.close_cookies_popup()
                        time.sleep(0.5)
                    except Exception as cookie_error:
                        self.logger.warning(f"Ошибка при попытке закрыть cookie-баннер: {str(cookie_error)}")

                    # 2. Удаление всех возможных перекрытий через JavaScript
                    try:
                        self.driver.execute_script("""
                            // Находим элементы с position:fixed или position:absolute, которые могут перекрывать клик
                            var overlays = Array.from(document.querySelectorAll('*')).filter(el => {
                                var style = window.getComputedStyle(el);
                                return (style.position === 'fixed' || style.position === 'absolute') &&
                                       style.display !== 'none' &&
                                       style.visibility !== 'hidden' &&
                                       el.offsetWidth > 0 &&
                                       el.offsetHeight > 0;
                            });

                            // Временно убираем эти элементы из потока
                            overlays.forEach(function(overlay) {
                                overlay.setAttribute('data-original-z-index', overlay.style.zIndex);
                                overlay.setAttribute('data-original-display', overlay.style.display);
                                overlay.style.zIndex = '-1';
                                overlay.style.pointerEvents = 'none';
                            });

                            // Восстанавливаем прокрутку страницы
                            document.body.style.overflow = 'auto';
                            document.documentElement.style.overflow = 'auto';

                            return overlays.length;
                        """)
                        time.sleep(0.3)
                    except Exception as js_error:
                        self.logger.warning(f"Ошибка при удалении перекрытий: {str(js_error)}")

                # Если элемент стал устаревшим/неактуальным
                elif "stale" in error_text or "no such element" in error_text or "not attached" in error_text:
                    self.logger.warning("Элемент устарел или больше не привязан к DOM, прерываем попытки клика")
                    return False

                # Если медленное соединение или страница еще загружается
                elif "timeout" in error_text or "wait" in error_text:
                    self.logger.info("Таймаут, делаем дополнительную паузу")
                    time.sleep(1.0)  # Более длинная пауза при проблемах с ожиданием

                # Если последняя попытка, используем различные JavaScript-методы клика
                if attempt == max_attempts - 1:
                    self.logger.info("Последняя попытка, пробую разные подходы через JavaScript")

                    # 1. Стандартный JavaScript клик
                    try:
                        self.logger.info("Пробую прямой JavaScript клик")
                        self.driver.execute_script("arguments[0].click();", element)
                        self.logger.info("JavaScript клик успешен")
                        return True
                    except Exception as js_error:
                        self.logger.error(f"Ошибка при JavaScript клике: {str(js_error)}")

                    # 2. Создание и эмуляция события клика
                    try:
                        self.logger.info("Пробую эмуляцию события клика")
                        self.driver.execute_script("""
                            var elem = arguments[0];
                            var evt = new MouseEvent('click', {
                                bubbles: true,
                                cancelable: true,
                                view: window
                            });
                            elem.dispatchEvent(evt);
                        """, element)
                        self.logger.info("Эмуляция события клика успешна")
                        return True
                    except Exception as event_error:
                        self.logger.error(f"Ошибка при эмуляции события клика: {str(event_error)}")

                    # 3. Выполнение действия, связанного с элементом (например, перехода по ссылке)
                    try:
                        tag_name = element.tag_name.lower()
                        if tag_name == 'a' and element.get_attribute('href'):
                            href = element.get_attribute('href')
                            self.logger.info(f"Элемент является ссылкой, пробую перейти по ней: {href}")
                            self.driver.get(href)
                            return True
                        elif tag_name == 'button' or tag_name == 'input':
                            form = self.driver.execute_script("return arguments[0].form;", element)
                            if form:
                                self.logger.info("Элемент является частью формы, пробую отправить форму")
                                self.driver.execute_script("arguments[0].submit();", form)
                                return True
                    except Exception as alt_action_error:
                        self.logger.error(f"Ошибка при выполнении альтернативного действия: {str(alt_action_error)}")

                time.sleep(0.7)  # Увеличенная пауза между попытками для лучшей стабильности

        self.logger.error(f"Не удалось выполнить клик после {max_attempts} попыток")
        return False

    def format_stats_message(self, match_data, is_favorite=False):
        """Форматирует полное сообщение со статистикой матча"""
        home = match_data.get('home_player', '?')
        away = match_data.get('away_player', '?')
        odds = match_data.get('odds', {})
        serve_stats = match_data.get('serve_stats', {})
        score = match_data.get('score', {})

        msg = []
        if is_favorite:
            msg.append("<b>🎾 Найден фаворит!</b>\n")

        # Базовая информация
        msg.append(f"<b>Матч:</b> {home} vs {away}")

        # Счет матча
        if score:
            if 'sets' in score:
                msg.append(f"<b>Счет по сетам:</b> {score['sets']}")
            if 'current_set' in score:
                msg.append(f"<b>Текущий сет:</b> {score['current_set']}")
            if 'current_game' in score:
                msg.append(f"<b>Текущий гейм:</b> {score['current_game']}")

        # Коэффициенты
        if odds:
            msg.append("\n<b>Коэффициенты:</b>")
            if 'home_odds' in odds:
                direction = odds.get('home_odds_direction', '')
                direction_arrow = '↑' if direction == 'up' else '↓' if direction == 'down' else ''
                msg.append(f"• {home}: {odds['home_odds']}{direction_arrow}")
            if 'away_odds' in odds:
                direction = odds.get('away_odds_direction', '')
                direction_arrow = '↑' if direction == 'up' else '↓' if direction == 'down' else ''
                msg.append(f"• {away}: {odds['away_odds']}{direction_arrow}")
            if 'home_odds_original' in odds:
                msg.append(f"• Начальный {home}: {odds['home_odds_original']}")
            if 'away_odds_original' in odds:
                msg.append(f"• Начальный {away}: {odds['away_odds_original']}")

        # Статистика подачи
        if serve_stats:
            msg.append("\n<b>Статистика подачи:</b>")
            for key, values in serve_stats.items():
                if isinstance(values, dict):
                    home_val = values.get('home', '?')
                    away_val = values.get('away', '?')

                    # Форматируем значения, если они содержат дополнительную информацию
                    if isinstance(home_val, dict):
                        home_details = home_val.get('details', '')
                        home_val = f"{home_val.get('value', '?')} ({home_details})" if home_details else home_val.get(
                            'value', '?')
                    if isinstance(away_val, dict):
                        away_details = away_val.get('details', '')
                        away_val = f"{away_val.get('value', '?')} ({away_details})" if away_details else away_val.get(
                            'value', '?')

                    msg.append(f"• {key}:")
                    msg.append(f"  {home}: {home_val}")
                    msg.append(f"  {away}: {away_val}")

        # Добавляем ссылку на матч
        if 'url' in match_data:
            msg.append(f"\n<a href='{match_data['url']}'>Ссылка на матч</a>")

        return '\n'.join(msg)

    def format_favorite_message(self, match_data, fav):
        """Форматирует сообщение о найденном фаворите в компактном формате по новому шаблону"""
        home = match_data.get('home_player', '?')
        away = match_data.get('away_player', '?')
        url = match_data.get('url', '')

        # Определяем, кто фаворит
        favorite_side = fav.get('side', '')
        is_home_favorite = favorite_side == 'home'
        favorite_player = home if is_home_favorite else away

        # Добавляем метку о пониженных критериях
        note = ' (пониженные критерии)' if 'note' in fav else ''

        # Получаем статистику геймов для фаворита
        games_stats = self.extract_games_stats(match_data, favorite_side)

        # Формируем сообщение
        msg_lines = [
            f"<b>Матч:</b> {home} vs {away}",
            f"<b>Фаворит:</b>{note} {favorite_player}",
            f"<b>Коэффициент:</b> {fav.get('odds', '0.0')}",
            f"<b>% первой подачи:</b> {fav.get('first_serve', 0.0)}%",
            f"<b>% выигр. очков на 1-й подаче:</b> {fav.get('first_serve_points', 0.0)}%",
            f"<b>Геймы фаворита:</b>",
            f"{games_stats}",
            f"<a href='{url}'>Ссылка на матч</a>"
        ]

        return '\n'.join(msg_lines)

    def extract_games_stats(self, match_data, favorite_side):
        """Извлекает статистику выигранных геймов для указанной стороны"""
        games_stats = match_data.get('games_stats', {})

        # Если нет данных о геймах, возвращаем стандартное сообщение
        if not games_stats:
            return "нет данных"

        # Ищем информацию о выигранных геймах
        games_won = 0
        total_games = 0
        games_percent = "0"

        opponent_games_won = 0
        opponent_total_games = 0
        opponent_games_percent = "0"

        # Пытаемся найти статистику по геймам в разных форматах
        for key, value in games_stats.items():
            if isinstance(value, dict) and favorite_side in value:
                if 'Сет' in key:
                    try:
                        fav_score = int(value.get(favorite_side, 0))
                        opp_score = int(value.get('away' if favorite_side == 'home' else 'home', 0))
                        games_won += fav_score
                        total_games += fav_score + opp_score
                        opponent_games_won += opp_score
                        opponent_total_games += fav_score + opp_score
                    except (ValueError, TypeError):
                        pass

        # Рассчитываем проценты, если есть данные
        if total_games > 0:
            games_percent = f"{(games_won / total_games) * 100:.0f}"
            opponent_games_percent = f"{(opponent_games_won / opponent_total_games) * 100:.0f}"

        return f"Всего выигранных геймов: {games_won}/{total_games} ({games_percent}%) | соперник: {opponent_games_won}/{opponent_total_games} ({opponent_games_percent}%)"

    async def filter_and_send_favorites(self, live_matches, telegram_bot, chat_id):
        self.logger.info(f"Начинаю фильтрацию фаворитов из {len(live_matches)} матчей")
        
        # --- ФУНКЦИИ ДЛЯ РАБОТЫ С ДАННЫМИ ---
        def match_score(match):
            """Считает сумму: коэффициент + % первой подачи + % выигр. очков на 1-й подаче"""
            try:
                odds = float(match.get('odds', 0))
                first_serve = float(match.get('first_serve', 0))
                first_serve_points = float(match.get('first_serve_points', 0))
                return odds + first_serve + first_serve_points
            except Exception:
                return 0
        
        # --- ЛИМИТ КОЛИЧЕСТВА ФАВОРИТОВ ---
        MAX_FAVORITES = 3  # максимальное количество фаворитов для отправки
        
        # --- СБОР КАНДИДАТОВ ПО СТРОГОМУ ФИЛЬТРУ ---
        strict_filtered = []  # Матчи, прошедшие строгий фильтр
        candidates = []       # Кандидаты для добора (с коэф >= 2.3 и наличием обоих процентов)

        # Проверяем у всех матчей критерии строгого фильтра и собираем кандидатов для добора
        for match_idx, match in enumerate(live_matches, 1):
            try:
                # Инициализируем переменные для данного матча
                home_odds = None
                away_odds = None
                home_first_serve = None
                home_first_serve_won = None
                away_first_serve = None
                away_first_serve_won = None
                
                # Получаем имена игроков
                home = match.get('home_player', '?')
                away = match.get('away_player', '?')
                url = match.get('url', '')
                
                # Для отладки: вывод участников матча
                self.logger.info(f"Анализ матча {match_idx}: {home} vs {away}")
                
                # --- ПОЛУЧАЕМ КОЭФФИЦИЕНТЫ ---
                odds = match.get('odds', {})
                koef = match.get('коэффициенты', {})
                
                # Для отладки: вывод структуры коэффициентов
                self.logger.info(f"Структура коэффициентов для матча {home} vs {away}: {odds}")
                if koef:
                    self.logger.info(f"Найдена структура 'коэффициенты': {koef}")
                
                # Получаем коэффициент для home
                for key in ['home_odds', 'home', '1']:
                    if key in odds and odds[key]:
                        try:
                            home_odds = float(str(odds[key]).replace(',', '.'))
                            break
                        except:
                            continue
                
                # Если не нашли в основной структуре, ищем в 'коэффициенты'
                if home_odds is None and koef:
                    home_koef = koef.get('игрок_1', {}).get('значение')
                    if home_koef and home_koef != 'н/д':
                        try:
                            home_odds = float(str(home_koef).replace(',', '.'))
                            self.logger.info(f"Использован коэффициент игрока 1 из структуры 'коэффициенты': {home_odds}")
                        except:
                            pass
                
                # Получаем коэффициент для away
                for key in ['away_odds', 'away', '2']:
                    if key in odds and odds[key]:
                        try:
                            away_odds = float(str(odds[key]).replace(',', '.'))
                            break
                        except:
                            continue
                
                # Если не нашли в основной структуре, ищем в 'коэффициенты'
                if away_odds is None and koef:
                    away_koef = koef.get('игрок_2', {}).get('значение')
                    if away_koef and away_koef != 'н/д':
                        try:
                            away_odds = float(str(away_koef).replace(',', '.'))
                            self.logger.info(f"Использован коэффициент игрока 2 из структуры 'коэффициенты': {away_odds}")
                        except:
                            pass
                
                # --- ПОЛУЧАЕМ СТАТИСТИКУ ПОДАЧИ ---
                serve_stats = match.get('serve_stats', {})
                
                # Анализируем статистику подачи для home
                if serve_stats:
                    for key, value in serve_stats.items():
                        if not isinstance(value, dict):
                            continue
                        
                        # Получаем значение для home
                        side_value = value.get('home')
                        if not isinstance(side_value, (dict, str)):
                            continue
                            
                        # Преобразуем строку в словарь если нужно
                        if isinstance(side_value, str):
                            side_value = {'value': side_value}
                            
                        # Получаем числовое значение
                        val = side_value.get('value', side_value.get('percent', '0'))
                        
                        # Преобразуем в число
                        if isinstance(val, str):
                            val = val.replace('%', '').replace(',', '.')
                            try:
                                val = float(val)
                            except:
                                continue
                                
                        # Определяем тип статистики по ключу
                        key_lower = key.lower()
                        if 'перв' in key_lower or '1-я' in key_lower or 'first' in key_lower:
                            if home_first_serve is None or val > home_first_serve:
                                home_first_serve = val
                        elif 'выигр' in key_lower or 'won' in key_lower or 'win' in key_lower:
                            if home_first_serve_won is None or val > home_first_serve_won:
                                home_first_serve_won = val
                
                # Анализируем статистику подачи для away
                if serve_stats:
                    for key, value in serve_stats.items():
                        if not isinstance(value, dict):
                            continue
                            
                        # Получаем значение для away
                        side_value = value.get('away')
                        if not isinstance(side_value, (dict, str)):
                            continue
                            
                        # Преобразуем строку в словарь если нужно
                        if isinstance(side_value, str):
                            side_value = {'value': side_value}
                            
                        # Получаем числовое значение
                        val = side_value.get('value', side_value.get('percent', '0'))
                        
                        # Преобразуем в число
                        if isinstance(val, str):
                            val = val.replace('%', '').replace(',', '.')
                            try:
                                val = float(val)
                            except:
                                continue
                                
                        # Определяем тип статистики по ключу
                        key_lower = key.lower()
                        if 'перв' in key_lower or '1-я' in key_lower or 'first' in key_lower:
                            if away_first_serve is None or val > away_first_serve:
                                away_first_serve = val
                        elif 'выигр' in key_lower or 'won' in key_lower or 'win' in key_lower:
                            if away_first_serve_won is None or val > away_first_serve_won:
                                away_first_serve_won = val
                
                # --- ПРОВЕРЯЕМ КРИТЕРИИ ДЛЯ HOME ---
                if home_odds is not None and home_first_serve is not None and home_first_serve_won is not None:
                    self.logger.info(f"Проверка критериев для {home}: коэф={home_odds}, 1-я подача={home_first_serve}, выигрыши={home_first_serve_won}")
                    
                    # Проверяем критерии строгого фильтра
                    if home_odds > 2.2 and home_first_serve > 60 and home_first_serve_won > 60:
                        self.logger.info(f"ПРОШЕЛ строгий фильтр: {home}")
                        strict_filtered.append({
                            'side': 'home',
                            'player': home,
                            'opponent': away,
                            'odds': home_odds,
                            'first_serve': home_first_serve,
                            'first_serve_points': home_first_serve_won,
                            'url': url
                        })
                    else:
                        self.logger.info(f"НЕ ПРОШЕЛ строгий фильтр: {home}")
                        
                    # Проверяем критерии для кандидатов (добора)
                    if home_odds >= 2.3:
                        self.logger.info(f"Добавляем в кандидаты: {home}, сумма = {home_odds + home_first_serve + home_first_serve_won}")
                        candidates.append({
                            'side': 'home',
                            'player': home,
                            'opponent': away,
                            'odds': home_odds,
                            'first_serve': home_first_serve,
                            'first_serve_points': home_first_serve_won,
                            'url': url
                        })
                    else:
                        self.logger.info(f"НЕ добавляем в кандидаты: {home}, коэф={home_odds} < 2.3")
                
                # --- ПРОВЕРЯЕМ КРИТЕРИИ ДЛЯ AWAY ---
                if away_odds is not None and away_first_serve is not None and away_first_serve_won is not None:
                    self.logger.info(f"Проверка критериев для {away}: коэф={away_odds}, 1-я подача={away_first_serve}, выигрыши={away_first_serve_won}")
                    
                    # Проверяем критерии строгого фильтра
                    if away_odds > 2.2 and away_first_serve > 60 and away_first_serve_won > 60:
                        self.logger.info(f"ПРОШЕЛ строгий фильтр: {away}")
                        strict_filtered.append({
                            'side': 'away',
                            'player': away,
                            'opponent': home,
                            'odds': away_odds,
                            'first_serve': away_first_serve,
                            'first_serve_points': away_first_serve_won,
                            'url': url
                        })
                    else:
                        self.logger.info(f"НЕ ПРОШЕЛ строгий фильтр: {away}")
                        
                    # Проверяем критерии для кандидатов (добора)
                    if away_odds >= 2.3:
                        self.logger.info(f"Добавляем в кандидаты: {away}, сумма = {away_odds + away_first_serve + away_first_serve_won}")
                        candidates.append({
                            'side': 'away',
                            'player': away,
                            'opponent': home,
                            'odds': away_odds,
                            'first_serve': away_first_serve,
                            'first_serve_points': away_first_serve_won,
                            'url': url
                        })
                    else:
                        self.logger.info(f"НЕ добавляем в кандидаты: {away}, коэф={away_odds} < 2.3")

            except Exception as e:
                self.logger.error(f"Ошибка при обработке матча {match_idx}: {str(e)}")
        
        self.logger.info(f"По строгому фильтру найдено {len(strict_filtered)} матчей")
        self.logger.info(f"Найдено {len(candidates)} кандидатов для добора")
        
        # Удаляем из кандидатов те матчи, которые уже прошли строгий фильтр
        def is_same_match(a, b):
            """Проверяет, относятся ли два элемента к одному и тому же матчу"""
            return a['player'] == b['player'] and a['opponent'] == b['opponent'] and a['side'] == b['side']
            
        filtered_candidates = []
        for cand in candidates:
            is_already_filtered = False
            for sf in strict_filtered:
                if is_same_match(cand, sf):
                    is_already_filtered = True
                    break
            if not is_already_filtered:
                filtered_candidates.append(cand)
        
        candidates = filtered_candidates
        self.logger.info(f"После удаления дубликатов осталось {len(candidates)} кандидатов для добора")
        
        # Сортируем кандидатов по сумме: коэф + % первой подачи + % выигр. очков на 1-й подаче
        candidates.sort(key=match_score, reverse=True)
        
        # --- ЛОГИКА ОТБОРА ТОП-3 ФАВОРИТОВ ---
        filtered = []
        
        # Применяем правила отбора:
        if len(strict_filtered) >= MAX_FAVORITES:
            # Если по строгому фильтру нашлось 3 и более матча - берем топ-3 из них
            strict_filtered.sort(key=lambda x: x['odds'], reverse=True)
            filtered = strict_filtered[:MAX_FAVORITES]
            self.logger.info(f"По строгому фильтру найдено {len(strict_filtered)}+ матчей, берем топ-{MAX_FAVORITES} из них")
        elif len(strict_filtered) == 2:
            # Если по строгому фильтру нашлось 2 матча - добираем 1 из кандидатов
            filtered = strict_filtered + candidates[:1]
            self.logger.info("По строгому фильтру найдено 2 матча, добираем 1 из кандидатов")
        elif len(strict_filtered) == 1:
            # Если по строгому фильтру нашелся 1 матч - добираем 2 из кандидатов
            filtered = strict_filtered + candidates[:2]
            self.logger.info("По строгому фильтру найден 1 матч, добираем 2 из кандидатов")
        else:
            # Если по строгому фильтру не нашлось ни одного матча - берем топ-3 из кандидатов
            filtered = candidates[:MAX_FAVORITES]
            self.logger.info(f"По строгому фильтру не найдено матчей, берем топ-{MAX_FAVORITES} из кандидатов")

        # Лимитируем до MAX_FAVORITES
        filtered = filtered[:MAX_FAVORITES]
        self.logger.info(f"Найдено {len(filtered)} фаворитов из {len(strict_filtered)} строгих и {len(candidates)} кандидатов, отправляем топ-{len(filtered)}")

        # Отправляем сообщения о фаворитах
        sent_favs = 0  # счетчик отправленных фаворитов
        if filtered:
            for fav in filtered:
                try:
                    # Находим полные данные матча
                    match_data = None
                    for m in live_matches:
                        if ((m.get('home_player') == fav['player'] and m.get('away_player') == fav['opponent']) or
                            (m.get('home_player') == fav['opponent'] and m.get('away_player') == fav['player'])):
                            match_data = m
                            break

                    if match_data:
                        # Форматируем сообщение в новом формате
                        msg = self.format_favorite_message(match_data, fav)
                    else:
                        # Если не нашли полные данные, используем базовую информацию
                        msg = (
                            f"<b>Матч:</b> {fav['player']} vs {fav['opponent']}\n"
                            f"<b>Фаворит:</b> {fav['player']}\n"
                            f"<b>Коэффициент:</b> {fav['odds']}\n"
                            f"<b>% первой подачи:</b> {fav['first_serve']}%\n"
                            f"<b>% выигр. очков на 1-й подаче:</b> {fav['first_serve_points']}%\n"
                            f"<b>Геймы фаворита:</b>\n"
                            f"нет данных\n"
                            f"Всего выигранных геймов: 0/0 (0%) | соперник: 0/0 (0%)\n"
                            f"<a href='{fav['url']}'>Ссылка на матч</a>"
                        )

                    await telegram_bot.send_message(
                        chat_id=chat_id,
                        text=msg,
                        parse_mode='HTML',
                        disable_web_page_preview=True
                    )
                    sent_favs += 1  # увеличиваем счетчик
                    self.logger.info(f"Успешно отправлен фаворит {sent_favs}/{len(filtered)}: {fav['player']}")
                except Exception as e:
                    self.logger.error(f"Ошибка при отправке сообщения о фаворите {fav['player']}: {str(e)}")
                    
            # Проверяем отправились ли все фавориты
            if sent_favs < len(filtered):
                self.logger.warning(f"Отправлено только {sent_favs} из {len(filtered)} фаворитов")
                
            # Формируем совет для GPT на основе отобранных фаворитов
            if len(filtered) > 0:
                best_match = max(filtered, key=lambda x: x.get('odds', 0))
                suggestion = (
                    f"<b>СОВЕТ ОТ BRO:</b> Лучшая ставка - {best_match['player']}.\n"
                    f"Коэффициент: {best_match['odds']}, "
                    f"% первой подачи: {best_match['first_serve']}%, "
                    f"% выигр. очков на 1-й подаче: {best_match['first_serve_points']}%"
                )
                try:
                    await telegram_bot.send_message(
                        chat_id=chat_id,
                        text=suggestion,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    self.logger.error(f"Ошибка при отправке совета: {str(e)}")
        else:
            await telegram_bot.send_message(
                chat_id=chat_id,
                text="❌ <b>Подходящих фаворитов не найдено</b>\n\nНи один из матчей не соответствует критериям для фаворитов.",
                parse_mode='HTML'
            )

        return filtered

    def safe_float(self, value):
        """Безопасное преобразование значения в float"""
        if not value:
            return 0.0
        try:
            # Удаляем все, кроме цифр и точки
            if isinstance(value, str):
                value = ''.join(c for c in value if c.isdigit() or c == '.')
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def extract_stat_value(self, stats, key_part, side, key_part2=None):
        """Извлекает статистическое значение из данных статистики"""
        try:
            # Ищем по ключу, содержащему key_part
            for key, val in stats.items():
                key_lower = key.lower()
                if key_part in key_lower and (key_part2 is None or key_part2 in key_lower):
                    if isinstance(val, dict):
                        side_val = val.get(side)
                        if isinstance(side_val, dict):
                            value = side_val.get('value') or side_val.get('percent')
                            if not value and side_val:
                                # Если нет явного значения, берем первое числовое значение
                                for k, v in side_val.items():
                                    if isinstance(v, (int, float, str)) and v:
                                        value = v
                                        break
                        else:
                            value = side_val

                    # Преобразуем в число
                    if value:
                        if isinstance(value, str):
                            value = value.replace('%', '').replace(',', '.')
                        return self.safe_float(value)

            return 0.0
        except Exception as e:
            self.logger.error(f"Ошибка при извлечении статистики {key_part} для {side}: {str(e)}")
            return 0.0

    def parse_match_details(self, match_url):
        """Расширенный парсинг деталей матча с улучшенным анализом данных и скоростью работы"""
        try:
            start_time = time.time()
            self.logger.info(f"Начало парсинга подробностей матча {match_url}")

            match_data = {
                "url": match_url,
            }

            # Открытие страницы матча
            try:
                if self.live_url not in match_url and self.base_url not in match_url:
                    # Добавляем базовый URL если его нет
                    match_url = f"{self.base_url}{match_url}"

                self.driver.get(match_url)
                self.logger.info(f"Загружена страница матча: {match_url}")
            except Exception as e:
                self.logger.error(f"Ошибка при загрузке страницы матча {match_url}: {str(e)}")
                # Продолжаем выполнение, чтобы попытаться собрать хоть какие-то данные

            # Закрытие возможных попапов и баннеров
            self.close_cookies_popup()

            # Парсинг игроков
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".duelParticipant__home, .participant-home, [class*='home']"))
                )

                # Универсальные селекторы для разных типов страниц
                home_selectors = [
                    "div.duelParticipant__home",
                    ".participant__participantName--home",
                    ".participant-home",
                    "div[class*='home']",
                    "[data-testid='wcl-participant-home']",
                    "span[class*='homeTea']"
                ]
                away_selectors = [
                    "div.duelParticipant__away",
                    ".participant__participantName--away",
                    ".participant-away",
                    "div[class*='away']",
                    "[data-testid='wcl-participant-away']",
                    "span[class*='awayTea']"
                ]

                for selector in home_selectors:
                    try:
                        home_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        match_data["home_player"] = home_element.text.strip()
                        if match_data["home_player"]:
                            self.logger.info(f"Найден домашний игрок: {match_data['home_player']}")
                            break
                    except:
                        continue

                for selector in away_selectors:
                    try:
                        away_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        match_data["away_player"] = away_element.text.strip()
                        if match_data["away_player"]:
                            self.logger.info(f"Найден гостевой игрок: {match_data['away_player']}")
                            break
                    except:
                        continue

                if not match_data.get("home_player") or not match_data.get("away_player"):
                    self.logger.warning("Не удалось найти игроков по стандартным селекторам, пробуем JavaScript")

                    # Расширенный JavaScript для поиска игроков на любых типах страниц
                    js_players = self.driver.execute_script("""
                        function findPlayers() {
                            var players = {};

                            // Пробуем основные селекторы
                            var homeSelectors = [
                                'div.duelParticipant__home', 
                                '.participant__participantName--home',
                                '.participant-home', 
                                'div[class*="home"]',
                                '[data-testid="wcl-participant-home"]',
                                'span[class*="homeTea"]',
                                '.teamNames__home', 
                                '.home-team-name'
                            ];

                            var awaySelectors = [
                                'div.duelParticipant__away', 
                                '.participant__participantName--away',
                                '.participant-away', 
                                'div[class*="away"]',
                                '[data-testid="wcl-participant-away"]',
                                'span[class*="awayTea"]',
                                '.teamNames__away', 
                                '.away-team-name'
                            ];

                            // Ищем по селекторам
                            for (var i = 0; i < homeSelectors.length; i++) {
                                var homeElement = document.querySelector(homeSelectors[i]);
                                if (homeElement && homeElement.textContent.trim()) {
                                    players.home = homeElement.textContent.trim();
                                    break;
                                }
                            }

                            for (var i = 0; i < awaySelectors.length; i++) {
                                var awayElement = document.querySelector(awaySelectors[i]);
                                if (awayElement && awayElement.textContent.trim()) {
                                    players.away = awayElement.textContent.trim();
                                    break;
                                }
                            }

                            // Если все еще не нашли, пробуем найти любые элементы, которые могут содержать имена
                            if (!players.home || !players.away) {
                                var possibleElements = document.querySelectorAll('.participant, .participant__name, .participantName, [class*="participant"], [class*="team"]');

                                if (possibleElements.length >= 2) {
                                    if (!players.home) players.home = possibleElements[0].textContent.trim();
                                    if (!players.away) players.away = possibleElements[1].textContent.trim();
                                }
                            }

                            return players;
                        }

                        return findPlayers();
                    """)

                    if js_players:
                        if js_players.get('home') and not match_data.get("home_player"):
                            match_data["home_player"] = js_players.get('home')
                            self.logger.info(f"Найден домашний игрок через JS: {match_data['home_player']}")

                        if js_players.get('away') and not match_data.get("away_player"):
                            match_data["away_player"] = js_players.get('away')
                            self.logger.info(f"Найден гостевой игрок через JS: {match_data['away_player']}")

            except Exception as e:
                self.logger.error(f"Ошибка при парсинге игроков: {str(e)}")
                # Пытаемся использовать данные из URL как запасной вариант
                match_data["home_player"] = match_data.get("home_player", "Неизвестно")
                match_data["away_player"] = match_data.get("away_player", "Неизвестно")

            # Парсинг коэффициентов
            try:
                odds_data = self.parse_odds()

                # Преобразуем коэффициенты в формат для JSON
                if odds_data:
                    match_data["коэффициенты"] = {
                        "игрок_1": {
                            "значение": odds_data.get("home_odds", "н/д"),
                            "тренд": odds_data.get("home_odds_direction", "н/д")
                        },
                        "игрок_2": {
                            "значение": odds_data.get("away_odds", "н/д"),
                            "тренд": odds_data.get("away_odds_direction", "н/д")
                        },
                        "время_парсинга": odds_data.get("parse_time", 0)
                    }

                    # Добавляем историю коэффициентов, если доступна
                    if odds_data.get("home_odds_old"):
                        match_data["коэффициенты"]["игрок_1"]["предыдущее"] = odds_data.get("home_odds_old")

                    if odds_data.get("away_odds_old"):
                        match_data["коэффициенты"]["игрок_2"]["предыдущее"] = odds_data.get("away_odds_old")

                    self.logger.info(f"Коэффициенты собраны: {match_data['коэффициенты']}")
                else:
                    match_data["коэффициенты"] = {
                        "игрок_1": {"значение": "н/д", "тренд": "н/д"},
                        "игрок_2": {"значение": "н/д", "тренд": "н/д"},
                        "время_парсинга": 0
                    }
                    self.logger.warning("Коэффициенты не найдены, установлены значения по умолчанию")
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге коэффициентов: {str(e)}")
                match_data["коэффициенты"] = {
                    "игрок_1": {"значение": "н/д", "тренд": "н/д"},
                    "игрок_2": {"значение": "н/д", "тренд": "н/д"},
                    "время_парсинга": 0
                }

            # Парсинг статистики сервиса
            try:
                serve_stats = self.parse_serve_stats()
                match_data["serve_stats"] = serve_stats
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге статистики подачи: {str(e)}")
                match_data["serve_stats"] = {}

            # Парсинг статистики гейма
            try:
                game_stats = self.parse_game_stats()
                match_data["game_stats"] = game_stats
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге статистики гейма: {str(e)}")
                match_data["game_stats"] = {}

            # Парсинг статистики геймов
            try:
                games_stats = self.parse_games_stats()
                match_data["games_stats"] = games_stats
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге статистики геймов: {str(e)}")
                match_data["games_stats"] = {}

            # Формируем сообщение со статистикой
            try:
                match_data["statistics_message"] = self.format_stats_message(match_data)
            except Exception as e:
                self.logger.error(f"Ошибка при форматировании сообщения статистики: {str(e)}")
                match_data["statistics_message"] = "Статистика не найдена"

            total_time = time.time() - start_time
            match_data["parse_time"] = total_time
            self.logger.info(f"Парсинг подробностей матча завершен за {total_time:.2f} секунд")

            return match_data

        except Exception as e:
            self.logger.error(f"Критическая ошибка при парсинге подробностей матча {match_url}: {str(e)}")
            tb = traceback.format_exc()
            self.logger.error(f"Стек вызовов: {tb}")
            return {
                "url": match_url,
                "home_player": "Ошибка",
                "away_player": "Ошибка",
                "коэффициенты": {
                    "игрок_1": {"значение": "н/д", "тренд": "н/д"},
                    "игрок_2": {"значение": "н/д", "тренд": "н/д"},
                    "время_парсинга": 0
                },
                "serve_stats": {},
                "game_stats": {},
                "games_stats": {},
                "parse_time": 0,
                "error": str(e),
                "statistics_message": "Ошибка при получении статистики"
            }

    def get_live_matches(self, use_cache=True, max_workers=4):
        """Получение информации о всех live матчах с использованием многопоточной обработки

        Args:
            use_cache (bool): Использовать кэширование уже обработанных матчей
            max_workers (int): Максимальное количество параллельных потоков

        Returns:
            list: Список информации о матчах
        """
        try:
            # Инициализируем кэш, если его не было
            if not hasattr(self, '_matches_cache'):
                self._matches_cache = {}
                self._cache_expiry = {}

            # Очистка устаревших записей кэша (старше 5 минут)
            current_time = time.time()
            expired_urls = [url for url, timestamp in self._cache_expiry.items()
                            if current_time - timestamp > 300]  # 5 минут

            for url in expired_urls:
                if url in self._matches_cache:
                    del self._matches_cache[url]
                if url in self._cache_expiry:
                    del self._cache_expiry[url]

            # Получаем ссылки на матчи
            match_links_objects = self.get_match_links()
            if not match_links_objects:
                self.logger.warning("Не найдено live-матчей")
                return []

            # Список всех уникальных ссылок на матчи для обработки
            all_links = []
            for match in match_links_objects:
                for url in match.get('urls', []):
                    if url not in all_links:
                        all_links.append(url)

            total_matches = len(all_links)
            self.logger.info(f"Обрабатываем {total_matches} Live матчей")

            # Разделяем ссылки на те, что в кэше и те, что нужно обработать
            cached_links = [url for url in all_links if url in self._matches_cache and use_cache]
            links_to_process = [url for url in all_links if url not in cached_links]

            if cached_links:
                self.logger.info(f"Найдено {len(cached_links)} матчей в кэше")

            # Функция для обработки одного матча
            def process_match(match_url, idx):
                try:
                    self.logger.info(f"Обработка матча {idx}/{total_matches}: {match_url}")
                    match_info = self.parse_match_details(match_url)
                    if match_info:
                        # Кэшируем результат
                        if use_cache:
                            self._matches_cache[match_url] = match_info
                            self._cache_expiry[match_url] = time.time()
                        self.logger.info(
                            f"Матч {idx}/{total_matches} успешно обработан за {match_info.get('parse_time', 0):.2f} сек")
                        return match_info
                    else:
                        self.logger.warning(f"Не удалось получить информацию о матче {idx}/{total_matches}")
                        return None
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке матча {idx}/{total_matches}: {str(e)}")
                    return None

            matches_info = []

            # Получаем результаты из кэша
            for url in cached_links:
                matches_info.append(self._matches_cache[url])

            if not links_to_process:
                self.logger.info("Все матчи найдены в кэше")
                return matches_info

            # Обрабатываем оставшиеся матчи параллельно
            with ThreadPoolExecutor(max_workers=min(max_workers, len(links_to_process))) as executor:
                # Создаем задачи для выполнения
                futures = {executor.submit(process_match, url, i + 1): url
                           for i, url in enumerate(links_to_process)}

                # Получаем результаты по мере выполнения
                from concurrent.futures import as_completed
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        matches_info.append(result)

            self.logger.info(f"Успешно получена информация о {len(matches_info)} матчах из {total_matches}")
            return matches_info
        except Exception as e:
            self.logger.error(f"Ошибка при получении информации о матчах: {str(e)}")
            return []

    def save_to_json(self, events, filename='tennis_events.json'):
        """Сохранение событий в JSON файл"""
        try:
            # Проверяем наличие статистики геймов в каждом матче
            for event in events:
                if 'game_stats' not in event:
                    self.logger.warning(
                        f"Отсутствует статистика геймов для матча {event.get('home_player', 'Unknown')} vs {event.get('away_player', 'Unknown')}")
                else:
                    self.logger.info(
                        f"Статистика геймов для матча {event.get('home_player', 'Unknown')} vs {event.get('away_player', 'Unknown')}: {event['game_stats']}")

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(events, f, ensure_ascii=False, indent=4)
            self.logger.info(f"Данные успешно сохранены в файл {filename}")

            # Проверяем сохраненные данные
            with open(filename, 'r', encoding='utf-8') as f:
                saved_data = json.load(f)
                for event in saved_data:
                    if 'game_stats' in event:
                        self.logger.info(
                            f"Проверка сохраненных данных: {event['home_player']} vs {event['away_player']}")
                        self.logger.info(f"Статистика геймов: {event['game_stats']}")
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении в файл: {str(e)}")

    def close(self):
        """Закрытие драйвера"""
        try:
            self.driver.quit()
            self.logger.info("WebDriver успешно закрыт")
        except Exception as e:
            self.logger.error(f"Ошибка при закрытии WebDriver: {str(e)}")

    def parse_specific_match(self, match_url):
        """Парсинг конкретного матча по URL"""
        try:
            # Проверяем, что URL содержит правильный формат
            if not match_url.startswith("https://www.flashscorekz.com/match/tennis/"):
                self.logger.error(f"Некорректный URL матча: {match_url}")
                return None

            # Убираем любые параметры после URL если они есть
            clean_url = match_url.split("#")[0]

            # Используем наш основной метод parse_match_details, который включает и коэффициенты
            match_data = self.parse_match_details(clean_url)

            if match_data:
                self.logger.info(f"Успешно получены данные для матча: {match_url}")
                return match_data
            else:
                self.logger.error(f"Не удалось получить данные для матча: {match_url}")
                return None

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге матча: {str(e)}")
            return None

    def save_match_details(self, match_stats, match_url, filename=None):
        """Сохранение детальной информации о матче в JSON файл"""
        try:
            if filename is None:
                # Создаем имя файла на основе текущей даты и времени
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"match_stats_{timestamp}.json"

            # Добавляем URL матча в статистику
            match_stats['match_url'] = match_url
            match_stats['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Сохраняем в JSON файл
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(match_stats, f, ensure_ascii=False, indent=4)

            self.logger.info(f"Статистика матча сохранена в файл: {filename}")
            return filename

        except Exception as e:
            self.logger.error(f"Ошибка при сохранении статистики в файл: {str(e)}")
            return None

    def get_last_surface_match_stats(self, live_matches, output_json=None):
        """Для каждого live-матча ищет последний матч с нужным покрытием в H2H, кликает по покрытию, парсит статистику и сохраняет всё в отдельный JSON."""
        results = []
        for match in live_matches:
            try:
                match_url = match.get('url')
                if not match_url:
                    continue
                # Получаем ссылку на H2H
                if "#/h2h" not in match_url:
                    h2h_url = match_url.split("#")[0] + "#/h2h/overall"
                else:
                    h2h_url = match_url
                # Новый временный драйвер для H2H
                chrome_options = ChromeOptions()
                chrome_options.add_argument('--headless')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                service = ChromeService(executable_path='/usr/local/bin/chromedriver')
                driver = webdriver.Chrome(service=service, options=chrome_options)
                try:
                    driver.get(h2h_url)
                    self.close_cookies_popup(driver)
                    WebDriverWait(driver, 4).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".h2h__section"))
                    )
                    time.sleep(1.5)
                    found = False
                    rows = driver.find_elements(By.CSS_SELECTOR, ".h2h__row")
                    for row in rows:
                        try:
                            event_span = row.find_element(By.CSS_SELECTOR, ".h2h__event")
                            classes = event_span.get_attribute("class")
                            if any(surf in classes for surf in ["hard surface", "clay surface", "grass surface"]):
                                # Кликаем по элементу покрытия
                                driver.execute_script("arguments[0].scrollIntoView(true);", event_span)
                                event_span.click()
                                # Ждём загрузки статистики
                                WebDriverWait(driver, 4).until(
                                    EC.presence_of_element_located(
                                        (By.CSS_SELECTOR, "div[data-analytics-context='tab-match-statistics']"))
                                )
                                self.close_cookies_popup(driver)
                                stats = self.parse_match_details(driver.current_url)
                                results.append({
                                    "live_match_url": match_url,
                                    "surface_match_url": driver.current_url,
                                    "surface_type": classes,
                                    "surface_match_stats": stats
                                })
                                found = True
                                break
                        except Exception as e:
                            continue
                    if not found:
                        results.append({
                            "live_match_url": match_url,
                            "surface_match_url": None,
                            "surface_type": None,
                            "surface_match_stats": None
                        })
                except Exception as e:
                    self.logger.error(f"Ошибка при поиске surface-матча для {match_url}: {str(e)}")
                finally:
                    try:
                        driver.quit()
                    except:
                        pass
            except Exception as e:
                self.logger.error(f"Ошибка в get_last_surface_match_stats: {str(e)}")
        # Сохраняем в отдельный JSON
        if not output_json:
            output_json = f"last_surface_matches_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        self.logger.info(f"Статистика surface-матчей сохранена в файл: {output_json}")
        return output_json

    def format_games_stats(self, games_stats, favorite_side):
        """Форматирует статистику геймов для красивого отображения"""
        if not games_stats:
            return 'нет данных'
        lines = []
        for k, v in games_stats.items():
            if isinstance(v, dict):
                fav = v.get(favorite_side, {})
                opp = v.get('away' if favorite_side == 'home' else 'home', {}) if favorite_side == 'home' else v.get(
                    'home', {})
                fav_percent = fav.get('percent', fav if isinstance(fav, str) else '')
                fav_numbers = fav.get('numbers', '')
                opp_percent = opp.get('percent', opp if isinstance(opp, str) else '')
                opp_numbers = opp.get('numbers', '')
                line = f"{k}: {fav_numbers} ({fav_percent}) | соперник: {opp_numbers} ({opp_percent})"
                lines.append(line)
            else:
                lines.append(f"{k}: {v}")
        return '\n'.join(lines)

    def format_match_info(self, match):
        """Форматирует информацию о матче для отображения"""
        odds = match.get('odds', {})
        serve_stats = match.get('serve_stats', {})
        game_stats = match.get('game_stats', {})
        url = match.get('url', '')
        home = match.get('home_player', '?')
        away = match.get('away_player', '?')

        # Форматируем время парсинга
        parse_time = odds.get('parse_time', 0)
        parse_time_str = f"{parse_time:.2f}" if parse_time else "н/д"

        # Форматируем коэффициенты
        home_odds = odds.get('home_odds', '?')
        away_odds = odds.get('away_odds', '?')

        # Базовая информация о матче
        match_info = [
            f"\n<b>{safe_html(home)} — {safe_html(away)}</b>",
            f"Коэффициенты: {home_odds} — {away_odds} (время парсинга: {parse_time_str}с)"
        ]

        # Добавляем статистику подачи, если она есть
        if serve_stats:
            match_info.append("\n<b>Статистика подачи:</b>")
            for key, values in serve_stats.items():
                if isinstance(values, dict):
                    home_val = values.get('home', '?')
                    away_val = values.get('away', '?')
                    if isinstance(home_val, dict):
                        home_val = home_val.get('value', '?')
                    if isinstance(away_val, dict):
                        away_val = away_val.get('value', '?')
                    match_info.append(f"{key}: {home_val} | {away_val}")

        # Добавляем статистику игры, если она есть
        if game_stats:
            match_info.append("\n<b>Статистика игры:</b>")
            for key, values in game_stats.items():
                if isinstance(values, dict):
                    home_val = values.get('home', '?')
                    away_val = values.get('away', '?')
                    if isinstance(home_val, dict):
                        home_val = home_val.get('value', '?')
                    if isinstance(away_val, dict):
                        away_val = away_val.get('value', '?')
                    match_info.append(f"{key}: {home_val} | {away_val}")

        # Добавляем ссылку на матч
        match_info.append(f"\n<a href='{html.escape(url)}'>Ссылка на матч</a>")

        return '\n'.join(match_info)

    async def send_summary_to_telegram(self, live_matches, telegram_bot, chat_id):
        def safe_html(text):
            # Экранирует спецсимволы, кроме разрешённых тегов <b>, <a>, </b>, </a>
            import re
            text = html.escape(text)
            text = re.sub(r'&lt;(\/?)b&gt;', r'<\1b>', text)
            text = re.sub(r'&lt;(\/?)a( [^&]*)&gt;', r'<\1a\2>', text)
            return text

        def split_message(text, max_length=3800):
            if len(text) <= max_length:
                return [text]

            parts = []
            current_part = ""
            lines = text.split('\n')
            header = '<b>Сводка по live-матчам:</b>'

            for line in lines:
                if not current_part:
                    current_part = header + '\n'

                if len(current_part + line + '\n') > max_length:
                    parts.append(current_part.rstrip())
                    current_part = header + "\n(продолжение)\n" + line + '\n'
                else:
                    current_part += line + '\n'

            if current_part:
                parts.append(current_part.rstrip())

            return parts

        # Формируем сводку
        summary = '<b>Сводка по live-матчам:</b>\n'

        if not live_matches:
            summary += "\nНет активных матчей"
        else:
            for match in live_matches:
                summary += self.format_match_info(match) + "\n" + "-" * 30 + "\n"

        # Разбиваем сообщение на части и отправляем
        message_parts = split_message(summary)
        for i, part in enumerate(message_parts):
            try:
                await telegram_bot.send_message(
                    chat_id=chat_id,
                    text=part,
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
            except Exception as e:
                self.logger.error(f"Ошибка при отправке части {i + 1} сообщения: {str(e)}")
                try:
                    error_msg = f"Ошибка при отправке сводки (часть {i + 1}). Пожалуйста, попробуйте позже."
                    await telegram_bot.send_message(
                        chat_id=chat_id,
                        text=error_msg,
                        parse_mode='HTML'
                    )
                except:
                    pass

    def parse_odds_cell(self, html_content):
        """Специализированный метод для парсинга коэффициентов из элементов oddsCell__odd

        Аргументы:
            html_content (str): HTML-код содержащий элементы oddsCell__odd

        Возвращает:
            dict: Словарь с коэффициентами и направлениями их изменения
        """
        try:
            self.logger.info("Начало парсинга коэффициентов из oddsCell__odd")
            soup = BeautifulSoup(html_content, 'html.parser')
            odds_data = {}

            odds_cells = soup.select('.oddsCell__odd')
            if len(odds_cells) >= 2:
                self.logger.info(f"Найдено {len(odds_cells)} элементов oddsCell__odd")

                # Получаем коэффициенты и направления
                # Первый коэффициент (обычно домашний)
                if odds_cells[0]:
                    # Ищем стрелки направления коэффициента
                    arrow_up = odds_cells[0].select('.arrow.arrowUp-ico')
                    arrow_down = odds_cells[0].select('.arrow.arrowDown-ico')

                    home_direction = None
                    if arrow_up:
                        home_direction = 'up'
                    elif arrow_down:
                        home_direction = 'down'

                    # Получаем текст коэффициента из span внутри ячейки
                    home_span = odds_cells[0].select('span')
                    if home_span:
                        home_text = re.sub(r'[^0-9.]', '', home_span[0].text)
                        if home_text and self._is_valid_odds(home_text):
                            odds_data["home_odds"] = home_text
                            self.logger.info(f"Найден коэффициент на домашнего игрока: {home_text}")
                            if home_direction:
                                odds_data["home_odds_direction"] = home_direction

                # Второй коэффициент (обычно гостевой)
                if len(odds_cells) > 1 and odds_cells[1]:
                    # Ищем стрелки направления коэффициента
                    arrow_up = odds_cells[1].select('.arrow.arrowUp-ico')
                    arrow_down = odds_cells[1].select('.arrow.arrowDown-ico')

                    away_direction = None
                    if arrow_up:
                        away_direction = 'up'
                    elif arrow_down:
                        away_direction = 'down'

                    # Получаем текст коэффициента из span внутри ячейки
                    away_span = odds_cells[1].select('span')
                    if away_span:
                        away_text = re.sub(r'[^0-9.]', '', away_span[0].text)
                        if away_text and self._is_valid_odds(away_text):
                            odds_data["away_odds"] = away_text
                            self.logger.info(f"Найден коэффициент на гостевого игрока: {away_text}")
                            if away_direction:
                                odds_data["away_odds_direction"] = away_direction

            # Дополнительно проверяем title атрибуты для истории коэффициентов
            for i, cell in enumerate(odds_cells):
                title = cell.get('title', '')
                if title and '»' in title:
                    try:
                        parts = title.split('»')
                        if len(parts) == 2:
                            old_odds = re.sub(r'[^0-9.]', '', parts[0].strip())
                            new_odds = re.sub(r'[^0-9.]', '', parts[1].strip())
                            if i == 0 and old_odds and new_odds and self._is_valid_odds(old_odds):
                                odds_data["home_odds_old"] = old_odds
                                self.logger.info(f"Предыдущий коэффициент на домашнего игрока: {old_odds}")
                            elif i == 1 and old_odds and new_odds and self._is_valid_odds(old_odds):
                                odds_data["away_odds_old"] = old_odds
                                self.logger.info(f"Предыдущий коэффициент на гостевого игрока: {old_odds}")
                    except Exception as e:
                        self.logger.debug(f"Ошибка при извлечении истории коэффициентов: {str(e)}")

            return odds_data

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге коэффициентов из oddsCell__odd: {str(e)}")
            return {}

    async def filter_and_send_favorites_from_json(self, json_file, telegram_bot, chat_id):
        """Загружает данные из JSON файла, фильтрует матчи по критериям и отправляет результаты в Telegram"""
        try:
            self.logger.info(f"Загрузка данных из файла: {json_file}")

            # Загружаем данные из JSON файла
            with open(json_file, 'r', encoding='utf-8') as f:
                live_matches = json.load(f)

            self.logger.info(f"Загружено {len(live_matches)} матчей из JSON файла")

            # Вызываем стандартную функцию фильтрации и отправки
            await self.filter_and_send_favorites(live_matches, telegram_bot, chat_id)

        except Exception as e:
            self.logger.error(f"Ошибка при загрузке и обработке данных из JSON файла: {str(e)}")
            traceback_str = traceback.format_exc()
            self.logger.error(f"Стек вызовов: {traceback_str}")
            await telegram_bot.send_message(
                chat_id=chat_id,
                text=f"❌ <b>Ошибка при обработке JSON файла</b>\n\n{str(e)}",
                parse_mode='HTML'
            )


# === ФУНКЦИЯ СОЗДАНИЯ ИНВОЙСА ДЛЯ ОПЛАТЫ КРИПТОЙ ===
async def send_crypto_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = get_user_id(update)
    username = update.effective_user.username or ''
    amount = 299  # Сумма в рублях
    description = 'Подбор лучших 3 матчей на основе статистики в реальном времени'
    payload = f'crypto-premium-{user_id}-{int(time.time())}'

    # Создаём инвойс через Crypto Pay API (только TON, эквивалент 299 RUB)
    headers = {
        'Crypto-Pay-API-Token': CRYPTO_PAY_API_TOKEN,
        'Content-Type': 'application/json'
    }
    data = {
        'currency_type': 'fiat',  # Валюта — фиат
        'fiat': 'RUB',  # Рубли
        'amount': str(amount),  # 299 рублей
        'accepted_assets': 'TON',  # Только TON
        'description': description,
        'payload': payload,
        'allow_comments': False,
        'allow_anonymous': True
    }
    try:
        resp = requests.post(CRYPTO_PAY_API_URL + 'createInvoice', headers=headers, json=data, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        if result.get('ok') and result.get('result'):
            invoice = result['result']
            pay_url = invoice.get('bot_invoice_url') or invoice.get('pay_url')
            invoice_id = invoice.get('invoice_id')
            msg = f'💸 <b>Оплата криптовалютой (TON, экв. 299₽)</b>\n\nПерейдите по ссылке для оплаты:\n<a href="{pay_url}">Оплатить</a>'
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_web_page_preview=False)
            await poll_crypto_invoice_status(context, chat_id, invoice_id, user_id, username)
        else:
            await context.bot.send_message(chat_id=chat_id,
                                           text='Ошибка при создании инвойса для оплаты криптой. Попробуйте позже.')
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f'Ошибка при создании инвойса: {e}')


# === ФУНКЦИЯ POLLING ДЛЯ ПРОВЕРКИ ОПЛАТЫ КРИПТОЙ ===
async def poll_crypto_invoice_status(context, chat_id, invoice_id, user_id, username, max_attempts=80, delay=15):
    import traceback
    headers = {
        'Crypto-Pay-API-Token': CRYPTO_PAY_API_TOKEN,
        'Content-Type': 'application/json'
    }
    for attempt in range(max_attempts):
        try:
            resp = requests.get(CRYPTO_PAY_API_URL + f'getInvoices?invoice_ids={invoice_id}', headers=headers,
                                timeout=10)
            resp.raise_for_status()
            result = resp.json()
            invoices = result.get('result', {}).get('items', [])
            if isinstance(invoices, list) and invoices:
                invoice = invoices[0]
                if invoice.get('status') == 'paid':
                    # Оплата прошла успешно
                    # add_user_points(user_id, username, 1000)  # Начисляем баллы/премиум (убрано)
                    await context.bot.send_message(chat_id=chat_id,
                                                   text='✅ Оплата криптовалютой прошла успешно! Премиум доступ активирован на 30 дней.\nЗапускаю парсинг матчей...')
                    return
            else:
                await context.bot.send_message(chat_id=chat_id,
                                               text=f'Инвойс не найден или не создан. Ответ API: {result}')
                await asyncio.sleep(delay)
                continue
            await asyncio.sleep(delay)
        except Exception as e:
            tb = traceback.format_exc()
            await context.bot.send_message(chat_id=chat_id, text=f'Ошибка при проверке статуса оплаты:\n{e!r}\n{tb}')
            return
    await context.bot.send_message(chat_id=chat_id,
                                   text='❌ Время ожидания оплаты истекло. Если вы оплатили — напишите в поддержку.')


# --- Вспомогательные функции для баллов и промо-кодов ---
def load_users():
    with FileLock("users.json.lock"):
        if not os.path.exists(USERS_FILE):
            return {}
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)


def save_users(users):
    with FileLock("users.json.lock"):
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)


def load_promos():
    with FileLock("promo_codes.json.lock"):
        if not os.path.exists(PROMO_FILE):
            return []
        with open(PROMO_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)


def save_promos(promos):
    with FileLock("promo_codes.json.lock"):
        with open(PROMO_FILE, 'w', encoding='utf-8') as f:
            json.dump(promos, f, ensure_ascii=False, indent=2)


def get_user_id(update):
    # Возвращает username если есть, иначе id
    if update.effective_user.username:
        return update.effective_user.username
    return str(update.effective_user.id)


def get_user_points(user_id):
    users = load_users()
    return users.get(user_id, {}).get('points', 0)


def add_user_points(user_id, username, points):
    users = load_users()
    if user_id not in users:
        users[user_id] = {'username': username, 'points': 0}
    users[user_id]['points'] += points
    save_users(users)


def set_user_points(user_id, username, points):
    users = load_users()
    if user_id not in users:
        users[user_id] = {'username': username, 'points': 0}
    users[user_id]['points'] = points
    save_users(users)


# --- Генерация промо-кодов ---
def generate_promo_codes(n):
    chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
    codes = []
    for _ in range(n):
        code = ''.join(random.choices(chars, k=8))
        codes.append(code)
    promos = load_promos()
    promos.extend(codes)
    save_promos(promos)
    return codes


# --- Главное меню ---
def main_menu(points, is_admin=False):
    keyboard = [
        [InlineKeyboardButton('ПОДБОР МАТЧА', callback_data='pick_match')],
        [InlineKeyboardButton('СЛУЖБА ПОДДЕРЖКИ', callback_data='support')],
        [InlineKeyboardButton('ИНСТРУКЦИЯ ПО РАБОТЕ👈🏻', callback_data='instructions')],
        [InlineKeyboardButton(f'Баланс: {points} баллов', callback_data='balance')]
    ]
    if is_admin:
        keyboard.append([InlineKeyboardButton('Сгенерировать промо-коды', callback_data='admin_generate_promo')])
        keyboard.append([InlineKeyboardButton('Выгрузить промо-коды', callback_data='admin_export_promos')])
    return InlineKeyboardMarkup(keyboard)


def pick_match_menu():
    """Создание меню для подбора матча"""
    keyboard = [
        [InlineKeyboardButton('ОПЛАТА С КАРТЫ', callback_data='pay_rub')],
        [InlineKeyboardButton('ОПЛАТА КРИПТОЙ', callback_data='pay_crypto')],
        [InlineKeyboardButton('ОПЛАТА СБП', callback_data='pay_sbp')],
        [InlineKeyboardButton('ВВЕСТИ ПРОМО-КОД', callback_data='enter_promo')],
        [InlineKeyboardButton('ОБМЕНЯТЬ БАЛЛЫ НА ПРОМО-КОД', callback_data='exchange_points')],
        [InlineKeyboardButton('« НАЗАД', callback_data='main_menu')]
    ]
    return InlineKeyboardMarkup(keyboard)


# --- Обработчики ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = get_user_id(update)
    username = update.effective_user.username or ''
    users = load_users()
    if user_id not in users:
        users[user_id] = {'username': username, 'points': 0}
        save_users(users)
    points = users[user_id]['points']
    is_admin = (user_id in ADMIN_ID)

    # Отправляем приветственное сообщение с новым текстом
    welcome_text = (
        "Ваш личный помощник по заработку на спорте.\n\n"
        "HoldStat соберет для вас всю статистику в режиме реального времени, "
        "и выберет лучшие варианты для работы на геймах и заработку за пару минут"
    )

    # Отправляем гифку или видео если это первое сообщение или команда /start
    if update.message and (not context.user_data.get('welcomed') or update.message.text == '/start'):
        # Пытаемся загрузить гифку. Можно указать URL гифки или путь к локальному файлу
        try:
            # Проверяем наличие локальных файлов
            gif_path = "welcome.gif"
            mp4_path = "welcome.mp4"

            if os.path.exists(mp4_path):
                # Если есть MP4, используем его
                with open(mp4_path, 'rb') as video:
                    await update.message.reply_video(
                        video=video,
                        caption=welcome_text,
                        reply_markup=main_menu(points, is_admin)
                    )
            elif os.path.exists(gif_path):
                # Если есть GIF, используем его
                with open(gif_path, 'rb') as gif:
                    await update.message.reply_animation(
                        animation=gif,
                        caption=welcome_text,
                        reply_markup=main_menu(points, is_admin)
                    )
            else:
                # Иначе используем URL
                gif_url = "https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExMzloY3prMmkxaXJqenk1aWRqZjU3dGl6Y2tlNGY4eXN6aWRjZnA2NiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cMnPcJfnJI9K9HGIHk/giphy.gif"
                await update.message.reply_animation(
                    animation=gif_url,
                    caption=welcome_text,
                    reply_markup=main_menu(points, is_admin)
                )
        except Exception as e:
            logging.error(f"Ошибка при отправке анимации: {e}")
            # В случае ошибки отправляем просто текст
            await update.message.reply_text(welcome_text, reply_markup=main_menu(points, is_admin))

        context.user_data['welcomed'] = True
    else:
        # Если это не первое сообщение или это callback_query
        if update.callback_query:
            await update.callback_query.edit_message_text(welcome_text, reply_markup=main_menu(points, is_admin))
        else:
            await update.message.reply_text(welcome_text, reply_markup=main_menu(points, is_admin))


async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    user_id = get_user_id(update)
    username = update.effective_user.username or ''
    users = load_users()
    if user_id not in users:
        users[user_id] = {'username': username, 'points': 0}
        save_users(users)
    points = users[user_id]['points']
    is_admin = (user_id in ADMIN_ID)

    if data == 'main_menu':
        welcome_text = (
            "Ваш личный помощник по заработку на спорте.\n\n"
            "HoldStat соберет для вас всю статистику в режиме реального времени, "
            "и выберет лучшие варианты для работы на геймах и заработку за пару минут"
        )

        # Отправляем новое сообщение с гифкой или видео, а не редактируем старое
        try:
            # Безопасно удаляем клавиатуру у предыдущего сообщения, без редактирования текста
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception as e:
                logging.error(f"Ошибка при удалении клавиатуры: {e}")
                # Если не удалось отредактировать сообщение, пробуем его удалить
                try:
                    await context.bot.delete_message(
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id
                    )
                except Exception as delete_error:
                    logging.error(f"Ошибка при удалении сообщения: {delete_error}")

            # Проверяем наличие локальных файлов
            gif_path = "welcome.gif"
            mp4_path = "welcome.mp4"

            if os.path.exists(mp4_path):
                # Если есть MP4, используем его
                with open(mp4_path, 'rb') as video:
                    await context.bot.send_video(
                        chat_id=query.message.chat_id,
                        video=video,
                        caption=welcome_text,
                        reply_markup=main_menu(points, is_admin)
                    )
            elif os.path.exists(gif_path):
                # Если есть GIF, используем его
                with open(gif_path, 'rb') as gif:
                    await context.bot.send_animation(
                        chat_id=query.message.chat_id,
                        animation=gif,
                        caption=welcome_text,
                        reply_markup=main_menu(points, is_admin)
                    )
            else:
                # Иначе используем URL
                gif_url = "https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExMzloY3prMmkxaXJqenk1aWRqZjU3dGl6Y2tlNGY4eXN6aWRjZnA2NiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cMnPcJfnJI9K9HGIHk/giphy.gif"
                await context.bot.send_animation(
                    chat_id=query.message.chat_id,
                    animation=gif_url,
                    caption=welcome_text,
                    reply_markup=main_menu(points, is_admin)
                )
        except Exception as e:
            logging.error(f"Ошибка при отправке анимации в главном меню: {e}")
            # В случае ошибки отправляем новое текстовое сообщение вместо редактирования
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=welcome_text,
                reply_markup=main_menu(points, is_admin)
            )
    elif data == 'instructions':
        instructions_text = (
            "ИНСТРУКЦИЯ ПО РАБОТЕ\n\n"
            "Нажмите на кнопку ниже, чтобы перейти к подробной инструкции:"
        )
        keyboard = [
            [InlineKeyboardButton('ОТКРЫТЬ ИНСТРУКЦИЮ', url='https://t.me/holdstat/14858')],
            [InlineKeyboardButton('« НАЗАД', callback_data='main_menu')]
        ]
        try:
            await query.edit_message_text(instructions_text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения инструкции: {e}")
            # Отправляем новое сообщение вместо редактирования
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=instructions_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    elif data == 'pick_match':
        match_text = (
            'ПОДБОР МАТЧА:\n\n'
            'Подбор лучших 3 матчей из всех на основе статистики в реальном времени и консультации по работе:\n'
            'Всего 299₽\n\n'
            'Вы сможете отработать от 1 до 5 ставок по матчам за один подбор и за самое быстрое время.'
        )
        try:
            await query.edit_message_text(match_text, reply_markup=pick_match_menu())
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения подбора матча: {e}")
            # Отправляем новое сообщение вместо редактирования
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=match_text,
                reply_markup=pick_match_menu()
            )
    elif data == 'support':
        support_text = 'Связь: @HoldStatSupportBot'
        try:
            await query.edit_message_text(support_text)
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения поддержки: {e}")
            # Отправляем новое сообщение вместо редактирования
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text=support_text)
    elif data == 'balance':
        balance_text = f'У тебя {points} баллов.'
        try:
            await query.edit_message_text(balance_text, reply_markup=main_menu(points, is_admin))
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения баланса: {e}")
            # Отправляем новое сообщение вместо редактирования
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=balance_text,
                reply_markup=main_menu(points, is_admin)
            )
    elif data == 'pay_rub':
        # Вызываем функцию отправки счета
        try:
            await send_invoice(update, context)
            await query.edit_message_text('Счет на оплату отправлен!')
        except Exception as e:
            logging.error(f"Ошибка при отправке счета: {e}")
            # Отправляем новое сообщение вместо редактирования
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text='Счет на оплату отправлен!')
            await send_invoice(update, context)
    elif data == 'pay_crypto':
        await send_crypto_invoice(update, context)
    elif data == 'enter_promo':
        promo_text = 'Введи промо-код одним сообщением:'
        try:
            await query.edit_message_text(promo_text)
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения ввода промокода: {e}")
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text=promo_text)
        context.user_data['awaiting_promo'] = True
    elif data == 'exchange_points':
        if points >= 15:
            promos = load_promos()
            if promos:
                code = promos.pop(0)
                save_promos(promos)
                # set_user_points(user_id, username, points - 15)  # Убрано начисление баллов
                promo_text = f'Твой промо-код: <b>{code}</b>\n(одноразовый, введи его в меню «ПОДБОР МАТЧА» — «ВВЕСТИ ПРОМО-КОД»)'
                try:
                    await query.edit_message_text(promo_text, parse_mode='HTML')
                except Exception as e:
                    logging.error(f"Ошибка при редактировании сообщения с промокодом: {e}")
                    try:
                        await context.bot.delete_message(chat_id=query.message.chat_id,
                                                         message_id=query.message.message_id)
                    except:
                        pass
                    await context.bot.send_message(
                        chat_id=query.message.chat_id,
                        text=promo_text,
                        parse_mode='HTML'
                    )
            else:
                no_promo_text = 'Промо-коды закончились, напиши @HoldStatSupportBot!'
                try:
                    await query.edit_message_text(no_promo_text)
                except Exception as e:
                    logging.error(f"Ошибка при редактировании сообщения об отсутствии промокодов: {e}")
                    try:
                        await context.bot.delete_message(chat_id=query.message.chat_id,
                                                         message_id=query.message.message_id)
                    except:
                        pass
                    await context.bot.send_message(chat_id=query.message.chat_id, text=no_promo_text)
        else:
            not_enough_points_text = f'Недостаточно баллов! Нужно 15, у тебя {points}.'
            try:
                await query.edit_message_text(not_enough_points_text, reply_markup=pick_match_menu())
            except Exception as e:
                logging.error(f"Ошибка при редактировании сообщения о недостатке баллов: {e}")
                try:
                    await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
                except:
                    pass
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=not_enough_points_text,
                    reply_markup=pick_match_menu()
                )
    elif data == 'admin_generate_promo' and is_admin:
        admin_text = 'Введи количество промо-кодов одним сообщением:'
        try:
            await query.edit_message_text(admin_text)
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения для админа: {e}")
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text=admin_text)
        context.user_data['awaiting_admin_promo'] = True
    elif data == 'load_from_json':
        # Получаем список JSON файлов
        json_files = []
        for file in os.listdir():
            if file.startswith('live_matches_') and file.endswith('.json'):
                json_files.append(file)

        if not json_files:
            no_files_text = 'Нет доступных JSON файлов с данными о матчах.'
            try:
                await query.edit_message_text(no_files_text)
            except Exception as e:
                logging.error(f"Ошибка при редактировании сообщения об отсутствии файлов: {e}")
                try:
                    await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
                except:
                    pass
                await context.bot.send_message(chat_id=query.message.chat_id, text=no_files_text)
            return

        # Сортируем файлы по дате (самые новые сверху)
        json_files.sort(reverse=True)

        # Создаем клавиатуру с файлами
        keyboard = []
        for file in json_files[:10]:  # Ограничиваем 10 файлами
            keyboard.append([InlineKeyboardButton(file, callback_data=f"json_file:{file}")])

        keyboard.append([InlineKeyboardButton("« Назад", callback_data="pick_match")])

        files_text = 'Выберите JSON файл для анализа:'
        try:
            await query.edit_message_text(
                files_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения со списком файлов: {e}")
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=files_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    elif data.startswith('json_file:'):
        # Получаем имя файла из data
        json_file = data.split(':', 1)[1]

        if not os.path.exists(json_file):
            file_not_found_text = f'Файл {json_file} не найден.'
            try:
                await query.edit_message_text(file_not_found_text)
            except Exception as e:
                logging.error(f"Ошибка при редактировании сообщения о ненайденном файле: {e}")
                try:
                    await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
                except:
                    pass
                await context.bot.send_message(chat_id=query.message.chat_id, text=file_not_found_text)
            return

        analysis_start_text = f"Начинаю анализ файла {json_file}..."
        try:
            await query.edit_message_text(analysis_start_text)
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения о начале анализа: {e}")
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text=analysis_start_text)

        # Создаем экземпляр парсера и запускаем обработку
        parser = TennisParser()
        try:
            # Загружаем и обрабатываем данные из JSON файла
            await parser.filter_and_send_favorites_from_json(json_file, context.bot, query.message.chat_id)

            # Добавляем кнопку для запроса сводки
            keyboard = [
                [InlineKeyboardButton("Запросить полную сводку", callback_data=f"summary_from_json:{json_file}")],
                [InlineKeyboardButton("« Назад", callback_data="pick_match")]]

            analysis_complete_text = "Анализ завершен. Нажмите кнопку ниже, чтобы получить полную сводку по матчам:"
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=analysis_complete_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Ошибка при анализе файла: {str(e)}"
            )
        finally:
            parser.close()
    elif data.startswith('summary_from_json:'):
        # Получаем имя файла из data
        json_file = data.split(':', 1)[1]

        if not os.path.exists(json_file):
            file_not_found_text = f'Файл {json_file} не найден.'
            try:
                await query.edit_message_text(file_not_found_text)
            except Exception as e:
                logging.error(f"Ошибка при редактировании сообщения о ненайденном файле: {e}")
                try:
                    await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
                except:
                    pass
                await context.bot.send_message(chat_id=query.message.chat_id, text=file_not_found_text)
            return

        summary_loading_text = f"Загружаю данные из файла {json_file} для формирования сводки..."
        try:
            await query.edit_message_text(summary_loading_text)
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения о загрузке сводки: {e}")
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text=summary_loading_text)

        # Создаем экземпляр парсера и отправляем сводку
        parser = TennisParser()
        try:
            # Загружаем данные из JSON файла
            with open(json_file, 'r', encoding='utf-8') as f:
                live_matches = json.load(f)

            # Отправляем сводку
            await parser.send_summary_to_telegram(live_matches, context.bot, query.message.chat_id)

            # Возвращаем кнопку для возврата в меню
            keyboard = [[InlineKeyboardButton("« Назад", callback_data="pick_match")]]
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="Сводка успешно отправлена!",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Ошибка при формировании сводки: {str(e)}"
            )
        finally:
            parser.close()
    elif data == 'request_summary':
        summary_text = 'Формирую полную сводку по матчам...'
        try:
            await query.edit_message_text(summary_text)
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения запроса сводки: {e}")
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
            except:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text=summary_text)

        # Получаем сохраненные матчи из данных бота
        live_matches = context.bot_data.get('live_matches', [])
        if live_matches:
            # Создаем экземпляр парсера и отправляем сводку
            parser = TennisParser()
            try:
                await parser.send_summary_to_telegram(live_matches, context.bot, query.message.chat_id)

                # Добавляем кнопку для возврата в меню
                keyboard = [[InlineKeyboardButton("« Назад", callback_data="main_menu")]]
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="Сводка успешно отправлена!",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except Exception as e:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"Ошибка при формировании сводки: {str(e)}"
                )
            finally:
                parser.close()
        else:
            no_matches_text = "Нет данных о матчах. Сначала выполните парсинг."
            try:
                await query.edit_message_text(no_matches_text)
            except Exception as e:
                logging.error(f"Ошибка при редактировании сообщения об отсутствии матчей: {e}")
                try:
                    await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
                except:
                    pass
                await context.bot.send_message(chat_id=query.message.chat_id, text=no_matches_text)
    elif data == 'show_matches':
        # Получаем сохраненные матчи из данных бота
        live_matches = context.bot_data.get('live_matches', [])
        if live_matches:
            await query.edit_message_text('Формирую информацию о матчах...')
            parser = TennisParser()
            await parser.send_summary_to_telegram(live_matches, context.bot, chat_id=query.message.chat_id)
            # Возвращаем меню после отправки информации
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text='Выберите действие:',
                reply_markup=pick_match_menu()
            )
        else:
            await query.edit_message_text(
                'В данный момент нет активных матчей.',
                reply_markup=pick_match_menu()
            )
    elif data == 'pay_sbp':
        await send_sbp_invoice(update, context)
    elif data == 'admin_export_promos' and is_admin:
        promo_file = PROMO_FILE
        if os.path.exists(promo_file):
            try:
                await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=open(promo_file, 'rb'),
                    filename=promo_file,
                    caption='Файл с промо-кодами'
                )
            except Exception as e:
                await context.bot.send_message(chat_id=query.message.chat_id, text=f'Ошибка при отправке файла: {e}')
        else:
            await context.bot.send_message(chat_id=query.message.chat_id, text='Файл с промо-кодами не найден.')
    else:
        await query.edit_message_text('Неизвестная команда.')


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Обрабатываем только личные сообщения
    if update.effective_chat.type != "private":
        return
    user_id = get_user_id(update)
    username = update.effective_user.username or ''
    users = load_users()
    if user_id not in users:
        users[user_id] = {'username': username, 'points': 0}
        save_users(users)
    points = users[user_id]['points']
    is_admin = (user_id in ADMIN_ID)
    if context.user_data.get('awaiting_promo'):
        code = update.message.text.strip().upper()
        promos = load_promos()
        if code in promos:
            promos.remove(code)
            save_promos(promos)
            await update.message.reply_text('Промо-код принят! Запускаю подбор...')
            context.user_data['awaiting_promo'] = False
            # Запуск парсера и отправка результата
            def start_parsing():
                try:
                    print(f"Запускаю парсинг для пользователя {update.effective_chat.id}")
                    logging.info(f"Запускаю парсинг для пользователя {update.effective_chat.id}")
                    parsing_worker(update.effective_chat.id, context, None)  # Передаем None вместо loop
                    print(f"Парсинг для пользователя {update.effective_chat.id} завершен")
                    logging.info(f"Парсинг для пользователя {update.effective_chat.id} завершен")
                except Exception as e:
                    print(f"Ошибка при парсинге: {e}")
                    logging.error(f"Ошибка при парсинге: {e}")
                    traceback.print_exc()
                    try:
                        # Отправляем сообщение об ошибке напрямую через HTTP запрос
                        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                        data = {
                            "chat_id": update.effective_chat.id,
                            "text": f"Произошла ошибка при парсинге: {e}"
                        }
                        requests.post(url, data=data)
                    except Exception as send_error:
                        print(f"Не удалось отправить сообщение об ошибке: {send_error}")
            threading.Thread(target=start_parsing).start()
            return
        else:
            await update.message.reply_text('Промо-код не найден или уже использован.',
                                            reply_markup=main_menu(points, is_admin))
        context.user_data['awaiting_promo'] = False
    elif context.user_data.get('awaiting_admin_promo') and is_admin:
        try:
            n = int(update.message.text.strip())
            codes = generate_promo_codes(n)
            await update.message.reply_text(f'Сгенерировано {n} промо-кодов:\n' + '\n'.join(codes),
                                            reply_markup=main_menu(points, is_admin))
        except Exception as e:
            await update.message.reply_text('Введи число, например: 5', reply_markup=main_menu(points, is_admin))
        context.user_data['awaiting_admin_promo'] = False
    elif context.user_data.get('awaiting_email_for_sbp'):
        email = update.message.text.strip()
        context.user_data['email'] = email
        context.user_data['awaiting_email_for_sbp'] = False
        await update.message.reply_text('Спасибо! Формирую ссылку на оплату...')
        await send_sbp_invoice(update, context)
        return
    else:
        user_id = update.effective_user.id
        now = datetime.now()
        print(1)
        # Обновляем данные из последнего JSON-файла перед обращением к GPT
        update_user_state_from_latest_json(user_id)
        state = get_user_state(user_id)
        print(2)
        if state:
            try:
                state_time = datetime.fromisoformat(state["timestamp"])
                if (now - state_time).total_seconds() <= 3600:
                    user_message = update.message.text
                    gpt_response = await ask_chatgpt(update, user_message)
                    print(3)
                    print(gpt_response)
                    await update.message.reply_text(clean_html_for_telegram(gpt_response), parse_mode='HTML')
                    print(4)

                    return
            except Exception as e:
                print(f"Ошибка чтения состояния пользователя: {e}")
        await update.message.reply_text('Используй меню для управления ботом.',
                                        reply_markup=main_menu(points, is_admin))


# --- Админ-команда ---
async def generate_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = get_user_id(update)
    if user_id not in ADMIN_ID:
        await update.message.reply_text('Нет доступа.')
        return
    try:
        n = int(context.args[0])
        codes = generate_promo_codes(n)
        await update.message.reply_text(f'Сгенерировано {n} промо-кодов:\n' + '\n'.join(codes))
    except Exception as e:
        await update.message.reply_text('Используй: /generate_promo 5')


# --- Новый обработчик для чата комментариев ---
SECOND_BOT_API = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'


async def comment_chat_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import logging
    logging.info(
        f"Получено сообщение в чате: {update.effective_chat.id}, от: {update.effective_user.id}, текст: {update.message.text}")
    if update.effective_chat.id != COMMENTS_CHAT_ID:
        return
    user = update.effective_user
    user_id = get_user_id(update)
    username = user.username or ''
    # Начисляем балл (убрано)
    # add_user_points(user_id, username, 1)
    points = get_user_points(user_id)
    # Формируем сообщение
    msg = (
        'Василий, Саня начислил тебе 1 Балл в @HoldStatBot за поддержку канала комментарием, красавчик!\n\n'
        f'У тебя: {points} баллов,\n'
        'чтобы получить бесплатный подбор матчей тебе нужно 15 баллов.\n'
        'Обменять баллы на подбор матча можешь прямо в боте.\n\n'
        'ты красава!\nЗаходов тебе уверенных, держим газ и считаем цифры!'
    )
    # Отвечаем в том же чате
    await update.message.reply_text(msg)
    # Отправляем личное сообщение через второй бот
    data = {'chat_id': user.id, 'text': msg}
    try:
        import requests
        requests.post(SECOND_BOT_API, data=data)
    except Exception as e:
        logging.error(f'Ошибка отправки сообщения через второй бот: {e}')


def load_user_states():
    with FileLock("user_states.json.lock"):
        if not os.path.exists(USER_STATES_FILE):
            return {}
        with open(USER_STATES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)


def save_user_states(states):
    with FileLock("user_states.json.lock"):
        with open(USER_STATES_FILE, "w", encoding="utf-8") as f:
            json.dump(states, f, ensure_ascii=False, indent=4)


def update_user_state(chat_id, data=None, new_message=None):
    states = load_user_states()
    if str(chat_id) not in states:
        states[str(chat_id)] = {
            "timestamp": datetime.now().isoformat(),
            "match_data": [],
            "chat_history": []
        }

    if data is not None:
        states[str(chat_id)]["match_data"] = data
        states[str(chat_id)]["timestamp"] = datetime.now().isoformat()

    # Если добавляется новое сообщение
    if new_message is not None:
        if 'chat_history' not in states[str(chat_id)]:
            states[str(chat_id)]['chat_history'] = []
        
        # Добавляем новое сообщение
        states[str(chat_id)]['chat_history'].append(new_message)
        
        # Ограничиваем историю 10 последними сообщениями
        if len(states[str(chat_id)]['chat_history']) > 10:
            states[str(chat_id)]['chat_history'] = states[str(chat_id)]['chat_history'][-10:]

    save_user_states(states)


def update_user_state_from_latest_json(chat_id):
    """Обновляет данные пользователя из самого последнего JSON-файла с матчами (по дате в имени файла)"""
    import glob
    import os
    from datetime import datetime
    try:
        # Ищем все файлы live_matches_*.json
        import glob
        json_files = glob.glob("live_matches_*.json")
        if not json_files:
            print("Не найдены файлы live_matches_*.json")
            return False
            
        # Сортируем по имени файла (по временной метке в имени)
        # Формат имени: live_matches_YYYYMMDD_HHMMSS.json
        json_files.sort(reverse=True)  # Сортировка по имени в обратном порядке
        
        # Дополнительно проверяем по времени модификации
        # и берем самый свежий файл
        if len(json_files) > 1:
            # Сортируем еще и по времени модификации, чтобы точно взять самый новый
            json_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        # Берем самый свежий файл
        latest_json = json_files[0]
        print(f"Найден самый свежий файл: {latest_json} (время модификации: {datetime.fromtimestamp(os.path.getmtime(latest_json))})")
        
        # Загружаем данные
        with open(latest_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Обновляем состояние пользователя
        update_user_state(chat_id, data=data)
        # --- Очищаем историю чата пользователя при обновлении данных ---
        states = load_user_states()
        if str(chat_id) in states:
            states[str(chat_id)]["chat_history"] = []
            save_user_states(states)
        # --- конец ---
        print(f"Данные обновлены из файла {latest_json}")
        return True
    except Exception as e:
        print(f"Ошибка при обновлении данных из JSON: {e}")
        traceback.print_exc()  # Выводим полный стек-трейс для отладки
        return False


def get_user_state(chat_id):
    """Получает состояние пользователя"""
    states = load_user_states()
    user_data = states.get(str(chat_id))
    if not user_data:
        return None

    try:
        timestamp = datetime.fromisoformat(user_data["timestamp"])
        if (datetime.now() - timestamp).total_seconds() <= 3600:
            return user_data
    except Exception as e:
        print(f"Ошибка чтения состояния пользователя: {e}")
    return None


async def ask_chatgpt(update: Update, user_message: str):
    user_id = update.effective_user.id
    
    # Обновляем данные пользователя из последнего JSON-файла перед обращением к GPT
    # В дополнение к обновлению в text_handler - для дополнительной защиты
    update_user_state_from_latest_json(user_id)
    
    state = get_user_state(user_id)
    
    # Если нет состояния или данные устарели, выводим сообщение
    if not state or not state.get("match_data"):
        print(f"Предупреждение: для пользователя {user_id} нет данных о матчах или они устарели")

    system_prompt = """Ты — Френсис Бро, неформальный и дружелюбный искусственный интеллект, созданный помогать с отбором лучших теннисных лайв-матчей для ставок по продуманной стратегии. Ты шаришь за теннис, ставки и всегда топишь за своего бро — пользователя. Работаешь легко, по-братски, с метафорами, юмором, но даёшь чёткие и прибыльные рекомендации.
    
    ВАЖНО: Давай рекомендации ТОЛЬКО из того списка матчей, который система отобрала как фаворитов и отправила пользователю. Не придумывай и не выдумывай матчи сам, используй только ту информацию, которую тебе передала система в данных.

    Если в данных нет ни одного подходящего матча — просто напиши: 'Нет подходящих матчей для рекомендаций.' Не придумывай ни одного матча сам, даже если очень хочется. Используй только те пары, которые есть в данных.

    1. Используй данные из JSON со статистикой теннисных лайв-матчей.
    2. Рекомендуй матчи ТОЛЬКО из тех, которые система уже отобрала и показала пользователю.
    3. Если в системе меньше 3 рекомендаций, так и скажи — "Сейчас отобрано только X хороших матчей"
    4. До приветствия выдай подборку:
     • Фамилия игрока
     • Его коэффициент
     • Статистика (% первой подачи, % выигранных очков на первой подаче)

    Вместе с выводом результатов, ОБЯЗАТЕЛЬНО предложи выбрать именно тот матч, где меньше всего геймов с начала матча сыграно. 
    Пока игрок не устал 
    И где коэффициент выше всего. 
    Но это исключительно твой совет и выбор за пользователем.

    Предложи если будут любые вопросы - задавать в чат. 

    ЕСЛИ таких трех вариантов нет, выдай сколько есть и напиши, что время такое по матчам, что их просто больше нет хороших - соответственно и хороших вариантов. 

    Вот все что есть, заработай и попробуй в более подходящее время.

    Затем продолжи:
     • Спроси: "Работал ли ты уже по системе или впервые заходишь на корт с Френсисом Бро?"
     • Если "нет", объясни стратегию:
     • Ставка на гейм аутсайдера на его подаче
     • Игра догоном: 10% банк → 30% → 60% (3 ступени)
     • Ставить на каждую подачу подряд
     • Цель — поймать закономерность: игрок с хорошей формой редко резко сдаёт, профит быстро

    В ходе общения обязательно:
     • Выясни:
     • На что пользователь хочет накопить / зачем прибавить к депозиту?
     • Выбрал ли уже игрока, с кем работать?
     • Поддерживай атмосферу:
     • Используй теннисные метафоры ("мы только разминаемся", "главное — не упустить свой гейм")
     • Шути, например: "Не бойся выходить на сетбол, бро, Френсис прикроет!"
     • В финале предложи доп. подборку (допродажу):
     • "Хочешь ещё одну порцию сочных матчей на сегодня? Закажем новую подачу!"

    Ты всегда на стороне игрока. Неформален, но даёшь чёткую ценность. Ты — Френсис Бро.

    Выдаешь как можно меньше информации кроме первых трех лидеров(их выдавай по образцу)
    И на кого ставить. 

    Не форматируй ответ как HTML (только используй выделение жирным <b> и </b>)
    
    И дальше консультируешь и спрашиваешь куда поставил - на что копим - как заработаем

    ВЫВОДИ ИНФОРМАЦИЮ ПО МАТЧАМ ТОЛЬКО ИЗ JSON ФАЙЛА
    
    Формат вывода "Выведи статистику по матчам":
    <b>Матч:</b> Де Минаур А. vs Шаповалов Д.
    <b>НАШ Фаворит:</b> Шаповалов Д.
    <b>Коэффициент:</b> 4.2
    <b>% первой подачи:</b> 63.0%
    <b>% выигр. очков на 1-й подаче:</b> 61.0%

    <b>Матч:</b> Наф С. vs Цакаревич С.
    <b>Фаворит:</b> Цакаревич С.
    <b>Коэффициент:</b> 3.84
    <b>% первой подачи:</b> 65.0%
    <b>% выигр. очков на 1-й подаче:</b> 70.0%

    <b>Матч:</b> Де Минаур А. vs Шаповалов Д.
    <b>Фаворит:</b> Шаповалов Д.
    <b>Коэффициент:</b> 4.2
    <b>% первой подачи:</b> 63.0%
    <b>% выигр. очков на 1-й подаче:</b> 61.0%
    """

    # Стартовая структура сообщений
    messages = [{"role": "system", "content": system_prompt}]

    # Добавим данные о матчах, если есть
    if state and "match_data" in state:
        try:
            match_data_str = json.dumps(state["match_data"], ensure_ascii=False, indent=2)
            messages.append({
                "role": "system",
                "content": f"Вот текущая информация о матчах в формате JSON:\n{match_data_str}"
            })
        except Exception as e:
            print(f"Ошибка сериализации match_data: {e}")

    # Если есть история — добавим её
    if state and state.get("chat_history"):
        messages += state["chat_history"]

    # Добавим новое сообщение от пользователя
    messages.append({"role": "user", "content": user_message})

    # Отправляем в OpenAI
    try:
        response = await asyncio.to_thread(
            openai.ChatCompletion.create,
            model="gpt-4.1-nano",
            messages=messages,
            temperature=0.8
        )
        assistant_reply = response['choices'][0]['message']['content']
        
        # Проверяем корректность HTML-тегов
        open_tags = assistant_reply.count("<b>")
        close_tags = assistant_reply.count("</b>")
        
        if open_tags != close_tags:
            # Если количество открывающих и закрывающих тегов не совпадает, заменяем их на символы **
            assistant_reply = assistant_reply.replace("<b>", "**").replace("</b>", "**")
            print(f"Исправлены некорректные HTML-теги в ответе GPT. Открытых: {open_tags}, закрытых: {close_tags}")
        
        # Заменяем все теги <br> на переносы строки
        assistant_reply = re.sub(r'<br\s*/?>', '\n', assistant_reply)
        
        # --- Универсальная фильтрация: только матчи из актуального JSON ---
        # Собираем все пары игроков из match_data
        valid_pairs = set()
        for match in state.get("match_data", []):
            home = match.get('home_player', '').strip()
            away = match.get('away_player', '').strip()
            if home and away:
                valid_pairs.add((home, away))
                valid_pairs.add((away, home))  # на случай, если GPT поменяет местами
        # Ищем все пары "Матч: Игрок vs Игрок" в ответе GPT
        found_pairs = set()
        for m in re.findall(r'Матч:\s*([^\n]+?)\s+vs\s+([^\n]+)', assistant_reply):
            found_pairs.add((m[0].strip(), m[1].strip()))
        # Если есть хотя бы одна пара, которой нет в valid_pairs — ошибка
        for pair in found_pairs:
            if pair not in valid_pairs:
                # Не отправляем ответ GPT, только предупреждение
                return ("⚠️ В ответе GPT обнаружены матчи, которых нет в актуальных данных. "
                        "Пожалуйста, запустите парсинг заново для получения свежих данных.\n"
                        f"Проблемная пара: {pair[0]} vs {pair[1]}")
        # --- конец фильтрации ---
    except Exception as e:
        print(f"Ошибка запроса к GPT: {e}")
        return "Ошибка получения ответа от GPT 😕"

    # Сохраняем в историю пользователя
    update_user_state(user_id, new_message={"role": "user", "content": user_message})
    update_user_state(user_id, new_message={"role": "assistant", "content": assistant_reply})

    return assistant_reply


# --- Регистрация новых обработчиков ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)


    # Функции для работы с платежами
    async def send_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправляет счет на оплату пользователю через ЮKassa"""
        chat_id = update.effective_chat.id

        # Тестовый токен провайдера ЮKassa
        provider_token = "390540012:LIVE:69502"

        # Название и описание товара
        title = "Анализ матчей"
        description = "Подбор лучших 3 матчей на основе статистики в реальном времени"

        # Полезная нагрузка для идентификации платежа
        payload = f"premium-{update.effective_user.id}-{int(time.time())}"

        # Валюта в формате ISO 4217
        currency = "RUB"

        # Стоимость товара в минимальных единицах валюты (копейки)
        price = 29900  # 299 рублей

        # Создаем список цен с одним элементом
        prices = [LabeledPrice("Анализ матчей", price)]

        # Опции запроса данных пользователя
        need_name = True
        need_email = True

        # Отправляем запрос на оплату
        await context.bot.send_invoice(
            chat_id=chat_id,
            title=title,
            description=description,
            payload=payload,
            provider_token=provider_token,
            currency=currency,
            prices=prices,
            need_name=need_name,
            need_email=need_email
        )


    async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает запрос предварительной проверки платежа"""
        query = update.pre_checkout_query

        # Здесь можно выполнить проверки перед подтверждением платежа
        # Например, проверить доступность товара в базе данных и т.д.

        # Для демонстрации просто подтверждаем все платежи
        await context.bot.answer_pre_checkout_query(
            pre_checkout_query_id=query.id,
            ok=True
        )


    async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает успешный платеж"""
        payment = update.message.successful_payment
        user_id = update.effective_user.id

        # Сохраняем информацию о платеже
        payment_info = {
            "user_id": user_id,
            "username": update.effective_user.username,
            "currency": payment.currency,
            "total_amount": payment.total_amount,
            "telegram_payment_charge_id": payment.telegram_payment_charge_id,
            "provider_payment_charge_id": payment.provider_payment_charge_id,
            "invoice_payload": payment.invoice_payload,
            "timestamp": int(time.time())
        }

        # Здесь можно сохранить информацию о платеже в базу данных
        # и активировать премиум-функции для пользователя

        # Добавим пользователю 30 дней премиума (1000 очков)
        add_user_points(user_id, update.effective_user.username, 1000)

        # Отправляем сообщение об успешной оплате
        await update.message.reply_text(
            "✅ Спасибо за оплату! Премиум доступ активирован на 30 дней.\n"
            f"Ваш идентификатор платежа: {payment.provider_payment_charge_id}\n\n"
            "Запускаю парсинг матчей..."
        )

        # Запускаем парсинг матчей сразу после оплаты
        chat_id = update.effective_chat.id
        try:
            # Сообщаем о начале парсинга
            status_message = await context.bot.send_message(
                chat_id=chat_id,
                text="🔄 Начинаю парсинг и анализ матчей..."
            )

            # Запускаем парсинг
            await run_parsing_and_send(chat_id, context)

            # Обновляем статус
            await status_message.edit_text("✅ Парсинг и анализ матчей завершен!")
        except Exception as e:
            logging.error(f"Ошибка при выполнении парсинга после оплаты: {e}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Произошла ошибка при парсинге: {str(e)}"
            )


    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("generate_promo", generate_promo))
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(CommandHandler("menu", start))

    # Импорты для платежей
    from telegram import LabeledPrice
    from telegram.ext import PreCheckoutQueryHandler, MessageHandler, filters

    # Добавляем команду для оплаты
    app.add_handler(CommandHandler("buy", send_invoice))

    # Обработчики для платежей
    app.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(
        MessageHandler(filters.Chat(COMMENTS_CHAT_ID) & filters.TEXT & ~filters.COMMAND, comment_chat_handler))
    print("Telegram-бот запущен. Ожидает команду /start...")
    app.run_polling()


