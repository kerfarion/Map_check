import schedule
import time
import logging
import os.path

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import requests
import json

from Generator_links import Generate_links

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# url для отправки put запроса
url_to_put = "https://example.com/api/places/update"


# Задаю время промежутка между запросами
time_between = 5

# Время в формате часы:минуты
time_reroll = "08:00"

# Cписок ссылок преобразовываю в json файл и сохраняю его
Generate_links("data/res.csv")

# Cчитываю список ссылок из файла
with open("links.json", "r") as f:
    list_links = json.load(f)

# Заголовки для парсинга
headers = {
    "Accept": "*/*",
    "User-Agent": UserAgent().chrome,
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Список для уже закрытых ссылок, чтобы не отправлять put запрос лишние разы
list_closed_before = []

# Если есть файл с сохранённой информациеё о закрытых заведениях,
# то загрузить id заведений в переменную уже закрытых заведений
# ( для избегания лишних запросов к серверу )
if os.path.exists("save_status.json"):
    list_closed_before = json.load(f)


def check_map(list_links):    # Даю функции доступ к списку
    global list_closed_before
    global headers
    count = 0

    # Каждое выполнение функции создаётся новый пустой список для записи ссылок на закрытые предприятия
    closed_links = []
    for link in list_links:
        flag = False
        count += 1
        try:
            # Формирую get запрос к api карт с таймаутом 15 секунд
            req = requests.get(link["url"], headers=headers, timeout=15)

            src = req.text
            id = link["id"]
            # Создаю объект класса Beautifulsoup, для парсинга данных со страницы
            soup = BeautifulSoup(src, "lxml")

            # Ищу класс в котором хранится информация о том, что предприятие больше не работает
            r = soup.find(class_="business-working-status-view _closed _no-data")
            captcha_error = soup.find(class_="CheckboxCaptcha CheckboxCaptcha_first_letter_highlighted")
            ne_ta_stranitsa_error = soup.find(class_="business-card-view__main-wrapper")

            if captcha_error:
                logger.warning("Попал на капчу") # Вывожу сообщение о том, что попал на капчу яндекса
                return 1

            # Если переменная не пустая, значит была выдана страница в которой был необходимый класс
            if r:
                closed_links.append(id)

            if ne_ta_stranitsa_error:
                if r:
                    logger.info(f"{count}. Запрос к {link['url']} мёртв")

                else:
                    logger.info(f"{count}. Запрос к {link['url']} жив")

            else:
                logger.warning(f"при запросе к {link['url']} открыта не та страница")
                closed_links.append({id})


        except requests.exceptions.Timeout: # Если превышено время ожидания вывожу сообщение об ошибке
            logger.error(f"Превышено время ожидания ответа от {link['url']}")
            continue
        except requests.exceptions.RequestException as e: # Если при запросе к картам появятся другие ошибки, обрабатываю их здесь
            logger.error(f"Ошибка при запросе к {link['url']}: {e}")
            continue
        except Exception as e:  # Все остальные ошибки
            logger.exception(f"Непредвиденная ошибка при обработке {link['url']}: {e}")
            continue



        # Делаю перерыв, чтобы не создавать слишком много запросов
        time.sleep(time_between)

    do_put_request(closed_links)


# Отправляю запрос на сервер
def do_put_request(closed_links):
    global list_closed_before
    # Если новый список отличается от старого, тоесть закрылось новое заведение, то отправить put запрос к сайту
    # И обновить файл с существующими ссылками
    if closed_links != list_closed_before:
        with open("save_status.json", "w") as file:
            json.dump(closed_links, file, indent=4)

        list_closed_before = closed_links
        json_data = json.dumps(closed_links)
        try:
            response = requests.put(url_to_put, data=json_data, headers={'Content-type': 'application/json'},
                                    timeout=15)  # Если вдруг будет проблема с доступом к серверу для пут запроса, тоже буду обрабатывать как ошибку
            logger.info("Запрос успешно отправлен")
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при отправке PUT запроса: {e}")
        return 0

    else:
        logger.info("Новых заведений не закрылось")
        return 0


# Запускаю функцию каждые два часа
schedule.every().day.at(time_reroll).do(check_map, list_links)
check_map(list_links)

while True:
    schedule.run_pending()
    time.sleep(1)