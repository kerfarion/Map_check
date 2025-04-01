import schedule
import time
import logging

from bs4 import BeautifulSoup
import requests
import json

from Generator_links import Generate_links

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# url для отправки put запроса
url_to_put = "https://example.com/api/places/update"


# Cписок ссылок преобразовываю в json файл и сохраняю его
Generate_links("data/res.csv")

# Cчитываю список ссылок из файла
with open("links.json", "r") as f:
    list_links = json.load(f)

# Заголовки для парсинга
headers = {
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Список для уже закрытых ссылок, чтобы не отправлять put запрос лишние разы
list_closed_before = []


def check_map(list_links):    # Даю функции доступ к списку
    global list_closed_before
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

            if captcha_error != None:
                logger.warning("Попал на капчу") # Вывожу сообщение о том, что попал на капчу яндекса
                return 1

            # Если переменная не пустая, значит была выдана страница в которой был необходимый класс
            if r != None:
                closed_links.append(id)
                flag = True

        except requests.exceptions.Timeout: # Если превышено время ожидания вывожу сообщение об ошибке
            logger.error(f"Превышено время ожидания ответа от {link['url']}")
            continue
        except requests.exceptions.RequestException as e: # Если при запросе к картам появятся другие ошибки, обрабатываю их здесь
            logger.error(f"Ошибка при запросе к {link['url']}: {e}")
            continue
        except Exception as e:  # Все остальные ошибки
            logger.exception(f"Непредвиденная ошибка при обработке {link['url']}: {e}")
            continue


        if flag:
            logger.info(f"{count}. Запрос к {link['url']} мёртв")
        else:
            logger.info(f"{count}. Запрос к {link['url']} жив")

        # Делаю перерыв на 0.1 секунду, чтобы не создавать слишком много запросов
        time.sleep(0.1)

    # Если новый список отличается от старого, тоесть закрылось новое заведение, то отправить put запрос к сайту
    if closed_links != list_closed_before:
        list_closed_before = closed_links
        json_data = json.dumps(closed_links)
        try:
            response = requests.put(url_to_put, data=json_data, headers={'Content-type': 'application/json'}, timeout=15) # Если вдруг будет проблема с доступом к серверу для пут запроса, тоже буду обрабатывать как ошибку
            logger.info("Запрос успешно отправлен")
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при отправке PUT запроса: {e}")
        return 0

    else:
        logger.info("Новых заведений не закрылось")
        return 0

# # Раскоментировать, если нужно протестировать
# for _ in range(100):
#     check_map(list_links)


# Запускаю функцию каждые два часа
schedule.every(2).hours.do(check_map, list_links)
check_map(list_links)

while True:
    schedule.run_pending()
    time.sleep(1)