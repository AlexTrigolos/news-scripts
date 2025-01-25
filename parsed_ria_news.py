import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta, date
import re
import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from traceback import format_exc
import json
import traceback
import time
import pytz
import io
import copy

os.environ['AWS_ACCESS_KEY_ID'] = 'YCAJEf4ZndSRFUxbXRXIvY_bf'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'YCN6eB6_2rWeBOgu_EvhtOW_yu1_rIa0K_Fu_Cok'

SOURCE_BUCKET_NAME = 'russian-news'
TARGET_BUCKET_NAME = 'parsed-russian-news'

access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
endpoint_url = 'https://storage.yandexcloud.net'

# Создание клиента S3
s3_client = boto3.client('s3',
                         region_name='ru-central1',
                         aws_access_key_id=access_key,
                         aws_secret_access_key=secret_key,
                         endpoint_url=endpoint_url)

def get_links_for_day(day_folder):
    """Получает список ссылок из файла для указанного дня."""
    try:
        response = s3_client.get_object(Bucket=SOURCE_BUCKET_NAME, Key=f'ria/{day_folder}.pkl')
        return json.loads(response['Body'].read())
    except Exception as e:
        print(f"Ошибка при чтении файла для дня {day_folder}: {e}")
        return []

def target_key(day_folder):
    return f'ria/{str(day_folder)[:4]}/{day_folder}.pkl'

def get_news_for_day(day_folder):
    """Получает список новостей из файла для указанного дня."""
    try:
        response = s3_client.get_object(Bucket=TARGET_BUCKET_NAME, Key=target_key(day_folder))
        return json.loads(response['Body'].read())
    except Exception as e:
        print(f"Ошибка при чтении get_news_for_day новостей для дня {day_folder}: {e}")

def save_to_s3(contents, count, day_folder, links_count):
    """Сохраняет контент в целевой бакет S3."""
    try:
        data = get_news_for_day(day_folder)
        data = data[:count] + contents
        s3_client.put_object(
            Bucket=TARGET_BUCKET_NAME,
            Key=target_key(day_folder),
            Body=json.dumps(data, ensure_ascii=False)
        )
        print(f"Файл {day_folder}.pkl сохранён успешно. (Записано {len(data)}/{links_count} новостей)")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Ошибка с учётными данными для S3:", e)
    except Exception as e:
        print(f"Ошибка при записи save_to_s3 в целевой бакет: {e}")

def exists_data_s3(day_folder):
    """Проверка контент в целевой бакет S3."""
    try:
        response = s3_client.get_object(Bucket=TARGET_BUCKET_NAME, Key=target_key(day_folder))
        return json.loads(response['Body'].read())
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Ошибка с учётными данными для S3:", e)
    except Exception as e:
        if str(e) == 'An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.':
            return []
        print(f"Ошибка при чтении exists_data_s3 в целевой бакет: {e}")

def save_day_to_s3(content, day_folder):
    """Сохраняет контент в целевой бакет S3."""
    try:
        s3_client.put_object(
            Bucket=TARGET_BUCKET_NAME,
            Key=target_key(day_folder),
            Body=json.dumps(content, ensure_ascii=False)
        )
        print(f"{day_folder}.pkl сохранён успешно файл с контентом.")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Ошибка с учётными данными для S3:", e)
    except Exception as e:
        print(f"Ошибка при записи save_day_to_s3 в целевой бакет: {e}")

def save_stange_news(url):
    """Сохраняет нестандартного парсинга страницы в целевой бакет S3."""
    try:
        response = s3_client.get_object(Bucket=TARGET_BUCKET_NAME, Key='ria/strange.pkl')
        data = json.loads(response['Body'].read())
        if url not in data[-50:]:
            data.append(url)
            s3_client.put_object(
                Bucket=TARGET_BUCKET_NAME,
                Key='ria/strange.pkl',
                Body=json.dumps(data)
            )
            print(f"strange.pkl сохранён успешно с новыйм url {url}")
        else:
            print(f"url {url} ранее сохранялся в strange.pkl")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Ошибка с учётными данными для S3:", e)
    except Exception as e:
        print(f"Ошибка при записи save_stange_news в целевой бакет: {e}")

def update_all_strange_news_links(data):
    """Сохраняет нестандартного парсинга страницы в целевой бакет S3."""
    try:
        s3_client.put_object(
            Bucket=TARGET_BUCKET_NAME,
            Key='ria/strange.pkl',
            Body=json.dumps(data)
        )
        print(f"strange.pkl сохранён успешно.")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Ошибка с учётными данными для S3:", e)
    except Exception as e:
        print(f"Ошибка при записи update_all_strange_news_links в целевой бакет: {e}")

def download_strange_news():
    """Получает ссылки с плохими новостями."""
    try:
        response = s3_client.get_object(Bucket=TARGET_BUCKET_NAME, Key='ria/strange.pkl')
        return json.loads(response['Body'].read())
    except Exception as e:
        print(f"Ошибка при чтении download_strange_news ссылок новостей: {e}")

def clean_text(text):
    # Удаляем лишние пробелы и переводы строк
    return ' '.join(text.split()).strip()

def append_data(lst, element):
    # Удаляем теги p, div, h1, h2, h3, h4, h5, h6, span
    data = copy.deepcopy(element)
    for tag in data(['a', 'p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'span']):
        tag.replaceWithChildren()
    text = clean_text(data.get_text())
    if len(text) != 0:
        lst.append(clean_text(text))

def parse_page(url, count = 0):
    # Получаем содержимое страницы
    import time

    response = requests.get(url)
    while response.status_code == 429:
        print(f"По {url} получили {response.status_code} ждем 10 секунд")
        time.sleep(10)
        response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')

    # Извлечение даты
    time_element = soup.find('div', class_='article__info-date')
    news_time = clean_text(time_element.find('a').get_text()[:5]) if time_element else None

    # Извлечение заголовков
    titles = []
    for title in ['article__title', 'white-longread__header-title', 'tag-biography__title', 't-title']:
        elements = soup.find_all(class_=title)
        for element in elements:
            append_data(titles, element)

    second_titles = []
    for second_title in ['article__second-title', 'white-longread__header-subtitle', 'tag-biography__subtitle']:
        elements = soup.find_all(class_=second_title)
        for element in elements:
            append_data(second_titles, element)

    # Извлечение текста
    texts = []
    for text in ['article__text', 'white-longread__text-body', 'online__item-time', 'online__item-text', 'article__photo-item-text', 't-descr', 't-text']:
        elements = soup.find_all(class_=text)
        for element in elements:
            append_data(texts, element)

    quote_texts = []
    for quote in ['article__quote-text', 'white-longread__quote-text']:
        elements = soup.find_all(class_=quote)
        for element in elements:
            append_data(quote_texts, element)

    announce_texts = []
    elements = soup.find_all(class_='article__announce-text')
    for element in elements:
        append_data(announce_texts, element)

    # Структурированные данные
    page_data = {
        'time': news_time,
        'titles': ' '.join(titles),
        'second_titles': ' '.join(second_titles),
        'texts': ' '.join(texts),
        'quote_texts': ' '.join(quote_texts),
        'announce_texts': ' '.join(announce_texts)
    }

    if len(titles) == 0 or len(texts) == 0:
        if count < 2:
            print('Данные считаются не валидными спим 5 секунд и пробуем повторить.')
            time.sleep(5)
            return parse_page(url, count + 1)
        else:
            save_stange_news(url)

    return page_data

"""Код для получения новостей из ссылок, сохраняет батчами 50 штук, и не обновляет уже сохраненные данные (дни и новости конкретного дня кратно 50)."""

def process_day(day_folder):
    """Обрабатывает все ссылки за указанный день."""
    links = get_links_for_day(day_folder)
    print(links)
    exsisted = exists_data_s3(day_folder)
    print(f'Найдено {len(exsisted)} сохраненных новостей из {len(links)}')
    if len(exsisted) == 0:
        save_day_to_s3([], day_folder)
        contents = list()
        count = 0
    else:
        save_count = len(exsisted) // 50
        count = save_count * 50
        contents = exsisted[count:]
    for index in range(count + len(contents), len(links)):
        print(f'{index + 1}/{len(links)}', links[index])
        content = parse_page(links[index])
        # print(content)
        contents.append(content)
        if len(contents) == 50 or len(links) == count + len(contents):
            save_to_s3(contents, count, day_folder, len(links))
            contents = list()
            count += 50

# date = datetime.now(pytz.timezone('Europe/Moscow')).date()
date = datetime.strptime('2022-03-03', '%Y-%m-%d').date()
delta = timedelta(days=1)
start_date = datetime.strptime('2001-10-16', '%Y-%m-%d').date()
# start_date = datetime.strptime('2024-05-17', '%Y-%m-%d').date()

while (date >= start_date):
    print(f"Обработка дня: {date}")
    process_day(date)
    date -= delta
