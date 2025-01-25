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
os.environ['API_KEY'] = 'cc6f3b3b-7b99-476f-a716-51887f5eec6d'

SOURCE_BUCKET_NAME = 'parsed-russian-news'
TARGET_BUCKET_NAME = 'rated-russian-news'

SECID_BUCKET = 'russian-stocks-quotes'

access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
endpoint_url = 'https://storage.yandexcloud.net'

# Создание клиента S3
s3_client = boto3.client('s3',
                         region_name='ru-central1',
                         aws_access_key_id=access_key,
                         aws_secret_access_key=secret_key,
                         endpoint_url=endpoint_url)

def get_news_for_day(day_folder):
    """Получает список ссылок из файла для указанного дня."""
    try:
        response = s3_client.get_object(Bucket=SOURCE_BUCKET_NAME, Key=target_key(day_folder))
        return json.loads(response['Body'].read())
    except Exception as e:
        print(f"Ошибка при чтении файла для дня {day_folder}: {e}")
        return []

def target_key(day_folder):
    return f'ria/{str(day_folder)[:4]}/{day_folder}.pkl'

def get_rates_for_day(day_folder):
    """Получает список новостей из файла для указанного дня."""
    try:
        response = s3_client.get_object(Bucket=TARGET_BUCKET_NAME, Key=target_key(day_folder))
        return json.loads(response['Body'].read())
    except Exception as e:
        print(f"Ошибка при чтении get_rates_for_day оценок новостей для дня {day_folder}: {e}")

def save_to_s3(contents, diff_count, day_folder, news_count):
    """Сохраняет контент в целевой бакет S3."""
    try:
        data = get_rates_for_day(day_folder)
        data_keys = data.keys()
        for secid in contents.keys():
            if secid == 'count':
                continue
            if secid not in data_keys:
                data[secid] = contents[secid]
            else:
                if data[secid]['sector'] is None:
                    data[secid]['sector'] = contents[secid]['sector']
                data[secid]['assessments'].extend(contents[secid]['assessments'])
        data['count'] += diff_count
        s3_client.put_object(
            Bucket=TARGET_BUCKET_NAME,
            Key=target_key(day_folder),
            Body=json.dumps(data, ensure_ascii=False)
        )
        print(f"Файл {day_folder}.pkl сохранён успешно. (Записано {data['count']}/{news_count} оценок новостей)")
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
            return {"count": 0}
        print(f"Ошибка при чтении exists_data_s3 в целевом бакете: {e}")

def save_start_day_to_s3(content, day_folder):
    """Сохраняет контент в целевой бакет S3."""
    try:
        s3_client.put_object(
            Bucket=TARGET_BUCKET_NAME,
            Key=target_key(day_folder),
            Body=json.dumps(content)
        )
        print(f"{day_folder}.pkl сохранён успешно пустой файл.")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Ошибка с учётными данными для S3:", e)
    except Exception as e:
        print(f"Ошибка при записи save_start_day_to_s3 в целевой бакет: {e}")

def download_secid_names(dir):
    key = f'{dir}secid_names.pkl'
    return json.loads(download_object_from_s3(key))

def download_object_from_s3(key):
    response = s3_client.get_object(Bucket=SECID_BUCKET, Key=key)
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print(f"Успешно получен из {SECID_BUCKET}/{key}")
    else:
        print(f"Ошибка при получении: {response['ResponseMetadata']['HTTPStatusCode']}")
    return response['Body'].read()

import os
import openai

client = openai.OpenAI(
    api_key=os.getenv('API_KEY'),
    base_url="https://api.sambanova.ai/v1",
)

secids = download_secid_names('preprocessed_data/')

def request_llm_api(content):
    return client.chat.completions.create(
        model='Meta-Llama-3.3-70B-Instruct',
        messages=[
            {"role":"system","content":"Ты опытный инвестиционный аналитик с стажем 30 лет и хорошо разбираешься в новостях и их влиянии на российские компании, и то как сами люди на них реагируют. Можешь очень точно определить на будущее каких компаний может повлиять новость и понять насколько положительно или отрицательно будет влияние каждой новости."},
            {"role":"user","content": content}
            ],
        temperature =  0.1,
        top_p = 0.1
    )

def rate_news(news, year, month, day, count = 0):
    news['texts'] = news['texts'][:4000 - len(str(news)) + len(str(news['texts']))]
    content = f'Определи для каких российских компаний важна новость. Тикеры российских компаний в списке далее {secids}. Если считаешь, что есть те компании, для которых новость важна, но их нет в списке, то добавляй согласно формату ответа. Считай, что сейчас {year} год {month} месяц и {day} число. Ты узнаешь новость которая включает в себя данные вида "ключ - значение" с следующими ключами time (время в которое вышла новость, где сначала идут две цифры часа, потом двоеточие и две цифрмы минуты), titles (основные заголовки новости), second_titles (заголовки второго уровня), texts (основные текста новости), quote_texts (цитаты людей из новости), announce_texts (анонсы из новости). Дальше приведена сама новсть {news}. Верни результат в виде массива, где каждый элемент это питоновский словарь (ключ - значение), где ключами будут secid, importance, sector, reason, а их значения соответственно тикер компании, важность новости, сектор компании, и кратко причина почему такую оценку новости дала для этой компании, но чтоб это касалось именно этой компании а не у всех было примерно одинаковое описание. Важность должна быть в диапозоне от -1 до 1, где -1 - это черезвычайно плохая новость для компании (например новость такая, что компания скоро перестанет существовать), а 1 - это невероятно хорошая новость для компании (например, компания станет наголову выше всех своих конкурентов или из-за описанного в новости у неё конкурентов вообще не будет). Так же не забывай, что влияние новостей одной компании может повлиять на конкурентов или союзников (а может и на весь сектор или смежные сектора) и это влияние тоже может быть положительным или отрицательным. Если ни для какой компании не нашел влияния, то отдавай пустой массив. А так же помни, что здесь идет оценка именно российских компаний, поэтому учитывай поведение в большей степери привычную для русских людей. Если новость никак не влияет на российские компании, то не надо искать хоть какие-то связи. Точность для оценки сделай до 3 знака после запятой. И не надо добавлять в ответ те компании, для которых влияние меньше чем 0.001 в абсолютном значении. Твоим ответом должен быть виде массива словарей питона без какого-либо лишнего текста.'
    try:
        response = request_llm_api(content)
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"Ошибка RateLimitError: {e}")
        if str(e)[-22:-3] == "rate_limit_exceeded":
            time.sleep(5)
            return rate_news(news, year, month, day, count)
        elif count < 4:
            time.sleep(1)
            return rate_news(news, year, month, day, count + 1)
        else:
            return []

def rate_day(day_folder):
    news = get_news_for_day(day_folder)
    exsisted = exists_data_s3(day_folder)
    print(f'Найдено {exsisted["count"]} оцененных новостей из {len(news)}')
    if exsisted["count"] == 0:
        save_start_day_to_s3({"count": 0}, day_folder)
        contents = dict()
        count = 0
    else:
        count = exsisted["count"]
        contents = dict()
    diff_count = 0
    year, month, day = str(day_folder).split('-')
    for index in range(count, len(news)):
        print(f'{index + 1}/{len(news)}', {key: news[index][key] for key in news[index].keys() if key == 'time' or key == 'titles'})
        rates = rate_news(news[index], year, month, day)
        print([{key: rate[key] for key in rate.keys() if key == 'secid' or key == 'importance'} for rate in rates])
        for rate in rates:
            if 'importance' not in rate.keys() or 'reason' not in rate.keys() or 'sector' not in rate.keys() or 'secid' not in rate.keys():
                continue
            if rate['secid'] not in contents.keys():
                contents[rate['secid']] = {'sector': rate['sector'], 'assessments': []}
            elif contents[rate['secid']]['sector'] is None:
                contents[rate['secid']]['sector'] = rate['sector']
            contents[rate['secid']]['assessments'].append({"time": news[index]['time'], "importance": rate['importance'], "reason": rate['reason'], "promt_version": 1})
        diff_count += 1
        if diff_count == 50 or len(news) == count + diff_count:
            save_to_s3(contents, diff_count, day_folder, len(news))
            contents = dict()
            count += diff_count
            diff_count = 0

# date = datetime.now(pytz.timezone('Europe/Moscow')).date()
date = datetime.strptime('2024-06-20', '%Y-%m-%d').date()
delta = timedelta(days=1)
start_date = datetime.strptime('2001-10-16', '%Y-%m-%d').date()

while (date >= start_date):
    print(f"Оценка дня: {date}")
    rate_day(date)
    date -= delta
