import json
import boto3
import os
import requests
import time

from datetime import datetime


def to_s3(file, file_name):
    s3 = boto3.client('s3')
    json_str = json.dumps(file)

    s3.put_object(
        Bucket="youtube.teste",
        Key=f"videos/files/{file_name}",
        Body=json_str,
    )


def get_video_por_id(videos, chave):
    api_key = os.environ.get('key_youtube')
    url_base_id = os.environ.get("url_id")

    temp = []
    video_json = dict()
    for video in videos:
        id_ = video['id']['videoId']
        url = f"{url_base_id}part=topicDetails%2Csnippet%2CcontentDetails%2Cstatistics&id={id_}&key={api_key}".replace(
            "'", "")
        video_temp = get_videos(url)
        temp.append(video_temp)
        # video_json[video_temp["items"][0]["snippet"]["title"]] = video_temp
        time.sleep(2)
    return to_json(temp, chave)


def get_videos(url):
    result = requests.get(url)
    return result.json()


def get_video_por_keyword(chave, max_result=5):
    api_key = os.environ.get('key_youtube')
    url_base_busca = os.environ.get("url_busca")

    url = f"{url_base_busca}part=snippet&maxResults={max_result}&q={chave}&key={api_key}".replace("'", "")
    return get_video_por_id(get_videos(url)["items"], chave)


def to_json(result, curso):
    now = datetime.now()
    day = now.day
    hour = now.hour
    minutes = now.minute

    name_file = rf"{curso}-{day}-{hour}-{minutes}.json"
    to_s3(result, name_file)
    return name_file


def lambda_handler(event, context):
    kw = "databricks unity catalog"
    file_path = get_video_por_keyword(kw, 50)
    return {
        'statusCode': 200,
        'body': json.dumps(f"Arquivo salvo em: '{file_path}'.")
    }
