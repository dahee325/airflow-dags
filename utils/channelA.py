# pip install google-api-python-client

from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
from pprint import pprint # 더 보기좋게 출력해줌
from hdfs import InsecureClient
from datetime import datetime
import json

load_dotenv('/home/ubuntu/airflow/.env')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY_2')

youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)


# 채널 id 뽑기
def get_channel_id(youtube, handles):
    result = []
    for handle in handles:
        response = youtube.channels().list(
            part='id', 
            forHandle=handle
        ).execute()
        result.append(response['items'][0]['id'])
    # pprint(result)
    return result

target_handles = ['@channelA-world', '@play-channelA']
channel_ids = get_channel_id(youtube, target_handles)
# print(channel_ids)


# video_ids 뽑기
def get_video_ids(youtube, channel_ids, query):
    all_items = []
    video_info_list = []

    for channel_id in channel_ids:
        response = youtube.search().list(
            q=query,
            order='relevance',
            part='snippet',
            channelId=channel_id,
            maxResults= 5,
            type='video'
        ).execute()
        # pprint(response)

        # items = response.get('items', [])
        # all_items.extend(items)


        for item in response.get('items', []):
            video_info = {
                'video': item['id']['videoId'],
                'title': item['snippet']['title'],
                'channelTitle': item['snippet']['channelTitle'],
                'publishedAt' : item['snippet']['publishedAt'], 
            }
            video_info_list.append(video_info)
    return video_info_list

# video_infos = get_video_ids(youtube, channel_ids, query='강철지구')

# video_id 파일로 저장
def save_to_file(response, save_dir='/home/ubuntu/damf2/data/channelA/', filename_prefix='search'):
    timestamp = datetime.now().strftime('%Y%m%d%H%M')
    filename = f'{filename_prefix}_{timestamp}.json'

    file_path = os.path.join(save_dir, filename)

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(response, file, ensure_ascii=False, indent=2)
    
    print(f'저장완료: {filename}')

# save_to_file(video_infos)

# videl_id 로 조회수와 좋아요 수
def get_video_statistics(video_ids):
    video_stats = []

    for video_id in video_ids:
        response = youtube.videos().list(
            part='statistics',
            id=video_id
        ).execute()

        for item in response.get('items', []):
            video_info = {
                'videoId': item['id'],
                'viewCount': item['statistics'].get('viewCount', 0),
                'likeCount': item['statistics'].get('likeCount', 0),
            }
            video_stats.append(video_info)

    return video_stats


# 파일에서 비디오 정보를 읽고, 조회수와 좋아요 수 가져오기
def get_video_stats_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        video_infos = json.load(file)

    video_ids = [video['video'] for video in video_infos]
    video_stats = get_video_statistics(video_ids)

    return video_stats

file_path = '/home/ubuntu/damf2/data/channelA/search_202504251459.json'  # 저장된 파일 경로
video_stats = get_video_stats_from_file(file_path)

def save_count_to_file(video_stats):
    count = []
    for stat in video_stats:
        result = f"Video ID: {stat['videoId']}, Views: {stat['viewCount']}, Likes: {stat['likeCount']}"
        count.append(result)

    save_to_file(count, filename_prefix='count')

save_count_to_file(video_stats)