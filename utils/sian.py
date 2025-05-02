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


# 최종시안 채널 id 뽑기
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

# target_handles = ['@channelA-world', '@play-channelA']
target_handles = ['@SIAN_youseeany']
channel_ids = get_channel_id(youtube, target_handles)
# print(channel_ids)


# 솔로지옥 비하인드 영상 video_id 뽑기
def get_video_ids(youtube, channel_ids, query):
    all_items = []
    video_id = []

    for channel_id in channel_ids:
        response = youtube.search().list(
            q=query,
            order='relevance',
            part='snippet',
            channelId=channel_id,
            maxResults= 1,
            type='video'
        ).execute()


        for item in response.get('items', []):
            # video_info = {
            #     'video': item['id']['videoId'],
            #     'title': item['snippet']['title'],
            #     'channelTitle': item['snippet']['channelTitle'],
            #     'publishedAt' : item['snippet']['publishedAt'], 
            # }
            # video_info_list.append(video_info)
            video_id.append(item['id']['videoId'])
    # return video_info_list
    return video_id

video_id = get_video_ids(youtube, channel_ids, query='솔로지옥')
# pprint(video_id)


# 영상의 댓글 뽑아오기
def get_comments(youtube, video_id):
    response = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        textFormat='plainText',
        maxResults=1000,
        order='relevance', # 좋아요가 많은 개수의 댓글 출력
    ).execute()
    # pprint(response)

    comments = []

    for item in response['items']:
        # author : 사용자 이름, text : 댓글내용, publishedAt : 작성 시간, likeCount : 좋아요 수, commentID : 댓글 id
        comment = {
            'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            'text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
            'likeCount': item['snippet']['topLevelComment']['snippet']['likeCount'],
            'commentId': item['snippet']['topLevelComment']['id'],
        }
        comments.append(comment)
    return comments

result = get_comments(youtube, video_id[0])
print(result)

# 뽑아온 댓글 1000개 파일로 저장
def save_to_file(response, save_dir='/home/ubuntu/damf2/data/channelA/', filename_prefix='comments'):
    timestamp = datetime.now().strftime('%Y%m%d%H%M')
    filename = f'{filename_prefix}_{timestamp}.json'

    file_path = os.path.join(save_dir, filename)

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(response, file, ensure_ascii=False, indent=2)
    
    print(f'저장완료: {filename}')

save_to_file(result)