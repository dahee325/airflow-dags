# pip install google-api-python-client
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
from pprint import pprint # 더 보기좋게 출력해줌
from hdfs import InsecureClient
from datetime import datetime
import json

load_dotenv('/home/ubuntu/airflow/.env')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
# print(youtube)


# handle을 가져와서 channelId로 바꾼 후 채널 검색

# 1. handle을 기준으로 channelId 리턴하는 함수 => channel()
def get_channel_id(youtube, handle):
    # list(part='') : 보고싶은 컬럼에 대한 정보, 필수 옵션
    response = youtube.channels().list(part='id', forHandle=handle).execute()
    # print(response)
    return response['items'][0]['id']

# 유사도로 가장 높은 handle 출력
target_handle = 'coldplay'
channel_id = get_channel_id(youtube, target_handle)
# print(channel_id)

# 2. channel_id를 기준으로 최신영상의 videoId들을 리턴하는 함수 => search()
def get_latest_video_ids(youtube, channel_id):
    # snippet : 
    response = youtube.search().list(
        part='snippet',
        channelId=channel_id,
        maxResults=5,
        order='date',
    ).execute()

    video_ids = []

    for item in response['items']:
        video_ids.append(item['id']['videoId'])
    
    return video_ids

latest_video_ids = get_latest_video_ids(youtube, channel_id)
# print(latest_video_ids)

# 3. video_id를 기준으로 comment를 리턴하는 함수 => commentThreads()
# 하나의 video_id를 기준으로 comment를 뽑는 함수를 먼저 만들고 반복문을 돌림
def get_comments(youtube, video_id):
    # maxResults=5 : 댓글 5개만 출력
    # order='relevance', # 좋아요가 많은 개수의 댓글 출력
    response = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        textFormat='plainText',
        maxResults=100,
        order='relevance', # 좋아요가 많은 개수의 댓글 출력
    ).execute()
    # pprint(response)

    comments = []

    for item in response['items']:
        # author : 사용자 이름, text : 댓글내용, publishedAt : 작성 시간, likeCount : 좋아요 수, commentID : 댓글 id
        comment = {
            'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            'text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
            'publishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt'],
            'likeCount': item['snippet']['topLevelComment']['snippet']['likeCount'],
            'commentId': item['snippet']['topLevelComment']['id'],
        }
        comments.append(comment)
    return comments


# for video_id in latest_video_ids:
#     result = get_comments(youtube, video_id)
#     print(result)

# 4. 위에 구현한 함수를 실행하여 최종 형태 리턴
def get_handle_to_comments(youtube, handle):
    channel_id = get_channel_id(youtube, handle)
    latest_video_ids = get_latest_video_ids(youtube, channel_id)

    all_comments = {}

    for video_id in latest_video_ids:
        comments = get_comments(youtube, video_id)
        all_comments[video_id] = comments
        # break # 반복문을 많이 돌리면 리소스가 빨리 사라지므로 반복문 한번만 실행

    return {
        'handle': handle,
        'all_comments': all_comments
    }

# get_handle_to_comments(youtube, target_handle)


# hdfs에 업로드
# pip install hdfs
def save_to_hdfs(data, path):
    # from hdfs import InsecureClient

    # InsecureClient('저장 경로', 사용자)
    client = InsecureClient('http://localhost:9870', user='ubuntu')

    # from datetime import datetime
    # 현재 시간을 원하는 시간형태로 만들어줌 -> ex. 2504241142
    current_date = datetime.now().strftime('%y%m%d%H%M')
    file_name = f'{current_date}.json'

    # '/input/yt-date + 2504241144.json
    hdfs_path = f'{path}/{file_name}'

    # import json
    # 딕셔너리를 json으로 바꾸는 함수
    # ensure_ascii=False : 이모티콘 제거
    json_data = json.dumps(data, ensure_ascii=False)

    with client.write(hdfs_path, encoding='utf-8') as writer:
        writer.write(json_data)

# data = get_handle_to_comments(youtube, target_handle)

# # save_to_hdfs(저장할 파일, 경로)
# # 하둡에 파일 생성 : hdfs dfs -mkdir /input/yt-data
# save_to_hdfs(data, '/input/yt-data')