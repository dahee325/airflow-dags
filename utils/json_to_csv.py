from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from hdfs import InsecureClient
import json
import pandas as pd

# 댓글의 감정상태를 수치로 표현하는 함수 => 한글을 숫자로 바꿔주는 함수
def analyze_sentiment(comment):
    analyzer = SentimentIntensityAnalyzer()
    # 분석된 결과
    result = analyzer.polarity_scores(comment)
    return result

# result = analyze_sentiment('i like you')
# print(result)
# 'i hate you' => {'neg': 0.649, 'neu': 0.351, 'pos': 0.0, 'compound': -0.5719}
# 'i like you' => {'neg': 0.0, 'neu': 0.444, 'pos': 0.556, 'compound': 0.3612}

def convert_json_to_csv():
    hdfs_json_path = '/input/yt-data'
    hdfs_csv_path = '/input/yt-data-csv'

    # from hdfs import InsecureClient
    # 하둡에 접속할 수 있는 파이썬 코드
    client = InsecureClient('http://localhost:9870', user='ubuntu')
    
    # hdfs_json_paht에 있는 파일 모두 불러오기
    # hdfs dfs -ls /input/yt-data
    json_files = client.list(hdfs_json_path) # 파일의 이름만 반환

    # import json
    for json_file in json_files:
        # json_file의 경로 => /input/yt-data/yymmddhhmm.json
        json_file_path = f'{hdfs_json_path}/{json_file}'

        # 데이터 읽기
        with client.read(json_file_path) as reader:
            data = json.load(reader)

        csv_data = []

        for video_id, comments in data['all_comments'].items():
            for comment in comments:
                text = comment['text']
                sentiment= analyze_sentiment(text) # 댓글을 숫자로 바꿔주는 함수
                csv_data.append({
                    'video_id': video_id,
                    # 'text': text.strip().replace(',', ' ').,
                    'positive': sentiment['pos'],
                    'negative': sentiment['neg'],
                    'neutral': sentiment['neu'],
                    'compound': sentiment['compound'],
                    'likeCount': comment['likeCount'],
                    'author': comment['author']
                })
        
        # 데이터 프레임으로 바꾸기
        # pip install pandas
        # import pandas as pd
        df = pd.DataFrame(csv_data)

        json_file_name = json_file.split('.')[0]
        csv_file_name = f'{json_file_name}.csv'
        csv_file_path = f'{hdfs_csv_path}/{csv_file_name}'

        # overwrite=True : 만약 코드를 한번 더 실행했을 때 이미 만들어진 파일이 있어도 덮어씌워주는 옵션
        with client.write(csv_file_path, encoding='utf-8', overwrite=True) as writer:
            df.to_csv(writer, index=False, encoding='utf-8')


convert_json_to_csv()