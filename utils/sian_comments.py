from collections import Counter
import re
from konlpy.tag import Okt
import json

# 1000개의 댓글이 저장된 json파일 불러오기
def load_comments_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        comments = json.load(f)
    return comments

file_path = '/home/ubuntu/damf2/data/channelA/comments_202505021554.json'
comments = load_comments_from_file(file_path)

# 가장 많이 나온 단어 뽑기
# 한국어 추출하는 함수
def extract_korean(text):
    return re.sub(r'[^가-힣\s]', '', text)

def get_most_common_words(comments, top_n=10):
    okt = Okt()
    all_text = ' '.join([c['text'] for c in comments])

    # 한글만 남기고 나머지 문자 제거
    all_text = extract_korean(all_text)

    words_pos = okt.pos(all_text)
    # 동사와 명사 모두 출력
    # words = [word for word, pos in words_pos if pos in ['Noun', 'Verb', 'Adjective']]
    words = [word for word, pos in words_pos if len(word) > 1]

    stopwords = {'시안', '진짜', '너무', '정말',
                '영상', '언니', '솔로', '솔지',
                '지옥', '최고', '그냥', '그',
                '지금', '사람', '최고', '해요',
                '제일', '..', '...', 'the',
                'you', '!!', 'in'}
    words = [w for w in words if w not in stopwords and len(w) > 1]

    korean_comments_count = sum(1 for c in comments if extract_korean(c['text']))
    print(korean_comments_count)
    
    counter = Counter(words)
    return counter.most_common(top_n)

common_words = get_most_common_words(comments, top_n=20)
for word, freq in common_words:
    print(f'{word}: {freq}회')

# 가장 많이 사용된 단어들을 포함하는 댓글만 필터링
