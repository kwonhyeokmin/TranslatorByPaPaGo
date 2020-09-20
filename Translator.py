import pymysql
import urllib
import json
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from tqdm import tqdm
import datetime
from CustomException import APIUsageExceededError

class Translator(object):

    def __init__(self, info):
        self.client_id = info['client_id']
        self.client_secret = info['client_secret']
        self.url = "https://openapi.naver.com/v1/papago/n2mt"

    def run(self, word):
        request = Request(self.url)
        encText = urllib.parse.quote(word)
        data = "source=en&target=ko&text=" + encText
        request.add_header("X-Naver-Client-Id", self.client_id)
        request.add_header("X-Naver-Client-Secret", self.client_secret)
        response = urlopen(request, data=data.encode("utf-8"))
        rescode = response.getcode()
        if (rescode == 200):
            response_body = response.read()
            response_body = response_body.decode("utf-8")
            result_dict = json.loads(response_body)
            translatedText = result_dict["message"]["result"]["translatedText"]
            return translatedText
        else:
            raise ValueError("Error Code:" + rescode)


if __name__ == "__main__":
    MAXIMUM_WORD_COUNT = 100

    with open('database_properties.json') as f:
        db_info = json.loads(f.read())
    conn = pymysql.connect(**db_info)

    select_sql = """select caption from a_text_bird where caption_kr is null;"""

    # papago api 정보 로드
    user_sql = """
        call getuser({});
    """.format(MAXIMUM_WORD_COUNT)
    infos = list()
    with conn.cursor() as cursor:
        cursor.execute(user_sql)
        rows = cursor.fetchall()
        for user_id, client_id, client_secret, left_count in rows:
            infos.append({
                'user_id': int(user_id),
                'client_id': client_id,
                'client_secret': client_secret,
                'left_count': int(left_count)
            })
    # with open('secret_properties.json') as f:
    #     info = json.loads(f.read())


    for info in infos:
        try:
            translator = Translator(info)
            for _ in tqdm(range(info['left_count'])):
                with conn.cursor() as cursor:
                    cursor.execute(select_sql)
                    row = cursor.fetchone()
                    caption = row[0]
                result = translator.run(caption)
                result = result.replace("'", "\\'")
                # now = datetime.datetime.now()
                # nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')
                with conn.cursor() as cursor:
                    update_sql = """
                    update a_text_bird set caption_kr = '{}', reg_dt = CURRENT_TIMESTAMP, user_id = {} where caption = '{}';
                    """.format(result.replace("'", "\\'").replace('"', '\\"'), info['user_id'], caption.replace("'", "\\'").replace('"', '\\"'))
                    cursor.execute(update_sql)
                conn.commit()
        except HTTPError as e:
            print('user_id: %d' % user_id)
            raise APIUsageExceededError("api 사용량을 초과하였습니다")
        except ValueError as e:
            print(e)
        except APIUsageExceededError as e:
            continue
    conn.close()
