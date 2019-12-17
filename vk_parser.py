import vk
import re
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
import psycopg2
from emoji import UNICODE_EMOJI

def collect_posts(sess, query, start):
    posts = []
    res = sess.newsfeed.search(q=query, v=5.101, start_from=start)

    posts = [item['text'] for item in res['items'] if item['post_type'] == 'post']
    next_from = res['next_from'] if 'next_from' in res.keys() else -1

    return next_from, posts

def stem_posts(posts):
    result = []
    stemmer = SnowballStemmer("russian")
    tokenizer = TweetTokenizer()
    to_remove = ['.', ',', '"']
    
    for post in posts:
        stemmed = []
                #TODO fix links detection and keep emojis in the result
        post = re.sub(r'(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})',
        '',
        post)
        post = re.sub(r"""#[a-zA-ZА-ЯЁ0-9]+""", '', post, flags=re.U)
        post = re.sub(r"""club[0-9]*""", '', post)
        post = re.sub(r"""id[0-9a-zA-Z]*""", '', post)
    
        #removing links, groups references and hashtags
        post = re.sub(r"""[^\u00a9\u00ae\u2000-\u3300\ud83c\ud000-\udfff\ud83d\ud000-\udfff\ud83e\ud000-\udfffА-ЯЁa-zA-Z0-9!?., ]*""",
        '', post, flags=re.U)
        #post = re.sub(r'#[a-zA-Z0-9]*|club[0-9]*|[-.,:;()"=*/\_{}]*|@[a-zA-Z0-9]*', '', post)
        tokens = tokenizer.tokenize(post)

        for word in tokens:
            stemmed_word = stemmer.stem(word)

            if stemmed_word not in stopwords.words('russian')\
            and stemmed_word not in to_remove\
            and len(stemmed_word) > 1\
            or stemmed_word in UNICODE_EMOJI:
                stemmed.append(stemmed_word)
        
        if len(stemmed) > 1:
            result.append(stemmed)

    return result

def send_to_database(words, cursor):
    words_table = 'words'
    emoji_table = 'emojis'
    words_emojis_table = 'words_emojis'
    column = 'value'

    sql_exists = """SELECT EXISTS
                        (SELECT 1 FROM {0} WHERE {1} = (%s));"""

    sql_increment = """UPDATE {}
            SET count = count + 1 WHERE value = (%s)
            returning count;"""

    sql_add = """INSERT INTO {}({}) VALUES (%s)
                returning id;"""    
    
    sql_select_id = """SELECT id
            FROM {}
            WHERE value = (%s);"""

    sql_link_record_exists = """SELECT we.id 
                        FROM "words_emojis" we
                            JOIN words ON we.words_id = words.id
                            JOIN emojis ON we.emojis_id = emojis.id
                        WHERE words.value = (%s)
                            AND emojis.value = (%s);"""
    
    sql_create_link_record = """INSERT INTO words_emojis(words_id, emojis_id, count)
                            SELECT words.id, emojis.id, 1
                            FROM words, emojis
                            WHERE words.value = (%s)
                            AND
                            emojis.value = (%s)

                            returning id;"""

    sql_update_reference_count = """update words_emojis
            set count = count + 1 where
            id = (%s)
            returning count;"""

    words_only = []
    for word in words:

        is_emoji = word in UNICODE_EMOJI
        table = emoji_table if is_emoji else words_table
        
        if not is_emoji:
            if word.isdigit():
                continue

            if not word.isalpha():
                continue

            words_only.append(word)

        try:
            #update independent counts
            cursor.execute(sql_exists.format(
            table,
            column),
            (word,))

            exists = cursor.fetchone()[0]

            if exists:
                cursor.execute(sql_increment.format(
                table),
                (word,))

                increment = cursor.fetchone()[0]
            else:
                cursor.execute(sql_add.format(
                table,
                column),
                (word, ))

                add = cursor.fetchone()[0]

            #update linked counts
            if is_emoji:
                for linked_word in words_only:
                    cursor.execute(sql_link_record_exists,
                        (linked_word, word,))

                    found = cursor.fetchone()

                    if found == None:
                        cursor.execute(sql_create_link_record,
                            (linked_word, word,))

                        record_id = cursor.fetchone()[0]
                    else:
                        cursor.execute(sql_update_reference_count,
                            (found[0],))
                        reference_count = cursor.fetchone()[0]

        except (Exception, psycopg2.DatabaseError) as error:
                    print(error)

     

token =  "" # Сервисный ключ доступа
session = vk.Session(access_token=token)  # Авторизация
vk_api = vk.API(session)
next_from = 0
result = []


#for emoji in UNICODE_EMOJI:
while next_from != -1:
    next_from, posts = collect_posts(vk_api, '😄', start=next_from)
    result += posts

stemmed_posts = stem_posts(result)

conn = psycopg2.connect(dbname='emoji_database',
        user='postgres', 
        password='',
        host='emoji.cqppiab1dnlz.eu-central-1.rds.amazonaws.com')

cursor = conn.cursor()

for post in stemmed_posts:
    send_to_database(post, cursor)
    conn.commit()

cursor.close()
conn.close()
