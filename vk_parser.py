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
    
    for post in posts:
        stemmed = []
                #TODO fix links detection and keep emojis in the result
        post = re.sub(r'(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]\
        {2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))\
        [a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})',
        '',
        post)
        #removing links, groups references and hashtags
        post = re.sub(r'#[a-zA-Z0-9]*|club[0-9]*|[-.,:?;()]*', '', post)
        print(post)
        tokens = tokenizer.tokenize(post)

        for word in tokens:
            stemmed_word = stemmer.stem(word)

            if stemmed_word not in stopwords.words('russian'):
                stemmed.append(stemmed_word)

        result.append(stemmed)

    return result

def send_to_database(words, cursor, table, column):
    words_table = 'words'
    emoji_table = 'emojis'
    words_emojis_table = 'words_emojis'

    sql_exists = 'select exists (select 1 from {0} where {1} = (%s));'

    sql_increment = """update {}
            set count = count + 1 where value = (%s)
            returning count;"""

    sql_add = 'insert into {}({}) values (%s) returning id;'
    
    sql_select_id = """select id
            from {}
            where value = (%s);"""

    sql_link_record_exists = """select we.id 
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
            words_only.append(word)

        try:
            #update independent counts
            cursor.execute(sql_exists.format(
            table,
            column),
            (word,))

            exists = cursor.fetchone()[0]
            print(exists)

            if exists:
                cursor.execute(sql_increment.format(
                table),
                (word,))

                increment = cursor.fetchone()[0]
                print(increment)
            else:
                cursor.execute(sql_add.format(
                table,
                column),
                (word, ))

                add = cursor.fetchone()[0]
                print(add)

            #update linked counts
            if is_emoji:
                for linked_word in words_only:
                    cursor.execute(sql_link_record_exists,
                        (linked_word, word,))

                    found = cursor.fetchone()
                    print(found)
                    
                    if found == None:
                        cursor.execute(sql_create_link_record,
                            (linked_word, word,))

                        record_id = cursor.fetchone()[0]
                    else:
                        cursor.execute(sql_update_reference_count,
                            (found[0],))
                        reference_count = cursor.fetchone()[0]
                        print(reference_count)


                        
                    
                

        except (Exception, psycopg2.DatabaseError) as error:
                    print(error)

     

token =  "" # Сервисный ключ доступа
session = vk.Session(access_token=token)  # Авторизация
vk_api = vk.API(session)
next_from = -1
result = []


#for emoji in UNICODE_EMOJI:
while next_from != -1:
    next_from, posts = collect_posts(vk_api, '😄', start=next_from)
    result += posts

stemmed_posts = stem_posts(result)
print(stemmed_posts)

conn = psycopg2.connect(dbname='emoji_database',
        user='postgres', 
        password='',
        host='emoji.cqppiab1dnlz.eu-central-1.rds.amazonaws.com')

cursor = conn.cursor()
print(cursor)

send_to_database(['ноготоч', '😄'], cursor, 'words', 'value')

conn.commit()
cursor.close()
conn.close()
