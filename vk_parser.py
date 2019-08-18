import vk
import re
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import sent_tokenize, word_tokenize, RegexpTokenizer
from nltk.corpus import stopwords
import psycopg2


def collect_posts(sess, query, start):
	posts = []
	res = sess.newsfeed.search(q=query, v=5.101, start_from=start)

	posts = [item['text'] for item in res['items'] if item['post_type'] == 'post']
	next_from = res['next_from'] if 'next_from' in res.keys() else -1

	return next_from, posts

def stem_posts(posts):
	result = []
	stemmer = SnowballStemmer("russian")
	tokenizer = RegexpTokenizer(r'\w+')

	for post in posts:
		stemmed = []
                #TODO fix links detection and keep emojis in the result
		post = re.sub(r'(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]\
			{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))\
			[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})',
			'',
			post) #removing links, groups references and hashtags
		post = re.sub(r'#[a-zA-Z0-9]*|club[0-9]*', '', post)


		tokens = tokenizer.tokenize(post)

		for word in tokens:
			stemmed_word = stemmer.stem(word)

			if stemmed_word not in stopwords.words('russian'):
				stemmed.append(stemmed_word)

		result.append(stemmed)

	return result

def send_to_database(words, cursor, table, column):
    sql_exists = 'select exists (select 1 from {0} where {1} = (%s));'.format(
            table, column
            )

    sql_increment = """update {}
            set count = count + 1 where value = (%s)
            returning count;""".format(
            table
            )
    sql_add = 'insert into {}({}) values (%s) returning id;'.format(
            table, column
            )

    print(sql_exists)


    for word in words:
        try:
            cursor.execute(sql_exists, (word,))
            exists = cursor.fetchone()[0]
            print(exists)

            if exists:
                cursor.execute(sql_increment, (word,))
                increment = cursor.fetchone()[0]
                print(increment)
            else:
                cursor.execute(sql_add, (word, ))
                add = cursor.fetchone()[0]
                print(add)

        except (Exception, psycopg2.DatabaseError) as error:
                    print(error)

     

token = ""  # Сервисный ключ доступа
session = vk.Session(access_token=token)  # Авторизация
vk_api = vk.API(session)
next_from = -1
result = []

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

send_to_database(['ноготоч'], cursor, 'words', 'value')

conn.commit()
cursor.close()
conn.close()
