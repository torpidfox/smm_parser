import vk
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import sent_tokenize, word_tokenize



limit = 50

def collect_posts(sess, query, start):
	posts = []
	res = sess.newsfeed.search(q=query, v=5.101, start_from=start)

	posts = [item['text'] for item in res['items'] if item['post_type'] == 'post']
	next_from = res['next_from'] if 'next_from' in res.keys() else -1

	return next_from, posts

def stem_posts(posts):
	result = []
	stemmer = SnowballStemmer("russian") 
	for post in posts:
		stemmed = []
		tokens = word_tokenize(post)

		for word in tokens:
			stemmed.append(stemmer.stem(word))

		result.append(stemmed)


token = ""  # Сервисный ключ доступа
session = vk.Session(access_token=token)  # Авторизация
vk_api = vk.API(session)
next_from = 0
result = []

while next_from != -1:
	next_from, posts = collect_posts(vk_api, '😄', start=next_from)
	result += posts

stemmed_posts = stem_posts(result)