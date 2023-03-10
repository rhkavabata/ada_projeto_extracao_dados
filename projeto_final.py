import requests
import pandas as pd
import json
import time
import pyarrow.parquet as pq
import pyarrow as pa

table_full = []
researches = ['brasil', 'brazil', 'brasil']
countries = ['br', 'us', 'pt']
apikey = "9c40fd592705f5408902457809ba3536"

# Inicia a busca de noticias aplicando os filtros informados em 'researches' e 'countries'
for item in range(len(countries)):
    url_news = f"https://gnews.io/api/v4/search?\
    q={researches[item]}&\
    country={countries[item]}&\
    max=10&\
    apikey={apikey}"

    news = requests.get(url_news)
    table = json.loads(news.content)

    # Ajusta os dados criando a coluna 'source_name' com a fonte da noticia e 'country' com o pais
    for i in range(len(table['articles'])):
        table['articles'][i]['source_name'] = table['articles'][i]['source']['name']
        table['articles'][i]['country'] = countries[item]

    # Concatena os dados em um unico dicionario
    table_full = table_full + table['articles']

# Gera um dataframe com as noticias e seleciona colunas relevantes
df_news = pd.DataFrame.from_dict(table_full)
df_news = df_news[['title', 'source_name', 'url', 'publishedAt', 'country']]

# Inicia a busca de analise de sentimentos
list_sentiment = []
url = "https://api.meaningcloud.com/sentiment-2.1"
key = "a447bb05801aea31149c47b677b46a68"

# Realiza a analise de sentimento de cada noticia
for i in range(len(df_news)):
    payload = {
        'key': key,
        'txt': df_news['title'][i],
        'lang': 'pt',
    }
    response = requests.post(url, data=payload)
    sentiment = json.loads(response.content)
    list_sentiment = list_sentiment+([{'title': df_news['title'][i], 'sentiment': sentiment['score_tag']}])
    time.sleep(1) # Espera 1 segundo para não gerar erro na API que realiza duas consultas por segundo

# Gera um dataframe com as analises de sentimento
df_sent = pd.DataFrame.from_dict(list_sentiment)
# Gera o dataframe final com o join das tabelas de noticias e analise de sentimento
df_final = df_news.merge(df_sent, how='left', left_on='title', right_on='title')
# Gera o arquivo parquet
df_final.to_parquet('noticias.parquet')
