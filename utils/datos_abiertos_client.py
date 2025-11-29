import requests

def get_terrorism_file():
    url = spark.conf.get("terrorism.url")
    response = requests.get(url)
    response.raise_for_status()
    return response.content.decode('utf-8')

def get_divipola_file():
    url = spark.conf.get("divipola.url")
    response = requests.get(url)
    response.raise_for_status()
    return response.content.decode('utf-8')
    