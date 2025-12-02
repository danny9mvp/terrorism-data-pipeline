import requests

class RestClient:

    def get_file_content(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    
