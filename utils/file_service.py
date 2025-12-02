from utils.datos_abiertos_client import RestClient
from utils.file_utils import FileUtils

class FileService:

    def __init__(self, client: RestClient, file_utils: FileUtils):
        self.client = client
        self.file_utils = file_utils

    def write_csv_file(self, resource_url, file_path):
        csv_content = self.client.get_file_content(resource_url)
        self.file_utils.write_csv(file_path, csv_content)