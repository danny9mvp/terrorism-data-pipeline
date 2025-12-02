import csv

class FileUtils:

    def write_csv(self, filename, data):
        with open(filename, 'wb') as csv_file:
            csv_file.write(data)
