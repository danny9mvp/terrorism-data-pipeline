import csv

def write_csv(filename, data):
    with open(filename, 'wb') as csv_file:
        csv_file.write(data)
