import pandas as pd
import argparse
import os
from zipfile import ZipFile
from io import BytesIO

parser = argparse.ArgumentParser()
parser.add_argument('-file')
parser.add_argument('-sep', nargs='?')
parser.add_argument('-qchar', nargs='?')
parser.add_argument('-enc', nargs='?')
parser.add_argument('-pwd', nargs='?')
args = parser.parse_args()

FILE_EXT = os.path.splitext(args.file)[1]

SUPPORTED_COMPRESSED_FORMATS = {'.zip'}
SUPPORTED_TEXT_FORMATS = {'.txt', '.csv'}
SUPPORTED_EXCEL_FORMATS = {'.xlsx', '.xls'}

LIST_SEPARATORS = {',',';','\t'}

'''
Run the script in command line from this working dir:
python table2dfs.py -file ../test/test.zip
'''


def main():
    datasets = {}

    # Handling of .zip files
    if FILE_EXT in SUPPORTED_COMPRESSED_FORMATS:
        archive = ZipFile(args.file, 'r')
        for sub_file in archive.namelist():
            extracted_file = archive.extract(sub_file)
            extension = os.path.splitext(sub_file)[1]

            if extension in SUPPORTED_TEXT_FORMATS:
                datasets[os.path.basename(extracted_file)] = text_to_df(extracted_file)
            if extension in SUPPORTED_EXCEL_FORMATS: 
                datasets.update(excel_to_df(extracted_file))

            os.remove(extracted_file)
            

    # Handling of text-based files
    if FILE_EXT in SUPPORTED_TEXT_FORMATS:
        datasets[os.path.basename(args.file)] = text_to_df(args.file)


    # Handling of excel-based files
    if FILE_EXT in SUPPORTED_EXCEL_FORMATS:
        datasets.update(excel_to_df(args.file))


    for name, dataset in datasets.items():
        print('\n')
        print(name)
        print(dataset)
        
   
def text_to_df(table):
    separator = args.sep
    if not args.sep:
        separator = sniff_for_sep(table)
    dataset = pd.read_csv(table, sep=separator).astype(str)
    return dataset

def excel_to_df(table):
    temp_datasets = {}
    with open(table, 'rb') as f:
        xls = pd.ExcelFile(f)
        for sheet in xls.sheet_names:
            temp_datasets[os.path.basename(table) + '.' + sheet] = (
                pd.read_excel(f, sheet_name=sheet).astype(str)
            )
        return temp_datasets
         
def sniff_for_sep(table_file, sniff_length=2000):
    with open(table_file) as f:
        sniff_string = f.read().replace('\n','')[:sniff_length]
    max_sep_count = 0
    sep_guess = ','

    for separator in LIST_SEPARATORS:
        current_sep_count = sniff_string.count(separator)
        if current_sep_count > max_sep_count:
            max_sep_count = current_sep_count
            sep_guess = separator

    return sep_guess


if __name__ == "__main__":
    main()