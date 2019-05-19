import pandas as pd
import argparse
import os
from zipfile import ZipFile
from io import BytesIO

parser = argparse.ArgumentParser()
parser.add_argument('-file')
parser.add_argument('-sep', nargs='?')
#parser.add_argument('-enc', nargs='?')
#parser.add_argument('-pwd', nargs='?')
parser.parse_args()
args = parser.parse_args()

FILE_EXT = os.path.splitext(args.file)[1]

SUPPORTED_COMPRESSED_FORMATS = {'.zip'}
SUPPORTED_TEXT_FORMATS = {'.txt', '.csv'}
SUPPORTED_EXCEL_FORMATS = {'.xlsx', '.xls'}

LIST_SEPARATORS = {',',';','\t'}


def main():
    dataFrames = {}

    # Handling of .zip files
    if FILE_EXT in SUPPORTED_COMPRESSED_FORMATS:
        archive = ZipFile(args.file, 'r')
        for subFile in archive.namelist():
            extension = os.path.splitext(subFile)[1]

            if extension in SUPPORTED_TEXT_FORMATS:
                dataFrames[subFile] = text_to_df(subFile)
            if extension in SUPPORTED_EXCEL_FORMATS: 
                dataFrames.update(excel_to_df(subFile))              


    # Handling of text-based files
    if FILE_EXT in SUPPORTED_TEXT_FORMATS:
        dataFrames[args.file] = text_to_df(args.file)


    # Handling of excel-based files
    if FILE_EXT in SUPPORTED_EXCEL_FORMATS:
        dataFrames.update(excel_to_df(args.file))


    for name, df in dataFrames.items():
        print('\n')
        print(name)
        print(df)
        
   
def text_to_df(table):
    separator = args.sep
    if not args.sep:
        separator = assume_separator(table)
    df = pd.read_csv(table, sep=separator)
    return df

def excel_to_df(table):
    tempFrames = {}
    with open(table, 'rb') as f:
        xls = pd.ExcelFile(f)
        for sheet in xls.sheet_names:
            tempFrames[table + '.' + sheet] = pd.read_excel(f, sheet_name=sheet)
        return tempFrames
    
         
def assume_separator(tableFile):
    with open(tableFile) as f:
        tableString = f.read().replace('\n','')[:2000]
    maxSeparatorOccurrences = 0
    assumedSeparator = ','

    for separator in LIST_SEPARATORS:
        count = tableString.count(separator)
        if count > maxSeparatorOccurrences:
            maxSeparatorOccurrences = count
            assumedSeparator = separator

    return assumedSeparator


if __name__ == "__main__":
    main()