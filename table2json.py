import pandas as pd
import io
import argparse
import os
import zipfile

parser = argparse.ArgumentParser()
parser.add_argument('-file')
parser.add_argument('-sep', nargs='?')
parser.add_argument('-enc', nargs='?')
parser.add_argument('-pwd', nargs='?')
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
        archive = zipfile.ZipFile(args.file, 'r')

        for subFile in archive.namelist():
            subFileExt = os.path.splitext(subFile)[1]
            subFileAUX = archive.read(subFile)
            io_subFile = io.BytesIO(subFileAUX)

            if subFileExt in SUPPORTED_TEXT_FORMATS:
                assumedSeparator = ','
                maxCount = 0      
                helpString = subFileAUX.decode('utf-8')[:2000]

                for separator in LIST_SEPARATORS:
                    count = helpString.count(separator)
                    if count > maxCount:
                        assumedSeparator = separator
                        maxCount = count

                df =  pd.read_csv(io_subFile, sep=assumedSeparator)
                dataFrames[args.file +'.' + subFile] = df
            
            if subFileExt in SUPPORTED_EXCEL_FORMATS:
                xls = pd.ExcelFile(io_subFile)
                for sheet in xls.sheet_names:
                    df = pd.read_excel(io_subFile, sheet_name=sheet)
                    dataFrames[args.file + '.' + subFile + '.' + sheet] = df
                



    # Handling of text-based files
    if FILE_EXT in SUPPORTED_TEXT_FORMATS:
        with open(args.file, encoding = args.enc) as f:
            fileSeparator = args.sep
            if not args.sep:
                fileSeparator = assume_separator(f)

            dataFrames[args.file] = pd.read_csv(args.file, sep=fileSeparator)



    # Handling of binary data (excel)
    if FILE_EXT in SUPPORTED_EXCEL_FORMATS:
        with open(args.file, 'rb') as f:
            xls = pd.ExcelFile(f)
            for sheet in xls.sheet_names:
                dataFrames[args.file + '.' + sheet] = pd.read_excel(f, sheet_name=sheet)


    for df_name, df in dataFrames.items():
        print('\n')
        print(df_name)
        print(df)
    

    

            
def assume_separator(openTableFile):

    dataString = openTableFile.read().replace('\n','')[:2000]
    maxSeparatorOccurrences = 0
    assumedSeparator = ','

    for separator in LIST_SEPARATORS:
        count = dataString.count(separator)
        if count > maxSeparatorOccurrences:
            maxSeparatorOccurrences = count
            assumedSeparator = separator

    return assumedSeparator



if __name__ == "__main__":
    main()