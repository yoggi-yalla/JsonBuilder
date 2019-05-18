import pandas as pd

with f as open('test.csv'):
    df = pd.read_csv(f)

print(df)