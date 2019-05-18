import pandas as pd

with open('test.csv') as f:
    df = pd.read_csv(f)

print(df)