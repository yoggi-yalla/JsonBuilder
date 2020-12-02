from datetime import date

header = ('name','discount_factor','date')


d1 = date(year=2020, month=1, day=1) 
d2 = date(year=2020, month=1, day=2)
delta = d2-d1 

rows = [header] + [("USD_OIS", str(1*0.999999**x), (d1+delta*x).isoformat()) for x in range(1000000-2)]

with open('testgiant.csv', 'w') as f:
    for row in rows:
        f.write(",".join(row))
        f.write("\n")
