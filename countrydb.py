import sqlite3
import pandas as pd
import math
conn = sqlite3.connect('ghi.db')


conn.execute(''' DROP TABLE IF EXISTS country2010 ''')
conn.execute(''' DROP TABLE IF EXISTS country2013 ''')
conn.execute(''' DROP TABLE IF EXISTS country2015 ''')
conn.execute(''' DROP TABLE IF EXISTS countryp2010 ''')
conn.execute(''' DROP TABLE IF EXISTS countryp2013 ''')
conn.execute(''' DROP TABLE IF EXISTS countryp2015 ''')

conn.execute(
    ''' CREATE TABLE country2010 (country text, total real, tb real, malaria real, hiv real, roundworm real, hookworm real, whipworm real, schistosomiasis real, lf real) ''')
conn.execute(
    ''' CREATE TABLE country2013 (country text, total real, tb real, malaria real, hiv real, roundworm real, hookworm real, whipworm real, schistosomiasis real, onchoceriasis real, lf real) ''')
conn.execute(
    ''' CREATE TABLE country2015 (country text, total real, tb real, malaria real, hiv real, roundworm real, hookworm real, whipworm real, schistosomiasis real, onchoceriasis real, lf real) ''')

conn.execute(
    ''' CREATE TABLE countryp2010 (country text, total real, tb real, malaria real, hiv real, roundworm real, hookworm real, whipworm real, schistosomiasis real, lf real) ''')
conn.execute(
    ''' CREATE TABLE countryp2013 (country text, total real, tb real, malaria real, hiv real, roundworm real, hookworm real, whipworm real, schistosomiasis real, onchoceriasis real, lf real) ''')
conn.execute(
    ''' CREATE TABLE countryp2015 (country text, total real, tb real, malaria real, hiv real, roundworm real, hookworm real, whipworm real, schistosomiasis real, onchoceriasis real, lf real) ''')


#url = 'https://docs.google.com/spreadsheets/d/1IBfN_3f-dG65YbLWQbkXojUxs2PlQyo7l04Ubz9kLkU/pub?gid=0&single=true&output=csv'

url = 'ORS_Impact_Score_2010_2013.csv'
url2010B2015 = 'ORS_Impact_Score_2010B_2015.csv'
df = pd.read_csv(url, skiprows=1)
df2015 = pd.read_csv(url2010B2015, skiprows=1)
is_df2015_true = df2015.notnull()
is_df_true = df.notnull()


def clean(num):
    return float(num.replace(' ', '').replace(',', '').replace('-', '0'))


countrydata = []
mapp = []

for i in range(3, 220):
    country = df.iloc[i, 0]
    if is_df_true.iloc[i, 7] == True:
        tb = clean(df.iloc[i, 7])
    else:
        tb = 0
    if is_df_true.iloc[i, 34] == True:
        malaria = clean(df.iloc[i, 34])
    else:
        malaria = 0
    if is_df_true.iloc[i, 47] == True:
        hiv = clean(df.iloc[i, 47])
    else:
        hiv = 0
    if is_df_true.iloc[i, 56] == True:
        roundworm = clean(df.iloc[i, 56])
    else:
        roundworm = 0
    if is_df_true.iloc[i, 57] == True:
        hookworm = clean(df.iloc[i, 57])
    else:
        hookworm = 0
    if is_df_true.iloc[i, 58] == True:
        whipworm = clean(df.iloc[i, 58])
    else:
        whipworm = 0
    if is_df_true.iloc[i, 61] == True:
        schistosomiasis = clean(df.iloc[i, 61])
    else:
        schistosomiasis = 0
    if is_df_true.iloc[i, 64] == True:
        lf = clean(df.iloc[i, 64])
    else:
        lf = 0
    total = tb + malaria + hiv + roundworm + hookworm + whipworm + schistosomiasis + lf
    row = [country, total, tb, malaria, hiv, roundworm, hookworm, whipworm, schistosomiasis, lf]
    countrydata.append(row)

sortedlist = sorted(countrydata, key=lambda xy: xy[1], reverse=True)
maxrow = sortedlist[0]
maxval = maxrow[1]
for j in sortedlist:
    country = j[0]
    total = (j[1] / maxval) * 100
    tb = (j[2] / maxval) * 100
    malaria = (j[3] / maxval) * 100
    hiv = (j[4] / maxval) * 100
    roundworm = (j[5] / maxval) * 100
    hookworm = (j[6] / maxval) * 100
    whipworm = (j[7] / maxval) * 100
    schistosomiasis = (j[8] / maxval) * 100
    lf = (j[9] / maxval) * 100
    row = [country, total, tb, malaria, hiv, roundworm, hookworm, whipworm, schistosomiasis, lf]
    mapp.append(row)
for k in countrydata:
    print(k)
    conn.execute(''' INSERT INTO country2010 VALUES (?,?,?,?,?,?,?,?,?,?) ''', k)

for l in mapp:
    conn.execute(''' INSERT INTO countryp2010 VALUES (?,?,?,?,?,?,?,?,?,?) ''', l)

countrydata2 = []
mapp2 = []
for i in range(3, 218):
    country = df.iloc[i, 67]
    if is_df_true.iloc[i, 74] == True:
        tb = clean(df.iloc[i, 74])
    else:
        tb = 0
    if is_df_true.iloc[i, 104] == True:
        malaria = clean(df.iloc[i, 104])
    else:
        malaria = 0
    if is_df_true.iloc[i, 115] == True:
        hiv = clean(df.iloc[i, 115])
    else:
        hiv = 0
    if is_df_true.iloc[i, 124] == True:
        roundworm = clean(df.iloc[i, 124])
    else:
        roundworm = 0
    if is_df_true.iloc[i, 125] == True:
        hookworm = clean(df.iloc[i, 125])
    else:
        hookworm = 0
    if is_df_true.iloc[i, 126] == True:
        whipworm = clean(df.iloc[i, 126])
    else:
        whipworm = 0
    if is_df_true.iloc[i, 129] == True:
        schistosomiasis = clean(df.iloc[i, 129])
    else:
        schistosomiasis = 0
    if is_df_true.iloc[i, 131] == True:
        onchoceriasis = clean(df.iloc[i, 131])
    else:
        onchoceriasis = 0
    if is_df_true.iloc[i, 134] == True:
        lf = clean(df.iloc[i, 134])
    else:
        lf = 0
    total = tb + malaria + hiv + roundworm + hookworm + whipworm + schistosomiasis + onchoceriasis + lf
    row = [country, total, tb, malaria, hiv, roundworm, hookworm, whipworm, schistosomiasis, onchoceriasis, lf]
    countrydata2.append(row)

sortedlist2 = sorted(countrydata2, key=lambda xy: xy[1], reverse=True)
maxrow = sortedlist2[0]
maxval = maxrow[1]
for j in sortedlist2:
    country = j[0]
    total = (j[1] / maxval) * 100
    tb = (j[2] / maxval) * 100
    malaria = (j[3] / maxval) * 100
    hiv = (j[4] / maxval) * 100
    roundworm = (j[5] / maxval) * 100
    hookworm = (j[6] / maxval) * 100
    whipworm = (j[7] / maxval) * 100
    schistosomiasis = (j[8] / maxval) * 100
    onchoceriasis = (j[9] / maxval) * 100
    lf = (j[10] / maxval) * 100
    row = [country, total, tb, malaria, hiv, roundworm, hookworm, whipworm, schistosomiasis, onchoceriasis, lf]
    mapp2.append(row)

for k in countrydata2:
    print(k)
    conn.execute(''' INSERT INTO country2013 VALUES (?,?,?,?,?,?,?,?,?,?,?) ''', k)

for l in mapp2:
    conn.execute(''' INSERT INTO countryp2013 VALUES (?,?,?,?,?,?,?,?,?,?,?) ''', l)


countrydata3 = []
mapp2 = []
for i in range(3, 218):
    country = df2015.iloc[i, 67]
    if is_df2015_true.iloc[i, 74] == True:
        tb = clean(df2015.iloc[i, 74])
    else:
        tb = 0
    if is_df2015_true.iloc[i, 104] == True:
        malaria = clean(df2015.iloc[i, 104])
    else:
        malaria = 0
    if is_df2015_true.iloc[i, 115] == True:
        hiv = clean(df2015.iloc[i, 115])
    else:
        hiv = 0
    if is_df2015_true.iloc[i, 124] == True:
        roundworm = clean(df2015.iloc[i, 124])
    else:
        roundworm = 0
    if is_df2015_true.iloc[i, 125] == True:
        hookworm = clean(df2015.iloc[i, 125])
    else:
        hookworm = 0
    if is_df2015_true.iloc[i, 126] == True:
        whipworm = clean(df2015.iloc[i, 126])
    else:
        whipworm = 0
    if is_df2015_true.iloc[i, 129] == True:
        schistosomiasis = clean(df2015.iloc[i, 129])
    else:
        schistosomiasis = 0
    if is_df2015_true.iloc[i, 131] == True:
        onchoceriasis = clean(df2015.iloc[i, 131])
    else:
        onchoceriasis = 0
    if is_df2015_true.iloc[i, 134] == True:
        lf = clean(df2015.iloc[i, 134])
    else:
        lf = 0
    total = tb + malaria + hiv + roundworm + hookworm + whipworm + schistosomiasis + onchoceriasis + lf
    row = [country, total, tb, malaria, hiv, roundworm, hookworm, whipworm, schistosomiasis, onchoceriasis, lf]
    countrydata3.append(row)

sortedlist2 = sorted(countrydata3, key=lambda xy: xy[1], reverse=True)
maxrow = sortedlist2[0]
maxval = maxrow[1]
for j in sortedlist2:
    country = j[0]
    total = (j[1] / maxval) * 100
    tb = (j[2] / maxval) * 100
    malaria = (j[3] / maxval) * 100
    hiv = (j[4] / maxval) * 100
    roundworm = (j[5] / maxval) * 100
    hookworm = (j[6] / maxval) * 100
    whipworm = (j[7] / maxval) * 100
    schistosomiasis = (j[8] / maxval) * 100
    onchoceriasis = (j[9] / maxval) * 100
    lf = (j[10] / maxval) * 100
    row = [country, total, tb, malaria, hiv, roundworm, hookworm, whipworm, schistosomiasis, onchoceriasis, lf]
    mapp2.append(row)
    #print(countrydata3)
for k in countrydata3:
    #print(k)
    conn.execute(''' INSERT INTO country2015 VALUES (?,?,?,?,?,?,?,?,?,?,?) ''', k)

for l in mapp2:
    print(l)
    conn.execute(''' INSERT INTO countryp2015 VALUES (?,?,?,?,?,?,?,?,?,?,?) ''', l)

conn.commit()
print("Database operation complete")