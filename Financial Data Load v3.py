import pandas as pd
import csv
from urllib.parse import urlparse
import urllib as ul
from bs4 import BeautifulSoup as bs


tickerlistpath = "tickers.csv"
df=pd.DataFrame()
with open(tickerlistpath, mode='r') as f:
    reader = csv.reader(f)
    for row in reader:
        try:
            ticker = row[0]
            print(ticker)
            url="https://www.google.com/finance?q=NYSE%3A"+ ticker
            o = ul.request.urlopen(url).read().decode()
            o=bs(o,"html.parser")
            url = "https://www.google.com/finance?q=" + ticker
            p = ul.request.urlopen(url).read().decode()
            p=bs(p,"html.parser")
            try:
                sector=o.find('a',{'id':'sector'}).text
            except:
                try:
                    sector=p.find('a',{'id':'sector'}).text
                except:
                    sector="N/A"
            df=df.append({"ticker":ticker,"sector":sector}, ignore_index=True)
            
        except IndexError:
            print("Nothing")
df.to_csv("sectors.csv")
