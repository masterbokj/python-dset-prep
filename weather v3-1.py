import pypyodbc
from pandas import DataFrame
import pandas as pd
import datetime
from time import gmtime, strftime
import csv
import sys
from datetime import timedelta, datetime, date

#this function iterates through dates when loading weather for airports
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
#establishing read connection
conn = pypyodbc.connect('Driver={SQL Server};'
                                'Server=borismsdn.database.windows.net;'
                                'Database=DemoData;'
                                'uid=readbot;pwd=xxxxxxx')
cursor = conn.cursor()
#How many years of data should be loaded for the new airport(initial load)
years_load_for_new_loc=1
#loading a dataframe that contains all needed airports, start and end dates for loading deltas (or initial load in case of new airports)
cur = cursor.execute('select l.airportcode, isnull(max_date, DateAdd(yy, -?, GetDate())) as max_date, DateAdd(dd, -1, GetDate()) as end_date from dbo.Locations l left join (select airportcode, max(date_utc) as max_date from dbo.Weather group by airportcode) w on l.airportcode=w.airportcode',(years_load_for_new_loc,))
df = DataFrame(cur.fetchall())

conn.close()
print(df)
#Parsing data for each airport
for index,row in df.iterrows():
    try:
        city = row[0]
        print("Parsing data for "+city)
        start_date = row[1]
        print("Start date: "+str(start_date))
        end_date = row[2]
        print("End date: "+str(end_date))
        df= []
        #Parsing each link based on date
        for single_date in daterange(start_date, end_date):
            datec=single_date.strftime("%Y/%m/%d")
            print(datec)
            url = "https://www.wunderground.com/history/airport/"+city+"/"+datec+"/DailyHistory.html?req_city=&req_statename=&MR=1&format=1"
            #some empty values were displayed as -9999, remove them. also last column had <br /> in the end of each row, didn't find other way to remove it
            cur_df=pd.read_table(url, delimiter=',',na_values='-9999').replace({'<br />': ''}, regex=True)
            #appending current day to previously loaded days in one dataframe to store them in 1 file then
            df.append(cur_df)
        df = pd.concat(df)
        filename = "data_"+city+".csv"
        #in some cases on this step columns were sorted in alphabetic order when storing to csv.
        #i also could not used names as first column had different names for different cities
        #to overcome this issue explicitly define names for columns to handle them in future
        df.columns = ['LocalTime','TemperatureC','Dew PointC','Humidity','SeaLevel PressurehPa','VisibilityKm','Wind Direction','Wind SpeedKm/h','GustSpeedKm/h','Precipitationmm','Events','Conditions','WindDirDegrees','DateUTC']
        #append airport name column
        df['airportcode']=city
        #append load date column
        df['today']=strftime("%Y-%m-%d %H:%M:%S", gmtime())
        #store dataframe into csv in a certain order that I need fir writing to SQL
        df.to_csv(filename,index=False, columns=['today','airportcode','LocalTime','TemperatureC','Dew PointC','Humidity','SeaLevel PressurehPa','VisibilityKm','Wind Direction','Wind SpeedKm/h','GustSpeedKm/h','Precipitationmm','Events','Conditions','WindDirDegrees','DateUTC'])
        #write connection
        conn = pypyodbc.connect('Driver={SQL Server};'
                                'Server=natemsdn.database.windows.net;'
                                'Database=DemoData;'
                                'uid=writebot;pwd=8u7Y6t5R4E')
        cursor = conn.cursor()
        with open(filename) as f:
            reader = csv.reader(f)
            #need this to skip the header and start writing from the first row
            next(reader,None)
            for row in reader:
                try:
                    print(row)
                    #everything is ready, so I can just pass a row from csv to SQL
                    cursor.execute("INSERT INTO dbo.Weather VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
                                 (row))
                    conn.commit()
                except:
                    #print error details in case of error
                    print(sys.exc_info()[0],sys.exc_info()[1])
                    conn.rollback()
    except IndexError:
        print("Nothing")
