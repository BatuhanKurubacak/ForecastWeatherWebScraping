# -*- coding: utf-8 -*-
#15.06.2019 - 10.07.2019 
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

import requests
from bs4 import BeautifulSoup






# In[2]:


#!pip install selenium


# In[2]:


import os
import sys
sys.path.insert(0, '/usr/local/spark/python/')
sys.path.insert(0, '/usr/local/spark/python/lib/py4j-0.10.4-src.zip')
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'ipython3'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.0.0 pyspark-shell'

#import pyspark 
#from pyspark.streaming import StreamingContext
#from pyspark.sql import SparkSession, Row
#from pyspark.sql.functions import col
from datetime import date, timedelta
import dateutil.relativedelta 
import time
import datetime


# In[3]:


import pandas as pd
from collections import OrderedDict
from datetime import date



# In[4]:


def getDate():
  
    creationDate = datetime.datetime.now()
    return creationDate.strftime("%d%b%Y")  


# In[5]:


def getCreationDate():
    creationDate = datetime.datetime.now()
    return (creationDate.strftime("%d%b%Y:%H:%M:%S:000"))  



# In[6]:


forecastDate = "2019-06-18"


# In[7]:


def getForecastDate(dateText):
    splitText = dateText.split("-")
    yil=splitText[0]
    ay = splitText[1]
    gun = splitText[2] 
    x = datetime.datetime(int(yil), int(ay), int(gun))
    return x.strftime("%d%b%Y")  


# In[8]:
def mphtoMs(ms):
    return ms*0.44704

def mphToKmh(kmh):
    return kmh*1.609344 


# In[9]:


def windDirection(windText):
    
    windDegree = 0
    if windText == "N":
        windDegree = 360.00
    elif windText == "NNE":
        windDegree = 20.00
    elif windText == "NE":
        windDegree = 40.00
    elif windText == "ENE":
        windDegree = 70.00
    elif windText == "E":
        windDegree = 90.00
    elif windText == "ESE":
        windDegree = 110.00
    elif windText == "SE":
        windDegree = 130.00
    elif windText == "SSE":
        windDegree = 160.00
    elif windText == "S":
        windDegree = 180.00
    elif windText == "SSW":
        windDegree = 200.00
    elif windText == "SW":
        windDegree = 220.00
    elif windText == "WSW":
        windDegree = 250.00
    elif windText == "W":
        windDegree = 270.00
    elif windText == "WNW":
        windDegree = 290.00
    elif windText == "NW":
        windDegree = 310.00
    elif windText == "NNW":
        windDegree = 340.00
    
    return float(windDegree)    
        
    


# In[36]:


def convertTime(time):
    newTime=""
    #new=time.split(" ")
    #if new[1]=='AM':
     #   new1=new[0].split(":")
      #  newTime=time
        
       # if new1[0]==12:
        #    new1[0]=0
         #   return new1
        #return newTime    
    #elif new[1]=='PM':
     #   new2=new[0].split(":")
      #  new2[0]=int(new2[0])+12
       # return new2
    
    
    
    if time == "1:00 AM":
        newTime ="1"
    elif time=='12:20 AM':
        newTime="0:20"
    elif time=='12:50 AM':
        newTime='0:50'
    elif time=='1:20 AM':
        newTime='1:20'
    elif time=='1:50 AM':
        newTime='1:50'
    elif time=='2:20 AM':
        newTime='2:20'
    elif time=='2:50 AM':
        newTime='2:50'
    elif time=='3:20 AM':
        newTime='3:20'
    elif time=='3:50 AM':
        newTime='3:50'
    elif time=='4:20 AM':
        newTime='4:20'
    elif time=='4:50 AM':
        newTime='4:50'
    elif time=='5:20 AM':
        newTime='5:20'
    elif time=='5:50 AM':
        newTime='5:50'
    elif time=='6:20 AM':
        newTime='6:20'
    elif time=='6:50 AM':
        newTime='6:50'
    elif time=='7:20 AM':
        newTime='7:20'
    elif time=='7:50 AM':
        newTime='7:50'
    elif time=='8:20 AM':
        newTime='8:20'
    elif time=='8:50 AM':
        newTime='8:50'
    elif time=='9:20 AM':
        newTime='9:20'
    elif time=='9:50 AM':
        newTime='9:50'
    elif time=='10:20 AM':
        newTime='10:20'
    elif time=='10:50 AM':
        newTime='10:50'
    elif time=='11:20 AM':
        newTime='11:20'
    elif time=='11:50 AM':
        newTime='11:50'
    elif time=='12:20 PM':
        newTime='12:20'
    elif time=='12:50 PM':
        newTime='12:50'
    elif time=='1:20 PM':
        newTime='13:20'
    elif time=='1:50 PM':
        newTime='13:50'
    elif time=='2:20 PM':
        newTime='14:20'
    elif time=='2:50 PM':
        newTime='14:50'
    elif time=='3:20 PM':
        newTime='15:20'
    elif time=='3:50 PM':
        newTime='15:50'
    elif time=='4:20 PM':
        newTime='16:20'
    elif time=='4:50 PM':
        newTime='16:50'
    elif time=='5:20 PM':
        newTime='17:20'
    elif time=='5:50 PM':
        newTime='17:50'
    elif time=='6:20 PM':
        newTime='18:20'
    elif time=='6:50 PM':
        newTime='18:50'
    elif time=='7:20 PM':
        newTime='19:20'
    elif time=='7:50 PM':
        newTime='19:50'
    elif time=='8:20 PM':
        newTime='20:20'
    elif time=='8:50 PM':
        newTime='20:50'
    elif time=='9:20 PM':
        newTime='21:20'        
    elif time=='9:50 PM':
        newTime='21:50'
    elif time=='10:20 PM':
        newTime='22:20'
    elif time=='10:50 PM':
        newTime='22:50'
    elif time=='11:20 PM':
        newTime='23:20'
    elif time=='11:50 PM':
        newTime='23:50'
    elif time == "2:00 AM":
        newTime ="2"
    elif time == "3:00 AM":
        newTime ="3"
    elif time == "4:00 AM":
        newTime ="4"
    elif time == "5:00 AM":
        newTime ="5"
    elif time == "6:00 AM":
        newTime ="6"
    elif time == "7:00 AM":
        newTime ="7"
    elif time == "8:00 AM":
        newTime ="8"
    elif time == "9:00 AM":
        newTime ="9"
    elif time == "10:00 AM":
        newTime ="10"
    elif time == "11:00 AM":
        newTime ="11"
    elif time == "12:00 AM":
        newTime ="0"
    elif time == "1:00 PM":
        newTime ="13"
    elif time == "2:00 PM":
        newTime ="14"
    elif time == "3:00 PM":
        newTime ="15"
    elif time == "4:00 PM":
        newTime ="16"
    elif time == "5:00 PM":
        newTime ="17"
    elif time == "6:00 PM":
        newTime ="18"
    elif time == "7:00 PM":
        newTime ="19"
    elif time == "8:00 PM":
        newTime ="20"
    elif time == "9:00 PM":
        newTime ="21"
    elif time == "10:00 PM":
        newTime ="22"
    elif time == "11:00 PM":
        newTime ="23"
    elif time == "12:00 PM":
        newTime ="12"
        
    return newTime


# In[31]:



def createUrlList(forecatDate):
    print("metod "+forecatDate)
    
    urlList.append("https://www.wunderground.com/history/daily/tr/sabiha-gokcen/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/adana-incirlik-afb/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/ankara-etimesgut-ab/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/muratpasa/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/yenisehir/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/cardak/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/gaziantep/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/bakirkoy/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/gaziemir/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/kayseri/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/trabzon/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/baglar/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/ankara-etimesgut-ab/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/ankara-etimesgut-ab/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/gr/karoti/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/erzurum/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/adana-incirlik-afb/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/ankara-etimesgut-ab/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/gaziantep/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/konya/date/"+forecatDate)  
    urlList.append("https://www.wunderground.com/history/daily/tr/cardak/date/"+forecatDate)            
    urlList.append("https://www.wunderground.com/history/daily/tr/ankara-etimesgut-ab/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/adana-incirlik-afb/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/gaziantep/date/"+forecatDate)
    urlList.append("https://www.wunderground.com/history/daily/tr/ankara-etimesgut-ab/date/"+forecatDate)
    
    
    cityList.append("Istanbul_Asia")
    cityList.append("Adana")
    cityList.append("Ankara")
    cityList.append("Antalya")
    cityList.append("Bursa")
    cityList.append("Denizli")
    cityList.append("Gaziantep")
    cityList.append("Istanbul_Europe")
    cityList.append("Izmir")
    cityList.append("Kayseri")
    cityList.append("Trabzon")
    cityList.append("Diyarbakır")
    cityList.append("Bartın")
    cityList.append("Çankırı")
    cityList.append("Edirne")
    cityList.append("Erzurum")
    cityList.append("Hatay")
    cityList.append("Kastamonu")  
    cityList.append("Kilis")
    cityList.append("Konya")
    cityList.append("Kütahya")
    cityList.append("Kırıkkale")
    cityList.append("Mersin")
    cityList.append("Osmaniye")
    cityList.append("Zonguldak")


# In[32]:
#import r-units
import calc_apparent
#import metpy
#from metpy import calc
#from metpy.units import units
#import calc_apparent 
#from pint import UnitRegistry
#ureg = UnitRegistry()


def beginParse(name_box,index,index50,forecatDate):
    windSpeedList=[]
    windGustList=[]
    timeList=[]
    conditionsList=[]
    tempList=[]
    feelsLikeList=[]
    precipList=[]
    createdByList=[]
    cloudCoveList=[]
    dewPointList=[]
    humidityList=[]
    #windList=[]
    windDirectionList=[]
    pressureList=[]
    cityForecastList = []
    forecastDateList=[]
    dateList=[]
    Forecast_Date_Difference=[]
    heightList=[]
    creationDateList=[]
    obsOrForecastList=[]
    guncelFlagList=[]
    #print(name_box)
    tableBody = name_box.find("tbody")
    y = tableBody.findAll("tr")
    
    
    #print(len(y))
    #print("Girdi")
    for i in y : 
        
        
        k=i.findAll("td")
        guncelFlagList.append(1)
        cloudCoveList.append(-1)
        heightList.append(-1)
        creationDateList.append(getCreationDate())
        forecastDateList.append(getDate())
        dateList.append(getForecastDate(forecatDate))
        Forecast_Date_Difference.append(1)
        obsOrForecastList.append("O")
        time = convertTime(k[0].find("span").text)
        #print(time)
        
        if time=='10:50'or time=='11':
            timeList.append(time)
            print(time)
        #print(k[1].find("span",attrs={"class":"show-for-medium conditions"}).text)
        #conditionsList.append(k[1].find("span",attrs={"class":"show-for-medium conditions"}).text)
        #print(k[2].find("span").text)
        
            fahrenheit = k[1].find("span").text
        #print(fahrenheit)
            splitFahrenheit = fahrenheit.split(" ")
            celcius = (int(splitFahrenheit[0]) - 32) / 1.8
        #print(celsius)
            tempList.append(celcius)
        #feelsLikeList.append(celsius)
        #print(k[2].find("span").text)
        
            fahrenheit1 = k[2].find("span").text
            splitFahrenheit1 = fahrenheit1.split(" ")
            celsiusD = (int(splitFahrenheit1[0]) - 32) / 1.8
            dewPointList.append(celsiusD)
        
        
       
        #humidityList.append(k[3].find("span",attrs={"class":"wu-value wu-value-to"}).text)
            humidity1=k[3].find("span",attrs={"class":"wu-value wu-value-to"}).text
            humidityList.append(humidity1)
      
        
            windText =  k[4].find("span").text
            windDirectionList.append(windDirection(windText))
        
            windSpeed= k[5].find("span").text
            windSpeedSplit=windSpeed.split(" ")
            newSpeedmeter=mphtoMs(int(windSpeedSplit[0])) # convert mile to metre
            windSpeedList.append(newSpeedmeter)  #meters per second
        
    
        #print(celsius)
        #print(humidity1)
        #print(newSpeedmeter)
            apparent_temp=calc_apparent.calc(splitFahrenheit[0],windSpeedSplit[0],humidity1)
            #print("feels like" +str(apparent_temp))
            apparent=(apparent_temp-32) / 1.8
            feelsLikeList.append(apparent)
        
        #if newSpeedmeter<10:
         #   apparent=metpy.calc.apparent_temperature(celcius,0.1*float(humidity1),
        #                                             metpy.units(newSpeedmeter),False)
        #elif newSpeedmeter>=10:
         #   apparent=metpy.calc.apparent_temperature(celcius,0.1*humidity1,newSpeedmeter,True)
       # print("feels like: " +apparent)
       # feelsLikeList.append(float(apparent))
        
        #newSpeedKM=mphToKmh(int(windSpeedSplit[0]))
        #t = Temp(celsius, 'c')  #define celcius or fahrenheit 
        #print("temperature = " +str(int(celsius)))
        #fl=feels_like(float(splitFahrenheit[0]),float(humidity1),float(windSpeedSplit[0]))
        #flCelsius=(fl-32) / 1.8
        #print("feelsLike = " +str(int(flCelsius)))
        #feelsLikeList.append(flCelsius)
        
            windGust= k[6].find("span").text
            windGustSplit=windGust.split(" ")
        #print(windGustSplit[0])
            windGustList.append(int(windGustSplit[0]))
        
            pressureList.append(k[7].find("span",attrs={"class":"wu-value wu-value-to"}).text)
            
            precipList.append(k[8].find("span",attrs={"class":"wu-value wu-value-to"}).text)
            createdByList.append(-9)
    
        #condition=k[10].find("span").text
        #print(condition)
            conditionsList.append(k[10].find("span").text)
            #cityForecastList.append(cityList[index])
        allList.append( list(zip(forecastDateList,dateList,Forecast_Date_Difference,timeList,
                             tempList,feelsLikeList,humidityList,windSpeedList,
                             windDirectionList,
                             cloudCoveList,heightList,creationDateList,createdByList,
                             obsOrForecastList,cityForecastList,guncelFlagList)))
 
 
    

        
    
    
    


# In[40]:

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException




driver=webdriver.Chrome('C:/Users/user/Desktop/chromedriver.exe')

if __name__ == "__main__": 
    #EndDate = str(str(datetime.datetime.now()+timedelta(days=k-3)))
    EndDate = str(datetime.datetime.now()-timedelta(days=4))
    dateTimeText = EndDate.split(" ")
    forecastDate = dateTimeText[0]
    urlList = []
    cityList = []
    allList=[]
    search=[]
    df=pd.DataFrame(search)
    #for k in range(1):
    createUrlList(forecastDate)
    index =-1
    index50=-1
    print(len(urlList))
    print(urlList[0])
               
    for item in urlList :
            
        try:
            driver.get(item)
            element=WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID,"history-observation-table")))
            print(driver.title)
            print(type(element))
            
        except TimeoutException:
                driver.quit()
          
        soup_level1=BeautifulSoup(driver.page_source, 'html.parser')
        print(type(soup_level1))
        name_box = soup_level1.find("table", attrs={"id": "history-observation-table"})
            #print(type(name_box))
        index = index +1                                   
        index50=index50+1
        beginParse(name_box,index,index50,forecastDate)
        index50=-1
            
    
        urlList = []
        cityList = []
        time.sleep(2)
           
                
            
#wb.save('test9.xls')     
            


# In[34]:


#df = pd.DataFrame(columns=['ForecastDate','Date','Forecast_Date_Difference','Hour','AirTemperature','ApparentTemperature','RelativeHumidity','WindSpeed','WindDirection','EffectiveCloudCover','PrecipitationHeight','CreationDate','CreatedBy','ObsOrForecast','City','guncel_flg'])
df = pd.DataFrame(allList[0])
df.columns = ['Observation_Date','Date','Observe_Date_Difference','Hour','Air_Temperature'
              ,'ApparentTemperature','Humidity','WindSpeed','WindDirection'
              ,'EffectiveCloudCover','PrecipitationHeight','CreationDate','CreatedBy'
              ,'ObsOrForecast','City','guncel_flg']
#multi_index=pd.MultiIndex(allList[0])
#df=pd.DataFrame(columns=multi_index)

for i in range(len(allList)-1):
    resultDf = pd.DataFrame(allList[i+1])
    resultDf.columns = ['Observation_Date','Date','Observe_Date_Difference','Hour',
                        'Air_Temperature','ApparentTemperature','Humidity','WindSpeed'
                        ,'WindDirection','EffectiveCloudCover','PrecipitationHeight',
                        'CreationDate','CreatedBy','ObsOrForecast','City','guncel_flg']
    
    df= pd.concat([df,resultDf])

#df.to_excel(r'test17.xlsx')
#    #spark_df.write.csv(str(index2+10)+'.xlsx')


# In[27]:


len(allList)


# In[35]:


df.to_excel(r'data_visualization.xlsx')


# In[413]:


df

# In[218]:


import xlwt 
from xlwt import Workbook 
  

# Workbook is created 
wb = Workbook() 

sheet1 = wb.add_sheet('Sheet 1') 

sheet1.write(0,0,"ForecastDate")
sheet1.write(0,1,"Date")
sheet1.write(0,2,"Forecast_Date_Difference")
sheet1.write(0,3,"Location")
sheet1.write(0,4,"Hour")
sheet1.write(0,5,"Conditions")
sheet1.write(0,6,"AirTemperature")
sheet1.write(0,7,"ComfortTemperature")
sheet1.write(0,8,"Precip")
sheet1.write(0,9,"Amount")
sheet1.write(0,10,"CloudCover")
sheet1.write(0,11,"DewPoint")
sheet1.write(0,12,"Humidity")
sheet1.write(0,13,"WindSpeed")
sheet1.write(0,14,"WindDirection")
sheet1.write(0,15,"Pressure")

sayac = 0 
for l in range (len(allList)):
    for k in range (len(allList[l])):
        for p in range(12):
            if p is 1 :
                
                time = convertTime (allList[l][k][p])
                sheet1.write(sayac+1, p+3, time) 
            elif p==3 or p==4 or p==8:
                fahrenheit = allList[l][k][p]
                splitFahrenheit = fahrenheit.split(" ")
                celsius = (int(splitFahrenheit[0]) - 32) / 1.8
                sheet1.write(sayac+1, p+3, int(celsius))
                
            elif p==9:
                percentText = allList[l][k][p]
                floatNumber = float(percentText.strip('%'))/100
                sheet1.write(sayac+1, p+3,floatNumber)
            elif p==10:
                windText =  allList[l][k][p]
                windSplitText  = windText.split(" ")
                sheet1.write(sayac+1, p+3,int(windSplitText[0]))
                sheet1.write(sayac+1, p+4,windDirection(windSplitText[2]))
            else :  
                if p==11:
                    sheet1.write(sayac+1, p+4, allList[l][k][p])
                else:
                    sheet1.write(sayac+1, p+3, allList[l][k][p]) 
                #allList[l][k][p] = convertTime (allList[l][k][p])   
                
        sayac=sayac+1

for i in range(sayac):
    sheet1.write(i+1, 0,getDate() )
    sheet1.write(i+1, 1,getForecastDate(forecatDate))
    sheet1.write(i+1, 2,getCreationDate())

    
wb.save("test16.xls")


# In[435]:


EndDate = str(datetime.datetime.now()+timedelta(days=23))
dateTimeText = EndDate.split(" ")
print(dateTimeText[0])


# In[437]:

#EndDate = str(datetime.datetime.now())
EndDate=str(datetime.datetime.now()+timedelta(days=k-3))
print(EndDate)



