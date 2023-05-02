import requests
from bs4 import BeautifulSoup
from lxml import etree
from pymongo import MongoClient
import time
from datetime import datetime, timedelta
import sys
import json
from pytz import timezone
#Bug Fixes to address: 
#NWS is not getting up to date scrape it is lagging
#NWS Hi Temp finding


class Scrape_Machine:

    def __init__(self):
        self.headers = {"#######"}
        cluster = "####"
        client = MongoClient(cluster)

        self.db = client['weather']



    def decodeMetar(self, metar, tz_adj = -4 ):
        result = {}

        split_metar = metar.split()
        date = split_metar[1]
        datetime_str_utc = date[0:2] + '-' + date[2:4] + ':' + date[4:6]
        timestamp_utc = datetime.strptime(datetime_str_utc, "%d-%H:%M")
        timestamp_est = timestamp_utc + timedelta(hours=tz_adj)
        result['time'] = str(timestamp_est.day) + ' ' + str(timestamp_est.hour) + ':' +  "{:02d}".format(timestamp_est.minute)
        #add temperature 
        temp_index = metar.find(" T")
        temp_reading = metar[temp_index +2: temp_index+9]
        temp_C = float(temp_reading[1:4])*.1
        if temp_reading[0] == "1":
            temp_C*=-1

        temp_f = round((temp_C * 9/5) + 32)
        result['temperature'] = temp_f

        Hi = False
        for code in split_metar:
            if code[0] == '1' and code.isnumeric() and len(code) == 5:
                print(code)
                Hi_temp_C = float(code[2:])*.1
                if code[1] == 1:
                    Hi_temp_C *= -1

                Hi_temp_f = round((Hi_temp_C * 9/5) + 32)
                result['Hi 6hr'] = Hi_temp_f
                Hi = True
        if Hi == False:
            result['Hi 6hr'] = None
        return result


  



        

    def getCurrentMetar(self, raw=False):
        req = requests.get('https://www.aviationweather.gov/metar/data?ids=knyc&format=raw&hours=0&taf=off&layout=on').text
        start = req.find("<code>") 
        if start > -1:
            code = req[start+ 6:req.find("</code>")]
            if raw:
                return code
            else:
                result = self.decodeMetar(code)
                return result



                

            
    def getRecordedData(self):
        #dom  = self.getHTML()
        dom = self.getHTML('https://www.aviationweather.gov/metar/data?ids=knyc&format=raw&hours=24&taf=off&layout=on')
        metars = dom.xpath('//*[@id="awc_main_content_wrap"]/code/text()')


        now = datetime.now()
        day = int(now.day)
        data = []
        for metar in metars:
            current_data = {}
            split_metar = metar.split()
            date = split_metar[1]
            datetime_str_utc = date[0:2] + '-' + date[2:4] + ':' + date[4:6]
            timestamp_utc = datetime.strptime(datetime_str_utc, "%d-%H:%M")
            timestamp_est = timestamp_utc + timedelta(hours=-4)
            current_data['time'] = str(timestamp_est.day) + ' ' + str(timestamp_est.hour) + ':' +  "{:02d}".format(timestamp_est.minute)


            if int(timestamp_est.day) == day or (int(timestamp_est.day) == day-1 and int(timestamp_est.hour) == 23) and int(timestamp_est.minute)==51:
                # add timestamp num
                if timestamp_est.hour == 23:
                    current_data['time number'] = 0
                else:
                    current_data['time number'] = timestamp_est.hour + 1
                #add temperature 
                try:
                    temp_index = metar.find(" T")
                    temp_reading = metar[temp_index +2: temp_index+9]
                    temp_C = float(temp_reading[1:4])*.1
                    if temp_reading[0] == "1":
                        temp_C*=-1

                    temp_f = round((temp_C * 9/5) + 32)
                    current_data['temperature'] = temp_f
                except:
                    #no farenheit reading but celcius reading
                    current_data['temperature'] = '*'
                
                for code in split_metar:
                    if code[0] == '1' and code.isnumeric() and len(code) == 5:
                        print(code)
                        Hi_temp_C = float(code[2:])*.1
                        if code[1] == 1:
                            Hi_temp_C *= -1

                        Hi_temp_f = round((Hi_temp_C * 9/5) + 32)
                        current_data['Hi 6hr'] = Hi_temp_f
                    

                data.append(current_data)
        data.reverse()
        hi = -100000
        first_past = False
        for dati in data[1:]:
            if dati['temperature'] > hi:
                hi = dati['temperature']
            if 'Hi 6hr' in dati.keys():
                if first_past == True:
                    if dati['Hi 6hr'] > hi:
                        hi = dati['Hi 6hr']
                else:
                    first_past = True

        
        result = {
            'Hi': hi,
            'Data': data
        }
        return result


            

    



                #print(str(timestamp_est.day) + '-' + str(timestamp_est.hour) + ':' + str(timestamp_est.minute), temp_f)
    def getHTML(self, link):
        response = requests.get(link, headers=self.headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        print(soup)
        body = soup.find('body')
        dom = etree.HTML(str(body))
        return dom


    def formatTime(self, time):
        time = time.lower()
        conversion = {
            '12 am': 0,
            '1 am': 1,
            '2 am': 2,
            '3 am': 3,
            '4 am': 4,
            '5 am': 5,
            '6 am': 6,
            '7 am': 7,
            '8 am': 8,
            '9 am': 9,
            '10 am': 10,
            '11 am': 11,
            '12 pm': 12,
            '1 pm': 13,
            '2 pm': 14,
            '3 pm': 15,
            '4 pm': 16,
            '5 pm': 17,
            '6 pm': 18,
            '7 pm': 19,
            '8 pm': 20,
            '9 pm': 21,
            '10 pm': 22,
            '11 pm': 23
        }

        if time in conversion.keys():
            return conversion[time]
        else:
            return int(time)

    def getCli(self):

        html = requests.get('https://forecast.weather.gov/product.php?site=NWS&issuedby=NYC&product=CLI&format=CI&version=1&glossary=1&highlight=off')
        content = str(html.content)


        date_tick = content[content.find('SUMMARY FOR'):]
        date_tick = date_tick[11:date_tick.find("""n""")][:-1]
        date_tick = date_tick.replace('.', '')

        if 'LOCAL TIME' in content:
            i = content.find('LOCAL TIME')
            local_time = content[i-9:i]
            date_tick += ' ' + local_time

        max_start_index = content.find('MAXIMUM')
        Hi_tick = content[max_start_index:][:30]
        Ticks = Hi_tick.split()


        Hi_temp = Ticks[1]

        try:
            Hi_time = Ticks[2] + ' ' + Ticks[3]
        except:
            Hi_time = Ticks[2]
        
        

        try:
            result = {
                'Valid Through': date_tick,
                'Hi Time': Hi_time,
                'Hi': int(Hi_temp)
            }

        except:
            result = {
                'Valid Through': date_tick,
                'Hi Time': Hi_time,
                'Hi': Hi_temp
            }

        print(result)
        return result

    def formatForecast(self, source, hi_today, hi_tomorrow, times_today, temps_today, times_tomorrow, temps_tomorrow):
        times_today = [self.formatTime(time) for time in times_today]
        times_tomorrow = [self.formatTime(time) for time in times_tomorrow]
        temps_today = [int(temp) for temp in temps_today]
        temps_tomorrow = [int(temp) for temp in temps_tomorrow]


        if hi_today != '-':
            hi_today = int(hi_today)
        else:
            hi_today = max(temps_today)

        hi_tomorrow = int(hi_tomorrow)

        if max(temps_today) > hi_today:
            hi_today = max(temps_today)
        if max(temps_tomorrow) > hi_tomorrow:
            hi_tomorrow = max(temps_tomorrow)

        #format all
        



        forecast_today = [{
            'time': times_today[i], 
            'temperature': temps_today[i]} 
            for i in range(len(times_today))]
        forecast_tomorrow = [{
            'time': times_tomorrow[i], 
            'temperature': temps_tomorrow[i]} 
            for i in range(len(temps_tomorrow))]
        print('!!!!!!!!!!!')
        print(datetime.now(timezone('America/New_York')).hour)
        if source == "Accuweather" and datetime.now(timezone('America/New_York')).hour == 23:
            print('accuweather acception')
            Forecast = {
                'Source': 'Accuweather',
                'Forecasts': {
                    'Today': {
                        'Hi': '-',
                        'Hourly': ['-']
                    },
                    'Tomorrow': {
                        'Hi': hi_today,
                        'Hourly': forecast_today
                    }
                }
            }
            
            return Forecast

        else:
            Forecast = {
                'Source': source,
                'Forecasts': {
                    'Today': {
                        'Hi': hi_today,
                        'Hourly': forecast_today
                    },
                    'Tomorrow': {
                        'Hi': hi_tomorrow,
                        'Hourly': forecast_tomorrow
                    }

                }
            }
            print(json.dumps(Forecast, indent=1))
            return Forecast

    def getAccuweatherForecast(self):
        dom_forecast_today = self.getHTML("https://www.accuweather.com/en/us/central-park/10028/hourly-weather-forecast/2627448")
        times = dom_forecast_today.xpath('//*[starts-with(@id, "hourlyCard")]/div[1]/div/div[1]/h2/span/text()')
        temps = dom_forecast_today.xpath('//*[starts-with(@id, "hourlyCard")]/div[1]/div/div[1]/div/text()')
        temps = [int(temp[0:-1]) for temp in temps]
        dom_forecast_tomorrow = self.getHTML("https://www.accuweather.com/en/us/central-park/10028/hourly-weather-forecast/2627448?day=2")
        times_tomorrow = dom_forecast_tomorrow.xpath('//*[starts-with(@id, "hourlyCard")]/div[1]/div/div[1]/h2/span/text()')
        temps_tomorrow = dom_forecast_tomorrow.xpath('//*[starts-with(@id, "hourlyCard")]/div[1]/div/div[1]/div/text()')
        temps_tomorrow = [int(temp[0:-1]) for temp in temps_tomorrow]

        hi_today = max(temps)
        hi_tomorrow = max(temps_tomorrow)

        
        Forecast = self.formatForecast(source="Accuweather", hi_today= hi_today, hi_tomorrow=hi_tomorrow, 
        times_today=times, temps_today=temps, times_tomorrow=times_tomorrow, temps_tomorrow=temps_tomorrow)

        return Forecast

    def getWeatherChannelForecast(self):
        dom = self.getHTML('https://weather.com/weather/hourbyhour/l/bfa50304b2d05006dc06fdbc0b4eca42b87a68024f0962d7063370d6d6bae1f1')
        temps = dom.xpath('//div[@class="HourlyForecast--DisclosureList--MQWP6"]/details/summary/div/div/div[@class="DetailsSummary--temperature--1kVVp"]/span/text()')
        times = dom.xpath('//div[@class="HourlyForecast--DisclosureList--MQWP6"]/details/summary/div/div/h3/text()')
        temps = [temp[0:-1] for temp in temps]
        forecast_today = []
        forecast_tomorrow = []
        times_today = []
        temps_today = []

        times_tomorrow = []
        temps_tomorrow = []
        isToday = True
        isTomorrow = False
        for i in range(len(temps)):
            if isToday:
                times_today.append(times[i])
                temps_today.append(temps[i])

                if times[i] == '11 pm':
                    isToday = False
                    isTomorrow = True
            elif isTomorrow:
                times_tomorrow.append(times[i])
                temps_tomorrow.append(temps[i])
                if times[i] == '11 pm':
                    isTomorrow = False

        hiDom = self.getHTML('https://weather.com/weather/today/l/bfa50304b2d05006dc06fdbc0b4eca42b87a68024f0962d7063370d6d6bae1f1')
        hi_today = hiDom.xpath('//*[@id="WxuDailyWeatherCard-main-bb1a17e7-dc20-421a-b1b8-c117308c6626"]/section/div/ul/li[1]/a/div[1]/span/text()')[0]
        hi_tomorrow = hiDom.xpath('//*[@id="WxuDailyWeatherCard-main-bb1a17e7-dc20-421a-b1b8-c117308c6626"]/section/div/ul/li[2]/a/div[1]/span/text()')[0]        

        hi_today = hi_today[:-1]
        hi_tomorrow = hi_tomorrow[:-1]

        Forecast = self.formatForecast(source="Weather Channel", 
            hi_today=hi_today, 
            hi_tomorrow=hi_tomorrow, 
            temps_today=temps_today, 
            temps_tomorrow=temps_tomorrow, 
            times_today=times_today, 
            times_tomorrow=times_tomorrow)
        
        return Forecast

    

    def getNWSForecast(self):
        dom = self.getHTML('https://forecast.weather.gov/MapClick.php?lat=40.78&lon=-73.97&lg=english&&FcstType=digital')
        times = dom.xpath('/html/body/table[6]/tr[3]/td/font/b/text()') + dom.xpath('/html/body/table[6]/tr[20]/td/font/b/text()')
        temps = dom.xpath('/html/body/table[6]/tr[4]/td/font/b/text()') + dom.xpath('/html/body/table[6]/tr[21]/td/font/b/text()')
        times = [time for time in times if time != 'Hour (EDT)']

        isToday = True
        isTommorrow = False
        
        temps_today = []
        times_today = []
        temps_tomorrow = []
        times_tomrrow = []
        for i in range(len(temps)):
            if isToday:
                temps_today.append(temps[i])
                times_today.append(times[i])
                if times[i] == '23':
                    isToday = False
                    isTommorrow = True

            elif isTommorrow:
                temps_tomorrow.append(temps[i])
                times_tomrrow.append(times[i])
                if times[i] == '23':
                    isTommorrow = False
            
        hiDom = self.getHTML('https://forecast.weather.gov/MapClick.php?lat=40.78&lon=-73.97#.ZBSk0-zMLtX')
        Highs = hiDom.xpath("""//*[@id="seven-day-forecast-list"]/li/div/p[starts-with(@class, 'temp')]/text()""")

        if 'Low:' in Highs[0]:
            hi_today = '-'
            hi_tomorrow = Highs[1]
            hi_tomorrow = hi_tomorrow.split()[1]

        
        #you are going to have to edit this one because there are more hi signals
        elif 'High:' in Highs[0]:
            hi_today = Highs[0]
            hi_tomorrow = Highs[2]
            hi_today = hi_today.split()[1]
            hi_tomorrow = hi_tomorrow.split()[1]



        Forecast = self.formatForecast(source="NWS", 
        hi_today=hi_today, 
        hi_tomorrow=hi_tomorrow, 
        times_today=times_today, 
        times_tomorrow=times_tomrrow,
        temps_today=temps_today,
        temps_tomorrow=temps_tomorrow)

        return Forecast


        #Forecast = self.formatForecast(source=)
    def uploadToDataBase(self, data, collection='forecast_data_knyc'):
        collection = self.db[collection]
        collection.insert_one(data)        

    def retrieveData(self, collection, query):
        collection = self.db[collection]
        results = collection.find(query)
        #for result in results:
        #    print(result)
        return results
    
    def retrieveDataSorted(self, collection, query, sort, limit):
        collection = self.db[collection]
        results = collection.find(query, sort=sort, limit=limit)
        #for result in results:
        #    print(result)
        return results

    def getAllForecasts(self):
        current_data = []
        current_data.append(self.getAccuweatherForecast())
        current_data.append(self.getWeatherChannelForecast())
        current_data.append(self.getNWSForecast())
        #add more as you get more

        result = {}
        for forecast in current_data:
            result[forecast['Source']] = forecast['Forecasts']
        print('result:',json.dumps(result))
        return result



    