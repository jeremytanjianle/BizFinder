
import calendar
import datetime
import json
import logging
import math
import re
import ssl
import threading
import urllib.request
import urllib.parse
from time import sleep, time
from queue import Queue
import pandas as pd
import numpy as np
import requests
import time
import os
os.listdir()

from geopy import Point
from geopy.distance import vincenty, VincentyDistance
import math
from scipy.signal import argrelextrema
from scipy.integrate import simps



class gmaps():
    """
    Pull Data from Google Maps. 
    
    Methods:
    -------
        get_raw(self,place_identifier)
        get_entity(self, place_identifier, visits = True)
        search_estab(self, ENTP_NM, headless = False, substr_filter = '')
        infer_ratio(self, last_week,next_week, plot = False)
        raw2weekly(idxed_raw, unique_id=None)
        weekly2monthly(self, weekly)
    """
    def __init__(self):
        self.hours = ['Monday00', 'Monday01', 'Monday02', 'Monday03', 'Monday04', 'Monday05', 'Monday06', 'Monday07', 
                     'Monday08', 'Monday09', 'Monday10', 'Monday11', 'Monday12', 'Monday13', 'Monday14', 'Monday15', 
                     'Monday16', 'Monday17', 'Monday18', 'Monday19', 'Monday20', 'Monday21', 'Monday22', 'Monday23', 
                     'Tuesday00', 'Tuesday01', 'Tuesday02', 'Tuesday03', 'Tuesday04', 'Tuesday05', 'Tuesday06', 'Tuesday07', 
                     'Tuesday08', 'Tuesday09', 'Tuesday10', 'Tuesday11', 'Tuesday12', 'Tuesday13', 'Tuesday14', 'Tuesday15', 
                     'Tuesday16', 'Tuesday17', 'Tuesday18', 'Tuesday19', 'Tuesday20', 'Tuesday21', 'Tuesday22', 'Tuesday23', 
                     'Wednesday00', 'Wednesday01', 'Wednesday02', 'Wednesday03', 'Wednesday04', 'Wednesday05', 'Wednesday06', 
                     'Wednesday07', 'Wednesday08', 'Wednesday09', 'Wednesday10', 'Wednesday11', 'Wednesday12', 'Wednesday13', 
                     'Wednesday14', 'Wednesday15', 'Wednesday16', 'Wednesday17', 'Wednesday18', 'Wednesday19', 'Wednesday20', 
                     'Wednesday21', 'Wednesday22', 'Wednesday23', 'Thursday00', 'Thursday01', 'Thursday02', 'Thursday03', 
                     'Thursday04', 'Thursday05', 'Thursday06', 'Thursday07', 'Thursday08', 'Thursday09', 'Thursday10', 
                     'Thursday11', 'Thursday12', 'Thursday13', 'Thursday14', 'Thursday15', 'Thursday16', 'Thursday17', 
                     'Thursday18', 'Thursday19', 'Thursday20', 'Thursday21', 'Thursday22', 'Thursday23', 'Friday00',
                     'Friday01', 'Friday02', 'Friday03', 'Friday04', 'Friday05', 'Friday06', 'Friday07', 'Friday08', 
                     'Friday09', 'Friday10', 'Friday11', 'Friday12', 'Friday13', 'Friday14', 'Friday15', 'Friday16', 
                     'Friday17', 'Friday18', 'Friday19', 'Friday20', 'Friday21', 'Friday22', 'Friday23', 'Saturday00', 
                     'Saturday01', 'Saturday02', 'Saturday03', 'Saturday04', 'Saturday05', 'Saturday06', 'Saturday07',
                     'Saturday08', 'Saturday09', 'Saturday10', 'Saturday11', 'Saturday12', 'Saturday13', 'Saturday14', 
                     'Saturday15', 'Saturday16', 'Saturday17', 'Saturday18', 'Saturday19', 'Saturday20', 'Saturday21',
                     'Saturday22', 'Saturday23', 'Sunday00', 'Sunday01', 'Sunday02', 'Sunday03', 'Sunday04', 'Sunday05',
                     'Sunday06', 'Sunday07', 'Sunday08', 'Sunday09', 'Sunday10', 'Sunday11', 'Sunday12', 'Sunday13', 'Sunday14', 
                     'Sunday15', 'Sunday16', 'Sunday17', 'Sunday18', 'Sunday19', 'Sunday20', 'Sunday21', 'Sunday22', 'Sunday23']
        pass
    
    def zfill_weekly_col(self, weekly):
        def zfill_weeks(string_col):
            if '_Week' not in string_col:
                return(string_col)
            elif '_Week' in string_col:
                weekno = string_col.split('_Week')[1].zfill(2)
                new_week = string_col.split('_Week')[0] + '_Week' + weekno
                return(new_week)
        weekly.columns = [zfill_weeks(col) for col in weekly.columns]
        return(weekly)
    
    def rephrase_nm(self, entp_nm):
        """
        Remove the company names and abbrieviations to increase search rate 
        on Google Maps
        """
        entp_nm = entp_nm.lower()
        substrings = ['ltd.', 'ltd', 'pte.', 'pte', 'private', 'limited', 
                      'holdings', 'holding', 'company', 'stores', 'store', '&']
        for substr in substrings:
            entp_nm = entp_nm.replace(substr, '')
        return(entp_nm.strip())

    def isdatetime(self, string):
        """Function returns Boolean Value for if it is a datetime or a datetimestring"""
        from dateutil import parser
        import datetime
        if (type(string) == datetime.datetime) or (type(string)==pd._libs.tslibs.timestamps.Timestamp):
            return(True)
        else:
            try:
                dt = parser.parse(string)
                return(True)
            except:
                return(False)


    def get_raw(self, place_identifier):
        USER_AGENT = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) "
                                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                                    "Chrome/54.0.2840.98 Safari/537.36"}

        params_url = {
            "tbm": "map",
            "tch": 1,
            "hl": "en",
            "q": urllib.parse.quote_plus(place_identifier),
            "pb": "!4m12!1m3!1d4005.9771522653964!2d-122.42072974863942!3d37.8077459796541!2m3!1f0!2f0!3f0!3m2!1i1125!2i976"
                  "!4f13.1!7i20!10b1!12m6!2m3!5m1!6e2!20e3!10b1!16b1!19m3!2m2!1i392!2i106!20m61!2m2!1i203!2i100!3m2!2i4!5b1"
                  "!6m6!1m2!1i86!2i86!1m2!1i408!2i200!7m46!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e3!2b0!3e3!"
                  "1m3!1e4!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e3!2b1!3e2!1m3!1e9!2b1!3e2!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e"
                  "10!2b0!3e4!2b1!4b1!9b0!22m6!1sa9fVWea_MsX8adX8j8AE%3A1!2zMWk6Mix0OjExODg3LGU6MSxwOmE5ZlZXZWFfTXNYOGFkWDh"
                  "qOEFFOjE!7e81!12e3!17sa9fVWea_MsX8adX8j8AE%3A564!18e15!24m15!2b1!5m4!2b1!3b1!5b1!6b1!10m1!8e3!17b1!24b1!"
                  "25b1!26b1!30m1!2b1!36b1!26m3!2m2!1i80!2i92!30m28!1m6!1m2!1i0!2i0!2m2!1i458!2i976!1m6!1m2!1i1075!2i0!2m2!"
                  "1i1125!2i976!1m6!1m2!1i0!2i0!2m2!1i1125!2i20!1m6!1m2!1i0!2i956!2m2!1i1125!2i976!37m1!1e81!42b1!47m0!49m1"
                  "!3b1"
        }

        search_url = "https://www.google.de/search?" + "&".join(k + "=" + str(v) for k, v in params_url.items())
        logging.info("searchterm: " + search_url)

        gcontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1)


        resp = urllib.request.urlopen(urllib.request.Request(url=search_url, data=None, headers=USER_AGENT),
                                      context=gcontext)
        data = resp.read().decode('utf-8').split('/*""*/')[0]

        jend = data.rfind("}")
        jdata = json.loads(data)["d"]
        jdata = json.loads(jdata[4:])

        def index_get(array, *argv):
            """
            checks if a index is available in the array and returns it
            :param array: the data array
            :param argv: index integers
            :return: None if not available or the return value
            """

            try:

                for index in argv:
                    array = array[index]

                return array

            # there is either no info available or no popular times
            # TypeError: rating/rating_n/populartimes wrong of not available
            except (IndexError, TypeError):
                return None

        info = index_get(jdata, 0, 1, 0, 14)
        return(info)




    def get_entity(self, place_identifier, visits = True):
        """
        Get entity details as well as visits data
        
        args:
        -----
            place_identifier: (str) name and address of place e.g. "Hard Rock Cafe Cuscaden Road"
            visits: (bool) if True, search for google visit hours,
                           else, only return the place details
        Return:
        ------
            entity_details: (pandas series) contains the entity details and raw footfall data
        """
        info = self.get_raw(place_identifier)

        def index_get(array, *argv):
            """
            utility function: checks if a index is available in the array and returns it
            :param array: the data array
            :param argv: index integers
            :return: None if not available or the return value
            """

            try:

                for index in argv:
                    array = array[index]

                return array

            # there is either no info available or no popular times
            # TypeError: rating/rating_n/populartimes wrong of not available
            except (IndexError, TypeError):
                return None


        def get_details(info):
            """
            info is the raw list that Google returns to us. 
            pull and parse useful information:
                2 - Address
                7 - website
                9[-2] - Lattitude
                9[-1] - Longitude
                11 - Place_name
                78 - Place_ID
            """
            details = pd.Series()
            details.loc['Address'] = None
            if info[2] is not None: details.loc['Address'] = ''.join([string+ ' ' for string in info[2] ]).strip()

            if type(info[7]) == list:
                details['Website'] = info[7][0]
            else:
                details['Website'] = np.nan

            try:
                details['Contact_number'] = int(info[3][0].replace(' ',''))
            except:
                details['Contact_number'] = None
                
            details['Lattitude'] = info[9][-2]
            details['Longitude'] = info[9][-1]

            details['Place_name'] = info[11]
            details['Place_ID'] = info[78]
            

            return(details)
        
        if info is not None:
            entity_details = get_details(info)
        else:
            return(None)
            
        if visits == False:
            return(entity_details)

        elif visits == True:
            try:
                """
                if visits == True:
                    format the hours and return a single data frame
                """
                popular_times = index_get(info, 84, 0)
        
                def get_popularity_for_day(popularity):
                    """
                    Returns popularity for day
                    :param popularity:
                    :return:
                    """
                    # Initialize empty matrix with 0s
                    pop_json = [[0 for _ in range(24)] for _ in range(7)]
                    wait_json = [[0 for _ in range(24)] for _ in range(7)]
        
                    for day in popularity:
        
                        day_no, pop_times = day[:2]
        
                        if pop_times:
                            for hour_info in pop_times:
        
                                hour = hour_info[0]
                                pop_json[day_no - 1][hour] = hour_info[1]
        
                                # check if the waiting string is available and convert no minutes
                                if len(hour_info) > 5:
                                    wait_digits = re.findall(r'\d+', hour_info[3])
        
                                    if len(wait_digits) == 0:
                                        wait_json[day_no - 1][hour] = 0
                                    elif "min" in hour_info[3]:
                                        wait_json[day_no - 1][hour] = int(wait_digits[0])
                                    elif "hour" in hour_info[3]:
                                        wait_json[day_no - 1][hour] = int(wait_digits[0]) * 60
                                    else:
                                        wait_json[day_no - 1][hour] = int(wait_digits[0]) * 60 + int(wait_digits[1])
        
                                # day wrap
                                if hour_info[0] == 23:
                                    day_no = day_no % 7 + 1
        
                    ret_popularity = [
                        {
                            "name": list(calendar.day_name)[d],
                            "data": pop_json[d]
                        } for d in range(7)
                    ]
        
                    # waiting time only if applicable
                    ret_wait = [
                        {
                            "name": list(calendar.day_name)[d],
                            "data": wait_json[d]
                        } for d in range(7)
                    ] if any(any(day) for day in wait_json) else []
        
                    # {"name" : "monday", "data": [...]} for each weekday as list
                    return( ret_popularity, ret_wait )
        
                visits_data = get_popularity_for_day(popular_times)
        
                def isNaN(x):
                    return(str(x).lower() == 'nan')
                def hours_from_day(day_dict):
                    """
                    processes raw data from Google Map
                    TODO: can be made faster with list comprehensions and zip
                    """
                    day_data = pd.Series()
                    for i in range(len(day_dict['data'])):
                        day_data.loc[day_dict['name']+str(i).zfill(2)] = day_dict['data'][i]
                    return(day_data)
        
        
                now = datetime.datetime.now()
                entity_details.loc['data_load_dt'] = str(now.date())
                entity_details.loc['data_load_year_wk'] = '{}_Week{}'.format(now.isocalendar()[0],now.isocalendar()[1])
        
                whole_week = pd.concat( [entity_details] + [hours_from_day(day_dict) for day_dict in visits_data[0]] )
                return(whole_week)
            except:
                return(entity_details)
                    

    def isElementPresent(self, driver, classname):
        """Is the element in the driver?"""
        from selenium.common.exceptions import NoSuchElementException
        try:
            driver.find_element_by_class_name(classname)
        except NoSuchElementException:
            return False
        return True
                  
                
    def search_estab(self, ENTP_NM, headless = False, substr_filter = '', try_=True):
        """
        Searches for establishment names using Enterprise Name
        
        args:
        ----
            ENTP_NM: (str) name of the enterprise to search
            headless: (bool) if True, show browser process. Default: False
            substr_filter: (str) further filter results based on required string
            
        return:
        -------
            list_of_entities: (list) list containing tuples of branch name and address    
        """
    
        # set up driver
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.keys import Keys
        from selenium.common.exceptions import InvalidElementStateException
        import time
        if headless:
            from selenium.webdriver.chrome.options import Options 
            chrome_options = Options()  
            chrome_options.add_argument("--headless")  
            driver = webdriver.Chrome(chrome_options=chrome_options)
        else:
            driver = webdriver.Chrome()
        url = 'https://www.google.com/maps'
        driver.get('https://www.google.com/maps')
        time.sleep(3)
        
        # input search text and press enter
        searchtext = driver.find_element_by_class_name('tactile-searchbox-input')
        searchtext.send_keys(ENTP_NM)
        search_button = driver.find_element_by_id('searchbox-searchbutton')
        time.sleep(3)
        searchtext.send_keys(Keys.RETURN)
        time.sleep(3)
    
        # section-weather-icon
        if self.isElementPresent(driver, 'section-weather-icon') or self.isElementPresent(driver, 'section-subheader-explanation-bubble'):
            return(None, driver)
    
        # search map results: only 1 entry 
        if self.isElementPresent(driver, 'section-hero-header-title'):# 'section-hero-header-directions-base'):
            entry = {}
            entry['name'] = driver.find_element_by_class_name('section-hero-header-title').text
            try:
                entry['address'] = [ele.text for ele in driver.find_elements_by_xpath('//*[@jsan="7.widget-pane-link"]') if len(ele.text) > 1][0]
            #entry['address'] = driver.find_element_by_xpath('//*[@id="pane"]/div/div[1]/div/div/div[6]/div/div[1]/span[3]/span[3]').text
            except IndexError:
                return(None, driver)
            
            entry_df = pd.DataFrame.from_dict(entry, orient='index').T
            driver.quit()
            return(entry_df, driver)
        
        try:
            # search map results: multiple entries
            list_of_entities = []
            for i in range(20):
                try: 
                    # store name and address of entry boxes
                    entities = driver.find_elements_by_class_name('section-result-text-content')
                    for entity in entities:
                        name = entity.find_element_by_class_name('section-result-title').text
                        address = entity.find_element_by_class_name('section-result-location').text
                        list_of_entities.append( (name,address) )
        
                    # get next 20 results
                    if self.isElementPresent(driver, 'n7lv7yjyC35__section-pagination-button-next'):
                        time.sleep(5)
                        next_button = driver.find_element_by_id('n7lv7yjyC35__section-pagination-button-next')
                        next_button.send_keys(Keys.RETURN)
                    else:
                        break
                    time.sleep(2)
        
                # this error is raised when the last paged is reached. its a good error
                except InvalidElementStateException:
                    searchtext.clear()
                    break
        
                except IndexError:
                    return(None, driver)
        
                # any other error is a problem with the code
                except Exception as e:
                    #raise
                    if try_ == True:
                        return(None, driver)
                    
            # filter out wrong results 
            list_of_entities = [entry for entry in list_of_entities if substr_filter.lower() in entry[0].lower()]
            driver.quit()
            list_of_entities = pd.DataFrame(list_of_entities, columns = ['name', 'address'])
            return(list_of_entities, driver)
        except:
            if try_ == True:
                return(None, driver)
            else:
                raise
        
    
    def infer_ratio(self, last_week,next_week, plot = False):
        """
        This infers the transition ratio of last_week to next_week
        by taking the peaks of regular work days (Mon-Thurs)
        
        Arguments: two weeks of data as a pandas Series
        Outputs: a tuple containing the transition ratio and the 
                 denormalized time series of next_week
        """
        index = next_week.iloc[5:115].index
        
        #add noise to dislodge 2-point local maximas 
        series_len = len(index)  # new line
        noise = 0.01* np.random.randn(series_len)  # new line
        last_weekdays = last_week.values[5:115] + noise # new term
        next_weekdays = next_week.values[5:115] + noise # new term
    
        peaks_last_week = np.hstack([argrelextrema(last_weekdays, np.greater)[0] ,(argrelextrema(last_weekdays, np.greater)[0]+1) ,(argrelextrema(last_weekdays, np.greater)[0]-1)])
        overlapping_peaks = peaks_last_week[np.isin(peaks_last_week, argrelextrema(next_weekdays, np.greater)[0])]
        overlapping_peaks = overlapping_peaks[last_weekdays[overlapping_peaks]>1] # new line: eliminates peaks amongst noise terms
        overlapping_peaks = overlapping_peaks[next_weekdays[overlapping_peaks]>1] 
        
        transition_ratio = (last_weekdays[overlapping_peaks] / next_weekdays[overlapping_peaks]).mean() # lastweek -> last_weekdays
        
        
        unnormalized_within_week = next_week*transition_ratio
        unnormalized_across_week = simps( unnormalized_within_week ) / simps( last_week ) 
        #display(unnormalized_within_week)
        
        # new block
        if plot == True:
            plt.figure(figsize = (12,6))
            plt.plot(last_weekdays, label = 'Last Week')
            plt.plot(next_weekdays, label = 'Next Week')
            
            plt.plot(overlapping_peaks, last_weekdays[overlapping_peaks], 'rx')
            plt.plot(overlapping_peaks, next_weekdays[overlapping_peaks], 'bx')
            
            plt.legend()
            plt.show()
            
        return(transition_ratio,unnormalized_across_week)
    
    def raw2weekly(self, idxed_raw, unique_id=None):
        """
        process and convert raw data into weekly footfall change
        Retain columns that are not part of the visits time series .e.g Place_ID, Address etc.
        
        args:
        -----
            idxed_raw: (pd.DataFrame) contains the raw data of the firm.
            unique_id: (str) used to name the output
            
        return:
        -------
            firm_weekly: (pd.Series) contains the weekly footfall with the retained firm details
        """
        # retain columns that are not part of the visits time series .e.g Place_ID, Address etc.
        firm_det = [column for column in idxed_raw.columns if column not in self.hours]
        firm_marker = idxed_raw.loc[:,firm_det].iloc[0]
        
        # trying to obtain a name to the output
        if unique_id==None:
            try:
                unique_id = firm_marker.loc['Place_ID']
            except:
                try:
                    unique_id = firm_marker.loc['ENTP_NM']
                except:
                    unique_id = firm_marker.iloc[0]
        
        # isolating single firm data and sorting by date
        firm_lvl_raw = idxed_raw.loc[:, ['data_load_dt'] + list(self.hours)]
        
        # if only 1 record, then Return None
        if type(firm_lvl_raw) == pd.Series:
            return(None)
        
        # get time series panel of raw data
        firm_lvl_raw.data_load_dt = pd.to_datetime(firm_lvl_raw.data_load_dt)
        firm_lvl_raw.drop_duplicates('data_load_dt', keep='first',inplace=True)
        firm_lvl_raw = firm_lvl_raw.set_index('data_load_dt').sort_index()
        firm_lvl_raw = firm_lvl_raw.sort_index()
        
        # narrow to weekly level
        weekly_dict = {}
        for i in range(1,len(firm_lvl_raw)):
            lastweek = firm_lvl_raw.iloc[i-1,:].loc[self.hours].apply(float)
            nextweek = firm_lvl_raw.iloc[i,:].loc[self.hours].apply(float)
            
            trans, delta_footfall = self.infer_ratio(lastweek, nextweek)
            weekly_dict[firm_lvl_raw.index[i]] = delta_footfall
        firm_weekly = pd.Series(weekly_dict)
        firm_weekly = firm_marker.append(firm_weekly)
        firm_weekly.name = unique_id
        return(firm_weekly)
        
        
    def weekly2monthly(self, weekly):
        """
        compile weekly freq to monthly freq
        retains non-datetime indices e.g. ENTP_NM, Place_ID, Address
        
        args:
        -----
            weekly: (pd.Series)
            
        return:
        ------
            monthly: (pd.Series) 
        """    
        
        import datetime
        import numpy as np
    
        assert type(weekly) == pd.Series
        def loadweek2date(loadweek):
            import datetime
            return(datetime.datetime.strptime(loadweek+'-1', '%Y_Week%W-%w'))
        proper_index = []
        for idx in weekly.index:
            try:
                datetimeobject = loadweek2date(idx)
                proper_index.append(datetimeobject)
            except Exception as e:
                proper_index.append(idx)
        weekly.index = proper_index
    
        firm_det = [idx for idx in weekly.index if not self.isdatetime(idx)]
        firm_weekly = [idx for idx in weekly.index if self.isdatetime(idx)]
        
        dets = weekly.loc[firm_det]
        vsts = weekly.loc[firm_weekly]
        
        # cumulative product of delta change
        vsts = (vsts+1).groupby(pd.Grouper(freq='MS')).prod() - 1
        
        # format to replace 0 with Nans
        vsts = vsts.replace(0,np.NaN)
        vsts = (vsts*100).round(2)
        
        monthly = pd.concat([dets,vsts], axis=0)
        return(monthly)