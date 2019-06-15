import pandas as pd
import selenium
import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

class WIPO():
    def __init__(self, headless = False):
        from selenium.webdriver.chrome.options import Options  
        chrome_options = Options()  
        chrome_options.add_argument("--headless")  
        if not headless: chrome_options=None
        self.browser = webdriver.Chrome(chrome_options=chrome_options)
        self.browser.get('https://www.wipo.int/branddb/')
        
    def format_entp_nm(self, entp_nm):
        """
        Remove company abbrievs
        """
        entp_nm = entp_nm.lower()

        # pte ltd
        entp_nm = entp_nm.replace(' pte.', '')
        entp_nm = entp_nm.replace(' pte', '')
        entp_nm = entp_nm.replace(' ltd.', '')
        entp_nm = entp_nm.replace(' ltd', '')

        # and, &
        entp_nm = entp_nm.replace('&', 'and')

        # Singapore
        entp_nm = entp_nm.replace(' of singapore', '')
        entp_nm = entp_nm.replace(' singapore', '')

        # CAPITAL RESOURCES
        entp_nm = entp_nm.replace(' capital resources', '')

        # REMOVE PARENTHESIS
        import re
        entp_nm = re.sub(r" ?\([^)]+\)", "", entp_nm)
        return(entp_nm.strip())
    
    def holdingcoy2brand(self, entp_nm):
        """
        Search https://www.wipo.int/branddb/ for brands to the company
        searches 1st page of results only.
        """
        # format the name
        entp_nm = self.format_entp_nm(entp_nm)

        # input the entp_nm and country
        self.browser.find_element_by_xpath('//a[@href="#name_search"]').click()
        self.browser.find_element_by_id('HOL_input').send_keys(entp_nm)
        time.sleep(0.05)
        self.browser.find_element_by_xpath('//a[@href="#country_search"]').click()
        self.browser.find_element_by_id('OO_input').send_keys('SG')
        time.sleep(1.6)

        # press the search button
        self.browser.find_element_by_link_text('search').click()
        time.sleep(2)

        # collect the data from first page
        collection = {}
        for id_ in range(30):
            try:
                table_row = self.browser.find_element_by_id(str(id_))
                brandname = table_row.find_elements_by_css_selector('td')[6].text.lower()
                holding_coy = table_row.find_elements_by_css_selector('td')[11].text.lower()
                holding_coy = self.format_entp_nm(holding_coy)
                collection[brandname] = holding_coy
            except NoSuchElementException:
                pass

        try:
            # form a neat dataframe 
            collection = pd.DataFrame.from_dict(collection, orient='index').reset_index()
            collection.columns = ['WIPO_Brand', 'Holding_Coy']

            # filter out those with names and verbal brands
            collection = collection[collection.Holding_Coy.str.contains(entp_nm)]
            collection = collection[~collection.WIPO_Brand.str.contains("no verbal elements")]
        except:
            print(entp_nm)
            print(collection)

        #time.sleep(0.5)
        self.browser.refresh()
        time.sleep(0.1)
        return(collection)