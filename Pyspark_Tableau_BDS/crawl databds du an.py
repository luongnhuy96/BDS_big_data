# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 10:28:55 2023

@author: yln
"""

#%%new
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
dflink=[]
dflink2=[]
'''
listquan=['quan-1','quan-2','quan-3','quan-4','quan-5','quan-6','quan-7'
          ,'quan-8','quan-9','quan-10','quan-11','quan-12','quan-binh-tan',
          'quan-binh-thanh','quan-go-vap','quan-phu-nhuan','quan-tan-binh',
          'quan-tan-phu','quan-thu-duc','huyen-binh-chanh','huyen-can-gio',
          'huyen-cu-chi','huyen-hoc-mon','huyen-nha-be']
'''
listquan=['quan-thu-duc']

for h in listquan:
    for i in range (1,90):
        print("so i la",i)
        #link='https://mogi.vn/ho-chi-minh/'+h+'/mua-nha-mat-tien-pho?cp='+i+"'"
        link = f'https://mogi.vn/ho-chi-minh/{h}/mua-nha-mat-tien-pho?cp={i}'
     
        # Initialize web driver
        service = Service(executable_path=r"C:\Users\ASUS VN\Downloads\F2 FTMS lecture notes\F2 Video\chromedriver-win32\chromedriver-win32\chromedriver.exe")
        driver = webdriver.Chrome(service=service)
        
        # Navigate to the URL
        driver.get(link)
        
        # Find the ul element with the class 'props'
        myDiv = driver.find_element(By.XPATH, "//ul[@class='props']")
        
        # Find all li elements within the ul element
        all_li = myDiv.find_elements(By.TAG_NAME, "li")
        
        # Create empty lists to store text, href, and attribute values
        texts = []
        hrefs = []
        squares = []
        beds = []
        bathrooms = []
        prices = []
        
        # Iterate over the li elements
        for li in all_li:
            # Retrieve text attribute
            text = li.text
            
            try:
                # Retrieve href attribute if the 'a' tag element exists
                href = li.find_element(By.TAG_NAME, "a").get_attribute("href")
            except NoSuchElementException:
                # Handle the exception by providing a default value for href
                href = ""
            
            try:
                # Retrieve square, bed, and bathroom attributes from 'ul' with class 'prop-attr'
                prop_attr = li.find_element(By.XPATH, ".//ul[@class='prop-attr']")
                square = prop_attr.find_element(By.XPATH, ".//li[1]").text
                bed = prop_attr.find_element(By.XPATH, ".//li[2]").text
                bathroom = prop_attr.find_element(By.XPATH, ".//li[3]").text
            except NoSuchElementException:
                # Handle the exception by providing default values for attributes
                square = ""
                bed = ""
                bathroom = ""
            
            try:
                # Retrieve price if the 'div' element with class 'price' exists
                price = li.find_element(By.CLASS_NAME, "price").text
            except NoSuchElementException:
                # Handle the exception by providing a default value for price
                price = ""
            
            # Append values to the respective lists
            texts.append(text)
            hrefs.append(href)
            squares.append(square)
            beds.append(bed)
            bathrooms.append(bathroom)
            prices.append(price)
        
        # Create a DataFrame from the lists
        df = pd.DataFrame({
            "Text": texts,
            "Href": hrefs,
            "Square": squares,
            "Bed": beds,
            "Bathroom": bathrooms,
            "Price": prices,
            "Using Square": "",
            "Land Square": "",
            "Length": "",
            "Width": "",
            "Legal": "",
            "Post Date": "",
            "BDS Code": ""
        })
        
        dflink.append(df)
               
    dflink2=pd.concat(dflink)
#%% specific web
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import pandas as pd

# Initialize web driver
service = Service(executable_path=r"C:\Users\ASUS VN\Downloads\F2 FTMS lecture notes\F2 Video\chromedriver-win32\chromedriver-win32\chromedriver.exe")
driver = webdriver.Chrome(service=service)
list_web = pd.DataFrame(dflink2['Href'].unique(),columns=['Href'])
list_web2=list_web[list_web['Href'] != ''].reset_index(drop=True)
#list_web2.to_csv(r"D:\databds\listhref_quan2.csv")
#list_web2=pd.read_csv(r"D:\databds\listhref_quan2.csv")
#list_web3=list_web2[1290:]
dataweb=[]
for i in list_web2['Href']:
    print(i)
# Navigate to the URL
#url = "https://mogi.vn/quan-1/mua-nha-mat-tien-pho/gia-so-c-80-ty-ba-n-mt-ngang-khu-ng-13-18m-gpxd-2-ha-m-12-ta-ng-id22099112"
    driver.get(i)
    
    # Extract information
    data = {}
    data['href'] = str(i)
    

    # Extract price
    try:
        price = driver.find_element(By.CLASS_NAME, "price").text
        data['Price'] = price
    except NoSuchElementException:
        data['Price'] = None
        
    
    # Extract address
    try:
        address = driver.find_element(By.CLASS_NAME, "address").text
        data['Address'] = address
    except NoSuchElementException:
        data['Address'] = None
    
    # Extract using square
    try:
        using_square = driver.find_element(By.XPATH, "//span[contains(text(), 'Diện tích sử dụng')]/following-sibling::span").text
        data['Using Square'] = using_square
    except NoSuchElementException:
        data['Using Square'] = None
    
    # Extract land square
    try:
        land_square = driver.find_element(By.XPATH, "//span[contains(text(), 'Diện tích đất')]/following-sibling::span").text
        index=land_square.find('m2')
        data['Land Square'] = land_square[:index].strip('m2')
        
        #data['Land Square'] = land_square
    except NoSuchElementException:
        data['Land Square'] = None
    
    # Extract length
    try:
        length = driver.find_element(By.XPATH, "//span[contains(text(), 'Diện tích đất')]/following-sibling::span").text
        index=length.find('x')
        data['Length'] = (length[index:].strip('x')).strip(')')

    except NoSuchElementException:
        data['Length'] = None
    
    # Extract width
    try:
        width = driver.find_element(By.XPATH, "//span[contains(text(), 'Diện tích đất')]/following-sibling::span").text.split('x')[0].strip()
        index=width.find('(')
        #data['Width'] = width
        data['Width'] = width[index:].strip('(')
        
    except NoSuchElementException:
        data['Width'] = None
        
    # Extract phòng ngủ
    try:
        bednum = driver.find_element(By.XPATH, "//span[contains(text(), 'Phòng ngủ')]/following-sibling::span").text
        #index=width.find('(')
        data['Bed_num'] = bednum
        
    except NoSuchElementException:
        data['Bed_num'] = None

    # Extract phòng tắm
    try:
        bathnum = driver.find_element(By.XPATH, "//span[contains(text(), 'Nhà tắm')]/following-sibling::span").text
        data['Bath_num'] = bathnum
        
    except NoSuchElementException:
        data['Bath_num'] = None
        
    # Extract legal
    try:
        legal = driver.find_element(By.XPATH, "//span[contains(text(), 'Pháp lý')]/following-sibling::span").text
        data['Legal'] = legal
    except NoSuchElementException:
        data['Legal'] = None
    
    # Extract post date
    try:
        post_date = driver.find_element(By.XPATH, "//span[contains(text(), 'Ngày đăng')]/following-sibling::span").text
        data['Post Date'] = post_date
    except NoSuchElementException:
        data['Post Date'] = None
    
    # Extract BDS code
    try:
        bds_code = driver.find_element(By.XPATH, "//span[contains(text(), 'Mã BĐS')]/following-sibling::span").text
        data['BDS Code'] = bds_code
    except NoSuchElementException:
        data['BDS Code'] = None
    
    # Extract image URL
    try:
        image_element = driver.find_element(By.XPATH, "//div[@class='media-item']/img")
        image_url = image_element.get_attribute("src")
        data['image_url'] = image_url
    except NoSuchElementException:
        data['image_url'] = None
    # Create dataframe
    
    dataweb.append(data)
# Close the web driver
driver.quit()

# Print the dataframe
df2 = pd.DataFrame(dataweb)

df2[['Ten_duong', 'Ten_phuong','Ten_Quan','Ten_TP']] = df2['Address'].str.split(',', expand=True)

df2.to_excel(r"D:\databds\databds"+h+".xlsx", encoding='utf8')
#%%
#df11=pd.DataFrame()
#df11.append(df2)
#%%
#dffull=pd.concat(df11)
df2.to_excel(r"D:\databds\databds"+h+".xlsx", encoding='utf8')
#df2.to_excel(r"D:\databds\databds"+"quan-2"+".xlsx", encoding='utf8')

