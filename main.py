import os
from selenium import webdriver
import requests
import json
from selenium.webdriver.chrome.options import Options
import pandas as pd
from time import sleep
from datetime import datetime, time, timedelta
import numpy as np

options = Options()
options.add_argument('--headless')

def get_session_cookies():
    currentdir = os.path.dirname(__file__)
    driver = webdriver.Chrome(executable_path=os.path.join(currentdir, "chromedriver"), options=options)
    driver.get("https://www.nseindia.com/option-chain")
    cookies = driver.get_cookies()
    cookie_dict = {}
    with open('cookies', 'w') as line:
        for cookie in cookies:
            cookie_dict[cookie['name']] = cookie['value']
        line.write(json.dumps(cookie_dict))
    driver.quit()
    return cookie_dict


url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
df_list = []
oi_filename = os.path.join("Files", "oi_record_{0}.json".format(datetime.now().strftime("%d%m%y")))

headers = {

}
cookie_dict = get_session_cookies()
session = requests.session()

for cookie in cookie_dict:
    session.cookies.set(cookie, cookie_dict[cookie])

def fetch_oi(df):
    tries = 1
    max_retries = 3
    while tries <= max_retries:
        try:
            r = session.get(url, headers=headers).json()
            with open("oidata.json", "w") as files:
                files.write(json.dumps(r, indent=4, sort_keys=True))
            ce_values = [data['CE'] for data in r['filtered']['data'] if "CE" in data]
            pe_values = [data['PE'] for data in r['filtered']['data'] if "PE" in data]
            ce_data = pd.DataFrame(ce_values)
            pe_data = pd.DataFrame(pe_values)
            ce_data['type'] = "CE"
            pe_data['type'] = "PE"
            df1 = pd.concat([ce_data, pe_data])
            if len(df_list) > 0:
                df1['Time'] = df_list[-1][0]['Time']
            if len(df_list) > 0 and df1.to_dict('records') == df_list[-1]:
                print("Duplicate data.")
                sleep(10)
                tries += 1
                continue
            df1['Time'] = datetime.now().strftime("%H:%M")
            if not df.empty:
                df=df[
                    ["Time", "askPrice", "askQty", "bidQty", "bidprice", "change", "changeinOpenInterest", "expiryDate",
                     "identifier", "impliedVolatility", "lastPrice", "openInterest", "pChange", "pchangeinOpenInterest",
                     "strikePrice", "totalBuyQuantity", "totalSellQuantity", "totalTradedVolume", "type", "underlying",
                     "underlyingValue"]
                ]
                df1 = df1[
                    ["Time", "askPrice", "askQty", "bidQty", "bidprice", "change", "changeinOpenInterest", "expiryDate",
                     "identifier", "impliedVolatility", "lastPrice", "openInterest", "pChange", "pchangeinOpenInterest",
                     "strikePrice", "totalBuyQuantity", "totalSellQuantity", "totalTradedVolume", "type", "underlying",
                     "underlyingValue"]
                ]
            df = pd.concat([df, df1])
            df_list.append(df1.to_dict('records'))
            with open(oi_filename, "w") as files:
                files.write(json.dumps(df_list, indent=4, sort_keys=True))
            return df1
        except Exception as error:
            print("error {0}".format(error))
            tries +=1
            sleep(10)
            continue
    if tries >= max_retries:
        print("Max retries exceeded {0}".format(datetime.now()))
        return df

def main():
    global df_list
    try:
        df_list = json.loads(open(oi_filename).read())
    except Exception as error:
        print(("Error recording data. Error: {0}".format(error)))
        df_list = []
    timeframe = 2
    if df_list:
        df = pd.DataFrame()
        for item in df_list:
            df = pd.concat([df, pd.DataFrame(item)])
    else:
        df = pd.DataFrame()
    while time(9,15) <= datetime.now().time() <= time(15,30):
        timenow = datetime.now()
        check = True if int(timenow.minute)/timeframe in list(np.arange(0.0,30.0)) else False
        if check:
            nextscan = timenow+ timedelta(minutes=timeframe)
            df =fetch_oi(df)
            if not df.empty:
                waitsecs = int((nextscan - datetime.now()).seconds)
                print("Wait for {0} seconds".format(waitsecs))
                sleep(waitsecs) if waitsecs >0 else sleep(0)
            else:
                print("NO data received")
                sleep(10)

if __name__ == "__main__":
    main()
