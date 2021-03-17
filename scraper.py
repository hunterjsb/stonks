from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import os
import pandas as pd
import time


def trend_cleaner(filepath):
    trends = pd.read_csv(filepath, header=1, parse_dates=['Week'])
    trends.set_index('Week', inplace=True)

    for col in trends:
        trends[col] = trends[col].apply(lambda x: 0.5 if x == '<1' else int(x))
        new_col_name = col.split(':')[0].upper()
        trends.rename(columns={col: new_col_name}, inplace=True)
    trends.to_csv(r'C:\Users\18435\PycharmProjects\elbonk\csv\multiTimeline_new.csv')


class Scraper:
    TRENDS_FP = r'C:\Users\18435\PycharmProjects\elbonk\csv\multiTimeline.csv'

    def __init__(self):
        self.endpoints = []
        self.driver = webdriver.Chrome(ChromeDriverManager().install())

    @staticmethod
    def _add_queries(endpoint: str, queries: dict):
        """for decorating a URL with PHP query requests."""
        i = 0
        for k, v in queries.items():
            i += 1
            c = "?" if i < 2 else "&"
            endpoint += f"{c}{k}={v}"

        return endpoint

    def open_trends(self, *args):
        DL_FP = r'C:\Users\18435\Downloads\multiTimeline.csv'
        concatenated = False
        tickers = args  # make tuple in to comma separated list
        ticker_bucket = [','.join(tickers[i:i+5]) for i in range(0, len(tickers), 5)]

        for q in ticker_bucket:
            if os.path.isfile(DL_FP):
                os.remove(DL_FP)

            self.driver.get('https://trends.google.com')  # load the web page first... something about a cookie 429...
            url = self._add_queries(f'https://trends.google.com/trends/explore', {'geo': 'US', 'q': q})
            self.driver.get(url)
            time.sleep(2)
            self.driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/md-content/div/div/div[1]/trends-widget/ng-'
                                              'include/widget/div/div/div/widget-actions/div/button[1]').click()
            time.sleep(1)

            if os.path.isfile(DL_FP):
                trend_cleaner(DL_FP)
                new_trends = pd.read_csv(r'C:\Users\18435\PycharmProjects\elbonk\csv\multiTimeline_new.csv')
                old_trends = pd.read_csv(Scraper.TRENDS_FP)
                concatenated = pd.merge(old_trends, new_trends)
            concatenated.to_csv(Scraper.TRENDS_FP)
            print('concatenated dataframes.')
        remove_false_index()
        self.driver.close()

    def google(self, value: str):
        search_string = value.replace(' ', '+')
        self.driver.get(f"https://www.google.com/search?q=" + search_string)

    def ed_scan(self, php_queries: dict):
        endpoint = f'https://finviz.com/screener.ashx'
        endpoint = self._add_queries(endpoint, php_queries)
        self.driver.get(endpoint)
        text = self.driver.find_element_by_xpath('//*[@id="screener-content"]/table/tbody/tr[4]/td/table/tbody').text
        return text.split('\n')


def remove_false_index():
    trends_dirty = pd.read_csv(Scraper.TRENDS_FP, index_col="Week")
    for col in trends_dirty:
        if 'Unnamed' in col:
            trends_dirty.drop(col, axis=1, inplace=True)
    trends_dirty.to_csv(Scraper.TRENDS_FP)


if __name__ == "__main__":
    remove_false_index()
