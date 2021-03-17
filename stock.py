import json
from os import path
import requests
import pandas
import datetime as dt
import time


# CONSTANTS
KEY = '1XlatBic3ePcDyKxiqG1D0nVWp0i6hcm'  # polygon.io API key
DATE = dt.datetime.today().strftime('%Y-%m-%d')
BIOTECH_STOCKS = ['TTNP', 'DXCM', 'NVAX', 'PDEX', 'VRTX', 'NBIX', 'INFU', 'CODX', 'GDRX', 'FLGT', 'PODD', 'NVCR']
SPACS = ['GHIV', 'THCB', 'STPK', "PCPL", 'IPOD', 'IPOE', 'GOEV', 'SFVA', "CCIV", "OCA", "OMEG", "FUSE"]
MEME_STOCKS = ['GME', 'AMC', 'NOK', 'BB', 'TSLA']

# VARS
t_req = 0  # total requests
today = dt.datetime.today().strftime('%Y-%m-%d')
two_years_ago = (dt.datetime.today() - dt.timedelta(365 * 2)).strftime('%Y-%m-%d')


# call this function every time a request is made to avoid exceeding the rate limit
def rate_limit():
    global t_req
    t_req += 1
    if t_req % 5 == 0:
        print('RATE LIMIT MET OR EXCEEDED | SLEEPING FOR SIXTY SECONDS')
        time.sleep(60)


class Stock:

    RUNNING_IN = 'root'  # default running outside of vehicles folder

    def __init__(self, ticker: str):
        self.ticker = ticker
        self.dots = '.' if Stock.RUNNING_IN == 'root' else '..'  # for contextual file paths

    # get aggregate data (bars) for a stock up to two years ago
    def agg_data(self, download: bool, multiplier=1, timespan='day', start=two_years_ago, end=today,
                 unadjusted='false', limit=5000):

        # REQUEST FROM POLYGON API
        if download:
            resp = requests.get(
                f'https://api.polygon.io/v2/aggs/ticker/{self.ticker}/range/{multiplier}/{timespan}/{start}/{end}?'
                f'unadjusted={unadjusted}&sort=asc&limit={limit}&apiKey={KEY}').json()
            print('MADE REQUEST')

            # SAVE THAT SHIT
            with open(f'{self.dots}/cache/stocks/{self.ticker}.json', 'w') as f:
                print('SAVED ' + self.ticker)  # DEBUG
                json.dump(resp, f, indent=4)
            rate_limit()

        # OR JUST OPEN IF DESIRED (WILL THROW ERROR IF DNE)
        else:
            with open(f'{self.dots}/cache/stocks/{self.ticker}.json') as f:
                resp = json.load(f)

        return resp

    # checks to see if there is saved data about the ticker, returns boolean
    def has_existing_data(self):
        return path.exists(f'{self.dots}/cache/stocks/{self.ticker}.json')


class LocalStock(Stock):  # for when you KNOW you have the data
    def __init__(self, ticker):
        super().__init__(ticker)

        # TRY TO OPEN WHAT WE HAVE, ELSE DOWNLOAD IT
        try:
            dat = self.agg_data(False)  # locally, regardless of timeframe
        except FileNotFoundError:
            dat = self.agg_data(True)  # download from two years ago today

        # CONSTRUCT A PANDAS TIME-SERIES AND SOME INSTANCE ATTR'S
        self.adjusted = dat['adjusted']
        self.count = dat['resultsCount']
        df = pandas.DataFrame(dat['results'])
        self.timeline = df.set_index(pandas.to_datetime(df['t'], unit='ms'))  # convert UNIX timestamps

    # returns price change over two dates
    def return_over(self, start: str, end: str):
        return float((self.timeline.at[end, 'c'] - self.timeline.at[start, 'o']) / self.timeline.at[start, 'o'])

    def compare_to_sub(self, subreddit='wallstreetbets'):  # other options are stocks, options, robinhood

        with open(f'{self.dots}/cache/reddit/{subreddit}.csv') as f:
            sub_df = pandas.read_csv(f, sep=';', parse_dates=True)
        sub_df.set_index('date', inplace=True)

        joint_df = self.timeline.join(sub_df, how='outer')
        return joint_df


if __name__ == "__main__":
    Stock.RUNNING_IN = 'main'  # so it knows what to reference
    s = LocalStock('GME')
    jdf = s.compare_to_sub()
