import requests
import pandas as pd
import psycopg2
from datetime import datetime
from prefect import Flow, task


class Crypto():
    def __init__(self):
        self.s = requests.Session()

    def get_assets(self):
        url = 'https://api.coincap.io/v2/assets'
        return requests.get(url).json()

    def get_rates(self):
        url = 'https://api.coincap.io/v2/rates'
        return requests.get(url).json()

    def get_exchanges(self):
        url = 'https://api.coincap.io/v2/exchanges'
        return requests.get(url).json()

    def get_markets(self):
        url = 'https://api.coincap.io/v2/markets'
        return requests.get(url).json()


@task
def load_data():
    cryp = Crypto()
    rates = cryp.get_rates()
    asset = cryp.get_assets()
    exchange = cryp.get_exchanges()
    market = cryp.get_markets()
    return rates, asset, exchange, market


# creating dataframes for each
@task
def transform_data(rates, asset, exchange, market):
    rates_df = pd.DataFrame(rates['data'])
    asset_df = pd.DataFrame(asset['data'])
    exchange_df = pd.DataFrame(exchange['data'])
    market_df = pd.DataFrame(market['data'])

    # dropping columns and changing datatypes

    rates_df = rates_df.astype({
        "rateUsd": "float"
    })
    asset_df = asset_df.astype({
        "rank": "int",
        "supply": "float",
        "maxSupply": "float",
        "marketCapUsd": "float",
        "volumeUsd24Hr": "float",
        "priceUsd": "float",
        "changePercent24Hr": "float",
    })
    asset_df = asset_df.drop(columns=["vwap24Hr", "explorer"])

    exchange_df = exchange_df.astype({
        "rank": "int",
        "percentTotalVolume": "float",
        "volumeUsd": "float",
        "tradingPairs": "int"
    })
    exchange_df = exchange_df.drop(columns=["socket", "exchangeUrl", "updated"])

    market_df = market_df.astype({
        "rank": "int",
        "priceQuote": "float",
        "priceUsd": "float",
        "volumeUsd24Hr": "float",
        "percentExchangeVolume": "float",
        "tradesCount24Hr": "float"
    })
    market_df = market_df.drop(columns=["updated"])

    asset_df['date'] = datetime.today()
    market_df['date'] = datetime.today()
    rates_df['date'] = datetime.today()
    exchange_df['date'] = datetime.today()
    return asset_df, market_df, rates_df, exchange_df


@task
def insert_data_into_postgres(asset_df, market_df, rates_df, exchange_df):
    conn = psycopg2.connect(database="stock", user="postgres", password="Booshan@2001", host="localhost", port="5432")

    cursor = conn.cursor()

    import psycopg2.extras as extras

    asset_tuples = [tuple(x) for x in asset_df.to_numpy()]
    asset_cols = ','.join(list(asset_df.columns))
    asset_query = "INSERT INTO asset(%s) VALUES %%s ON CONFLICT DO NOTHING" % asset_cols
    extras.execute_values(cursor, asset_query, asset_tuples)

    market_tuples = [tuple(x) for x in market_df.to_numpy()]
    market_cols = ','.join(list(market_df.columns))
    market_query = "INSERT INTO market(exchangeid,rank,basesymbol,baseid,quotesymbol,quoteid,pricequote,priceusd,volumeusd24hr,percentexchangevolume,tradescount,date) VALUES %s ON CONFLICT DO NOTHING"
    extras.execute_values(cursor, market_query, market_tuples)

    rates_tuples = [tuple(x) for x in rates_df.to_numpy()]
    rates_cols = ','.join(list(rates_df.columns))
    rates_query = "INSERT INTO rates(id,symbol,currencysymbol,type,rateusd,date) VALUES %s ON CONFLICT DO NOTHING"
    extras.execute_values(cursor, rates_query, rates_tuples)

    exchange_tuples = [tuple(x) for x in exchange_df.to_numpy()]
    exchange_cols = ','.join(list(exchange_df.columns))
    exchange_query = "INSERT INTO exchange(exchangeid,name,rank,percenttotalvolume,volumeusd,tradingpairs,date) VALUES %s ON CONFLICT DO NOTHING"
    extras.execute_values(cursor, exchange_query, exchange_tuples)

    conn.commit()


# calling api and storing json in variables
@Flow(name="cryptoflow", log_prints=True)
def crypto_flow():
    print("hello")
    rates, asset, exchange, market = load_data()
    rates_df, asset_df, exchange_df, market_df = transform_data(rates, asset, exchange, market)
    insert_data_into_postgres(asset_df, market_df, rates_df, exchange_df)


if __name__ == "__main__":
    print("one")
    crypto_flow.serve(name="first-deployment")
