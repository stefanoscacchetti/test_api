

import pandas as pd
#try:
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import os
'''except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Il pacchetto 'alpaca-py' non è installato. "
        "Installa con 'pip install alpaca-py pandas'"
    )'''

#qua inriamo le chiavi api di alpaca per il download dei dati
API_KEY_ALPACA = 'PK6FOT3K6588126LLQFV'
SECRET_API_KEY_ALPACA = 'rOG6npoJBTSPVJsag9teZnrHME1bzLtubqEnmYdP'
def get_close_prices_dataframe(api_key: str, api_secret: str, tickers: list, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Recupera i prezzi di chiusura giornalieri per i ticker specificati
    utilizzando l'Alpaca Market Data API e restituisce un DataFrame con
    date come indice e ticker come colonne.

    Parameters:
    - api_key: API Key for Alpaca
    - api_secret: Secret Key for Alpaca
    - tickers: List of ticker symbols (e.g., ["AAPL", "TSLA"])
    - start_date: Data di inizio (YYYY-MM-DD)
    - end_date: Data di fine (YYYY-MM-DD)

    Returns:
    - DataFrame pandas con indice date e colonne ticker corrispondenti ai prezzi di chiusura.
    """

    # Inizializza il client storico
    client = StockHistoricalDataClient(api_key, api_secret)

    # Prepara la richiesta
    request_params = StockBarsRequest(
        symbol_or_symbols=tickers,
        timeframe=TimeFrame.Day,
        start=start_date,
        end=end_date
    )

    # Esegui la chiamata
    bars = client.get_stock_bars(request_params)


    # Organizza i dati in un dizionario
    #check the tickers to remove
    flag = True
    while flag:
        flag = False
        for sym in tickers:
            try:
                for bar in bars[sym]:
                    continue
            except:
                flag = True
                tickers.remove(sym)

    data = {
        sym: [(bar.timestamp.date(), bar.close) for bar in bars[sym]]
        for sym in tickers
    }

    # Costruisci DataFrame per ciascun ticker
    df_list = []
    for sym, values in data.items():
        df_sym = pd.DataFrame(values, columns=["date", sym]).set_index("date")
        df_list.append(df_sym)

    # Concatena i DataFrame su colonne, allineando per date
    df_close = pd.concat(df_list, axis=1)

    return df_close

# Esempio d'uso:
if __name__ == "__main__":

#API_KEY = os.getenv("APCA_API_KEY_ID", "<YOUR_API_KEY>")
#API_SECRET = os.getenv("APCA_API_SECRET_KEY", "<YOUR_SECRET_KEY>")
    tickers = ["TSLA", "AAPL", "AMZN", "META", "RANDOM","ciao"]
    df = get_close_prices_dataframe(API_KEY_ALPACA, SECRET_API_KEY_ALPACA, tickers, "2025-05-01", "2025-05-15")
    print(df.head())
    print('ciao')