import pandas as pd
import plotly.express as px
import pickle
def plot_tre_map(database:dict, cik : str):
    """
    Plot an interactive treemap of the last quarter's portfolio allocation,
    colored by each ticker's actual return over that quarter.

    - weights_history: dict with DataFrames of Ticker and pct_alloc
    - dates: dict of filing dates
    """
    #find the dabase
    d = database[cik]['f0'].copy()
    # Get tickers and allocations (exclude the last two items which are not tickers)
    tickers = [k for k in d.keys() if k not in ['stock_returns', 'filing_date']]
    pct_alloc = [d[t] for t in tickers]
    
    # Get stock returns and filter to only include tickers we have allocations for
    t = d['stock_returns']
    # Only keep tickers that exist in both allocation and returns data
    valid_tickers = [ticker for ticker in tickers if ticker in t.columns]
    
    # Filter allocations to only include valid tickers
    valid_allocations = [d[ticker] for ticker in valid_tickers]
    
    # Calculate performance only for valid tickers
    performance_last_trimester = []
    for stock in valid_tickers:
        series = t[stock].dropna()  # Drop NA values for safety
        if len(series) >= 2:
            start = series.iloc[0]  # First available price
            end = series.iloc[-1]   # Last available price
            performance = (end - start) / start if start != 0 else 0
            performance_last_trimester.append(performance)
        else:
            performance_last_trimester.append(0)  # Default to 0 if not enough data

    #data = d[d['Ticker'].notna()].reset_index(drop=True)
    #print(type(data))
    #print(data.columns)
    #print(data.tail(5))
    #find the performances
    #p = list()
    #lista_ticker = data['Ticker'].tolist()
    #lista_perf = data['pct_alloc'].tolist()
    #for i in lista_ticker:
    #  p.append(get_n_month_return(str(i), end=datetime.today(), start=dates['f0']))
    #data['performance'] = p
    #map colors based on performances
    #performances = list()

    #for s in data['performance'].tolist():
        # estrai l'indice (ticker) e il valore
    #    performances.append(s.iloc[0])
    # Create DataFrame with only valid tickers that exist in both allocation and returns data
    data = pd.DataFrame({
        'tickers': valid_tickers,
        'pct_alloc': valid_allocations,
        'color': performance_last_trimester
    })

    fig = px.treemap(
          data,
          path = ['tickers'],
          values= 'pct_alloc',
          color = "color",
          color_continuous_scale =  ["red", "green"],
          title="Stock Performance Treemap Interattivo"
    )
    fig.show()
if __name__ == '__main__':
    with open('database.pickle', 'rb') as data:
        d = pickle.load(data)
    plot_tre_map(d, '0001067983')
    
