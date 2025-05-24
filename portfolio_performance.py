import pickle
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from edgar import set_identity, Company
import numpy as np
import plotly.express as px
import re
import requests
from alpaca_data_loader import get_close_prices_dataframe
API_KEY_ALPACA = 'PK6FOT3K6588126LLQFV'
SECRET_API_KEY_ALPACA = 'rOG6npoJBTSPVJsag9teZnrHME1bzLtubqEnmYdP'
def plot_portfolio_performance(
    database: dict,
    cik : str,
    confronto_sp500 : bool = False
):
    """
    1) Plot individual cumulative returns for each filing period.
    2) Plot overall cumulative return chaining all periods sequentially.

    - weights_history: dict 'f0'.. newest
    - dates: dict of corresponding filing dates
    -in confronto sp500 vogliamo chiedere se va mostrato il grafico dell' sp500 insieme a quello del portafoglio del fondo
    """
    #here we define a portfolio to make a dictionary where for the key are the quartile and the values are another dictionary with as key the ticker
    #of the stock and as value the pct change history during the quartil


    plt.figure(figsize=(12, 6))
    all_returns = []
    portfolio_data = database[cik]
    
    # Itera attraverso tutte le chiavi tranne le ultime due che contengono dati di performance
    valid_periods = 0
    for key in list(portfolio_data.keys()):
        # Salta le chiavi che non sono periodi (f0, f1, ecc.)
        if key in ['overall_performances', 'sp500_performances']:
            continue
            
        try:
            # Prova a ottenere i ritorni del portafoglio
            if 'cum_perf' in portfolio_data[key]:
                cum_perf = portfolio_data[key]['cum_perf']
                if not cum_perf.empty:
                    all_returns.append((key, cum_perf))
                    valid_periods += 1
                    print(f"Periodo {key} - Trovati {len(cum_perf)} punti dati")
            
            # Se non c'è cum_perf, prova a calcolarlo dai ritorni azionari
            elif 'stock_returns' in portfolio_data[key]:
                stock_returns = portfolio_data[key]['stock_returns']
                if stock_returns.empty:
                    print(f"Nessun dato di ritorno per il periodo {key}")
                    continue
                    
                # Estrai i pesi per ogni ticker
                pct_alloc = []
                tickers = []
                for ticker, value in portfolio_data[key].items():
                    if ticker not in ['stock_returns', 'cum_perf'] and isinstance(value, (int, float)):
                        tickers.append(ticker)
                        pct_alloc.append(value)
                
                if not pct_alloc:
                    print(f"Nessun peso valido trovato per il periodo {key}")
                    continue
                
                # Seleziona solo le colonne presenti sia nei ticker che nei ritorni
                common_tickers = [t for t in tickers if t in stock_returns.columns]
                if not common_tickers:
                    print(f"Nessun ticker comune trovato per il periodo {key}")
                    continue
                    
                # Calcola i pesi normalizzati
                weights = pd.Series(pct_alloc, index=tickers)
                weights = weights[common_tickers]  # Mantieni solo i ticker comuni
                weights = weights / weights.sum()  # Normalizza a somma 1
                
                # Calcola il rendimento del portafoglio
                port_ret = (stock_returns[common_tickers] * weights).sum(axis=1)
                cum_perf = (1 + port_ret).cumprod()
                
                if not cum_perf.empty:
                    all_returns.append((key, cum_perf))
                    valid_periods += 1
                    print(f"Periodo {key} - Calcolati {len(cum_perf)} punti dati")
                
        except Exception as e:
            print(f"Errore durante l'elaborazione del periodo {key}: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print(f"\nTrovati {valid_periods} periodi validi su {len(portfolio_data)-2} periodi totali")
        
    # Verifica se ci sono dati da plottare
    if not all_returns:
        print("Nessun dato di ritorno valido da visualizzare.")
        return
    
    try:
        # Estrai le Series dalle tuple (chiave, serie)
        if all_returns and isinstance(all_returns[0], tuple):
            # Crea un dizionario con chiavi i nomi dei periodi e valori le Series
            returns_dict = {key: series for key, series in all_returns}
            # Crea un DataFrame da tutte le Series
            all_returns_df = pd.DataFrame(returns_dict)
        else:
            # Se non sono tuple, procedi normalmente
            all_returns_df = pd.concat(all_returns, axis=1)
        
        # Verifica se il DataFrame risultante non è vuoto
        if all_returns_df.empty:
            print("Nessun dato disponibile per il plotting.")
            return
            
        # Plot dei dati
        plt.figure(figsize=(14, 8))
        
        # Ordina le colonne per avere un ordine logico (f0, f1, f2, ...)
        all_returns_df = all_returns_df.reindex(sorted(all_returns_df.columns), axis=1)
        
        # Crea una mappa di colori per le linee
        cmap = plt.get_cmap('tab10')
        colors = [cmap(i % cmap.N) for i in range(len(all_returns_df.columns))]
        
        # Plotta tutte le serie
        for i, col in enumerate(all_returns_df.columns):
            plt.plot(
                all_returns_df.index, 
                all_returns_df[col] - 1, 
                label=col,
                color=colors[i],
                linewidth=2
            )
        
        # Aggiungi una griglia e legenda
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend(title='Periodo', bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # Formatta gli assi
        plt.gca().yaxis.set_major_formatter(plt.FuncFormatter('{0:.0%}'.format))
        plt.gcf().autofmt_xdate()  # Ruota le date per maggiore leggibilità
        
        plt.title('Cumulative Return')
        plt.xlabel('Date')
        plt.ylabel('Cumulative Return')
        plt.grid(True)
        plt.legend()
        plt.show()
        
    except Exception as e:
        print(f"Errore durante il plotting: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # Calcola le performance giornaliere concatenate
    if all_returns:
        try:
            # Ordina i quartili in ordine cronologico (f0 è il più vecchio, fn il più nuovo)
            sorted_returns = sorted(all_returns, key=lambda x: int(x[0][1:]) if x[0][1:].isdigit() else float('inf'))
            
            # Inizializza una lista per i rendimenti giornalieri
            daily_returns = []
            
            # Per ogni quartile, calcola i rendimenti giornalieri
            for key, series in sorted_returns:
                if not series.empty:
                    # Calcola i rendimenti giornalieri dal cumulativo
                    daily_ret = series.pct_change().dropna()
                    daily_returns.append(daily_ret)
            
            if not daily_returns:
                print("Nessun dato di rendimento giornaliero valido trovato.")
                return
                
            # Concatena tutti i rendimenti giornalieri
            all_daily_returns = pd.concat(daily_returns)
            
            # Rimuovi eventuali duplicati mantenendo il primo valore
            all_daily_returns = all_daily_returns[~all_daily_returns.index.duplicated(keep='first')]
            
            # Ordina per data
            all_daily_returns = all_daily_returns.sort_index()
            
            # Calcola il cumulativo a partire dai rendimenti giornalieri
            cumulative_returns = (1 + all_daily_returns).cumprod()
            
            # Normalizza per iniziare da 1 (100%)
            cumulative_returns = cumulative_returns / cumulative_returns.iloc[0]
            
            # Plot del cumulativo
            plt.figure(figsize=(14, 7))
            plt.plot(cumulative_returns.index, cumulative_returns - 1, 
                    label='Performance Cumulativa', linewidth=2, color='blue')
            
            # Formattazione del grafico
            plt.title('Performance Cumulativa del Portafoglio')
            plt.xlabel('Data')
            plt.ylabel('Rendimento Cumulativo')
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter('{0:.0%}'.format))
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.legend()
            plt.tight_layout()
            plt.show()
            
        except Exception as e:
            print(f"Errore durante il calcolo del cumulativo: {str(e)}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    #load database
    with open('database.pickle', 'rb') as handle:
        database = pickle.load(handle)
    #print(database['0001067983']['overall_performances'])
    plot_portfolio_performance(database, '0000102909')