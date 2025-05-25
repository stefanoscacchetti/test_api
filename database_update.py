from load_data import load_data
import time
#import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from edgar import set_identity, Company
import numpy as np
import plotly.express as px
import re
import requests
import os
import pickle
import time
from alpaca_data_loader import get_close_prices_dataframe
from datetime import datetime, timedelta


API_KEY_ALPACA = 'PK6FOT3K6588126LLQFV'
SECRET_API_KEY_ALPACA = 'rOG6npoJBTSPVJsag9teZnrHME1bzLtubqEnmYdP'


def data_prep_for_db_update(
    database: dict,
    cik: str,
    filings_count: int = 10,
    identity: str = None,
    top_n: int = 20
):
    """
    Prepara i dati per l'aggiornamento del database
    """
    print(f"\n⏳ Inizio elaborazione per CIK: {cik}")
    
    # Inizializza il dizionario per il CIK se non esiste
    if cik not in database:
        database[cik] = {}
    
    # Carica i dati
    try:
        print(f"Caricamento dati per CIK: {cik}")
        full_history, data2, top_history, dates = load_data(
            cik=cik,
            filings_count=filings_count,
            identity=identity,
            top_n=top_n
        )
    except Exception as e:
        print(f"❌ Errore durante il caricamento dei dati: {str(e)}")
        import traceback
        traceback.print_exc()
        return database
    
    if not data2:
        print(f"⚠️ Nessun dato trovato per CIK: {cik}")
        return database
    
    # Prendi le chiavi e ordinale
    keys = sorted(data2.keys(), key=lambda k: int(k[1:]) if k[1:].isdigit() else 0)
    print('\n' + '#' * 50)
    print(f"📋 Periodi trovati: {keys}")
    print(f"📊 Dati caricati per {len(keys)} periodi")
    print('#' * 50 + '\n')
    
    all_returns = []
    
    if not keys:
        print("⚠️ Nessun periodo da elaborare")
        return database
    
    for idx, key in enumerate(keys):
        print(f"\n{'='*50}")
        print(f"Elaborazione periodo {idx+1}/{len(keys)}: {key}")
        print(f"Inizio elaborazione alle: {datetime.now().strftime('%H:%M:%S')}")
        
        # Pausa tra le richieste
        if idx > 0:  # Non serve aspettare prima della prima richiesta
            wait_time = 60  # 60 secondi tra una richiesta e l'altra
            print(f"Attesa di {wait_time} secondi per rispettare i limiti di frequenza...")
            time.sleep(wait_time)
        try:
            print(f"\nEstrazione dati per il periodo {key}")
            print("-" * 50)
            
            # Estrai il dataframe per il periodo corrente
            df = data2[key]
            print(f"Trovate {len(df)} righe nel dataframe per {key}")
            
            if df.empty:
                print(f"Attenzione: DataFrame vuoto per il periodo {key}")
                continue
                
            # Crea un dizionario che mappa i ticker alle loro allocazioni percentuali
            ticker_allocations = df.groupby('Ticker')['pct_alloc'].sum().to_dict()
            print(f"Trovati {len(ticker_allocations)} ticker unici nelle allocazioni")
            
            # Estrai i ticker unici dal dataframe
            tickers = df['Ticker'].dropna().astype(str).unique().tolist()
            print(f"Trovati {len(tickers)} ticker unici (dopo pulizia)")
            
            if not tickers:
                print(f"Nessun ticker valido trovato per il periodo {key}")
                continue
                
            # Ordina i ticker per allocazione percentuale (decrescente)
            sorted_tickers = sorted(tickers, key=lambda x: ticker_allocations.get(x, 0), reverse=True)
            
            # Inizializza il dizionario per questo periodo
            database[cik][key] = dict()
            
            # Imposta le date di inizio e fine
            end = dates[key]
            print(f"Data di fine per {key}: {end}")
            
            if idx + 1 < len(keys):
                start = dates[keys[idx+1]]
            else:
                start = end - relativedelta(days=90)
                
            print(f"Data di inizio per {key}: {start}")
            
            # Limita il numero di ticker se necessario (per evitare limiti di API)
            if len(sorted_tickers) >= 31:
                print(f"Troppi tickers ({len(sorted_tickers)}), tengo solo i primi 29")
                sorted_tickers = sorted_tickers[:29]
            
            # Salva le allocazioni nel database
            try:
                print(f"Salvataggio di {len(sorted_tickers)} ticker per {key} nel database...")
                
                # Assicurati che la chiave esista nel database
                if key not in database[cik]:
                    database[cik][key] = {}
                    
                # Salva ogni ticker con la sua allocazione
                saved_count = 0
                for ticker in sorted_tickers:
                    if ticker in ticker_allocations:
                        database[cik][key][ticker] = ticker_allocations[ticker]
                        saved_count += 1
                
                print(f"✅ Salvati con successo {saved_count} ticker per il periodo {key}")
                
                # Stampa un riepilogo delle chiavi attualmente presenti nel database
                print(f"\n📊 Riepilogo database dopo il salvataggio di {key}:")
                print(f"CIK nel database: {list(database.keys())}")
                print(f"Periodi salvati per {cik}: {list(database[cik].keys())}")
                
                # Salva il database dopo ogni periodo
                try:
                    with open('database.pkl', 'wb') as f:
                        pickle.dump(database, f)
                    print(f"💾 Database salvato con successo dopo il periodo {key}")
                except Exception as save_error:
                    print(f"\n⚠️ Attenzione: impossibile salvare il database dopo il periodo {key}")
                    print(f"Tipo di errore: {type(save_error).__name__}")
                    print(f"Messaggio: {str(save_error)}")
                
            except Exception as save_error:
                print(f"\n❌ Errore durante il salvataggio dei dati per {key}:")
                print(f"Tipo di errore: {type(save_error).__name__}")
                print(f"Messaggio: {str(save_error)}")
                import traceback
                traceback.print_exc()
            
            print("-" * 50)
            
            # Aggiungi una piccola pausa per evitare sovraccarichi
            time.sleep(1)
            
        except Exception as e:
            print(f"\n❌ Errore durante l'elaborazione del periodo {key}:")
            print(f"Tipo di errore: {type(e).__name__}")
            print(f"Messaggio: {str(e)}")
            import traceback
            traceback.print_exc()
            print("-" * 50)
            continue  # Continua con il prossimo periodo
        
        #print(len(tickers))
        #if len(sorted_tickers)> 350:
        # break
        '''
        data = get_close_prices_dataframe(API_KEY_ALPACA, SECRET_API_KEY_ALPACA, sorted_tickers, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
            start=start.strftime('%Y-%m-%d'),
            end=end.strftime('%Y-%m-%d'),
            progress=False,
            auto_adjust=True)
        
        '''

        try:
            print(f"\nRichiesta prezzi per {len(sorted_tickers)} ticker dal {start.date()} al {end.date()}")
            print("-" * 50)
            
            # Verifica che ci siano ticker da processare
            if not sorted_tickers:
                print("Nessun ticker valido da processare, passo al prossimo periodo")
                continue
                
            # Richiedi i prezzi
            print(f"Chiamata a get_close_prices_dataframe con {len(sorted_tickers)} ticker...")
            data = get_close_prices_dataframe(
                API_KEY_ALPACA, 
                SECRET_API_KEY_ALPACA, 
                sorted_tickers, 
                start.strftime('%Y-%m-%d'), 
                end.strftime('%Y-%m-%d')
            )
            
            print("\nRisultato della richiesta prezzi:")
            print(f"Dimensione del dataframe: {data.shape}")
            
            if data.empty:
                print(f"⚠️ Nessun dato prezzo disponibile per il periodo {key}")
                print(f"Ticker richiesti: {sorted_tickers}")
                print(f"Intervallo date: {start.date()} - {end.date()}")
            else:
                print("Prime righe dei dati:")
                print(data.head())
                
        except Exception as e:
            print(f"\n❌ Errore durante la richiesta dei prezzi per il periodo {key}:")
            print(f"Tipo di errore: {type(e).__name__}")
            print(f"Messaggio: {str(e)}")
            import traceback
            traceback.print_exc()
            print("-" * 50)
            continue  # Continua con il prossimo periodo
            
        if data.empty:
            continue

        prices = data['Close'] if 'Close' in data else data
        if isinstance(prices.columns, pd.MultiIndex):
            prices.columns = prices.columns.get_level_values(-1)
        print(prices)
        prices = prices.loc[:, ~prices.columns.duplicated()]
        
        returns = prices.pct_change().fillna(0)
        #save all the returns of all the stocks
        database[cik][key]['stock_returns'] = returns
        
        # Crea il vettore dei pesi a partire dalle allocazioni
        weights_series = pd.Series(ticker_allocations)
        # Normalizza i pesi per assicurarsi che la somma sia 1
        #weights_series = weights_series / weights_series.sum()
        weights_series = weights_series
        
        # Allinea i pesi con le colonne di returns
        w = weights_series.reindex(returns.columns).fillna(0)

        #print debug per vedere se i pesi vengono assegnati bene
        print('#############################\n\n\n')
        print()
        print(w)
        print(returns)
        print('#############################\n\n\n')
        
        # Calcola il rendimento del portafoglio
        port_ret = returns.mul(w, axis=1).sum(axis=1)
        cum_perf = (1 + port_ret).cumprod()
        
        #here we put a inside the quartile the rendimenti giorno per giorno solo per quel quartile
        database[cik][key]['cum_perf'] = cum_perf
        # individual period plot
        #plt.plot(cum_perf.index, cum_perf.values, label=f"{key}: {start.date()} to {end.date()}")

        all_returns.append(port_ret)

    #here we save the performance of the portfolio for all the period day by day
    #print(all_returns.head())
    #print(all_returns.tail())
    #print returns in the middle
    #print(all_returns.iloc[all_returns.shape[0]//2-8:all_returns.shape[0]//2])
    chained = pd.concat(all_returns[::-1])
    chained = chained[~chained.index.duplicated(keep='first')]
    overall_cum = (chained).cumsum()
    '''
    sp500_data = yf.download(
            '^GSPC',
            start=start.strftime('%Y-%m-%d'),
            end=datetime.today(),
            progress=False,
            auto_adjust=True
        )
    '''
    sp500_data = get_close_prices_dataframe(API_KEY_ALPACA, SECRET_API_KEY_ALPACA,["SPY"] , start.strftime('%Y-%m-%d'), datetime.today() - timedelta(days=2))
    #we use the close column
    sp500_close = sp500_data['SPY']
    #find intersection of the indexes
    common_dates = overall_cum.index.intersection(sp500_close.index)
    # Filtra le serie per le date comuni
    overall_cum_aligned = overall_cum.loc[common_dates]
    sp500_close_aligned = sp500_close.loc[common_dates]

    # normalization because we use a graph witht he the percentage on the y axes
    sp500_normalized = sp500_close_aligned / sp500_close_aligned.iloc[0]

    #here we save in the database the general performances of the portfolio and the performances of the sp500
    database[cik]['overall_performances'] = overall_cum_aligned
    database[cik]['sp500_performances'] = sp500_normalized
    print(f'cik {cik} saved correctly')
    return database



def database_update(filings_count : int= 10, identity : str = 'mario rossi mario@example.com', top_n :int = 2,database : str = 'database.pickle', path_cik_list : str = 'cik.txt', f=0):
    '''
    in input mettere il nome del file pickle con estensione e stessa cosa per path>_cik_list
    '''

    #se il database non viene dato in input lo si cerca nella current directory
    
    # Ottieni il percorso corrente
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, str(database))

    # Prova a caricare il database esistente; se non esiste, inizializza con una lista vuota
    try:
        with open(file_path, 'rb') as handle:
            database = pickle.load(handle)
    except FileNotFoundError:
        print("File database.pickle non trovato. Inizializzo un nuovo database.")
        database = {}  # oppure {} se preferisci usare un dizionario
      
    
    #legge file cik.txt per trovare i dati da scaricare
    file_path_cik = os.path.join(os.getcwd(), str(path_cik_list))
    # Leggi il file e crea la lista, rimuovendo eventuali spazi o caratteri di nuova linea
    with open(file_path_cik, "r") as file:
            cik_list = [line.strip() for line in file if line.strip()]
    print(cik_list)
    
    # Process each CIK
    for cik in cik_list:
        try:
            if len(cik) == 10:  # Ensure it's a valid CIK (10 digits)
                print(f"\n{'='*80}")
                print(f" Inizio elaborazione CIK: {cik}")
                print(f"{'='*80}\n")
                
                # Processa il CIK
                database = data_prep_for_db_update(database, cik, filings_count=filings_count, identity=identity, top_n=20)
                
                # Stampa riepilogo dopo ogni CIK
                # Salva il database dopo ogni CIK
                with open('database.pkl', 'wb') as f:
                    pickle.dump(database, f)
                print(" Database salvato con successo dopo l'elaborazione del CIK")
                
                print(f"\n{'='*80}")
                print(f" Completata elaborazione CIK: {cik}")
                print(f"Periodi elaborati: {list(database.get(cik, {}).keys())}")
                print(f"{'='*80}\n")
                
        except Exception as e:
            print(f"\n Errore durante l'elaborazione del CIK {cik}:")
            print(f"Tipo di errore: {type(e).__name__}")
            print(f"Messaggio: {str(e)}")
            import traceback
            traceback.print_exc()
            print("-" * 50)
            continue
            
    # Salva l'oggetto aggiornato nel file pickle, sovrascrivendo il vecchio
    print(f'the keys are {database["0001067983"].keys()}')
    print(f'the checks in the database for now are:\n {database.keys()}')
    with open(file_path, 'wb') as handle:
        pickle.dump(database, handle, protocol=pickle.HIGHEST_PROTOCOL)

    print("Database aggiornato con successo!")

if __name__ == '__main__':
    database_update(filings_count=2)