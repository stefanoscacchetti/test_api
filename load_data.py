'''
here we write the functions to download all the data from the edgar api
'''
from edgar import set_identity, Company
import pandas as pd

def load_data(
    cik: str,
    filings_count: int = 15,
    identity: str = 'mario rossi mariorossi@gmail.com',
    top_n: int = 5,
    form: str = '13F-HR'
) -> (dict, dict, dict, dict):
    """
    Downloads and processes SEC 13F-HR filings for a given CIK.

    Args:
        cik: Company CIK number as a string (with or without leading zeros)
        filings_count: Number of most recent filings to process
        identity: Email address to identify to EDGAR (required by SEC)
        top_n: Number of top holdings to track in top_history
        form: SEC form type to retrieve (default: '13F-HR')

    Returns:
        tuple: (full_history, weights_history, top_history, dates)
            - full_history: raw DataFrames of holdings per valid filing
            - weights_history: dict of DataFrames with Ticker and pct_alloc
            - top_history: dict of top N positions
            - dates: dict of actual SEC filing_date per key
    """
    print(f"\n{'='*50}")
    print(f"Loading data for CIK: {cik}")
    print(f"Requested filings: {filings_count}")
    print(f"Top N positions to track: {top_n}")
    print(f"Form type: {form}")
    print(f"{'='*50}\n")
    
    if identity:
        set_identity(identity)
    
    try:
        company = Company(cik)
        filings = company.get_filings(form=form)
        print(f"Found {len(filings)} total {form} filings\n")
    except Exception as e:
        print(f" Error accessing EDGAR data for CIK {cik}: {str(e)}")
        return {}, {}, {}, {}
    
    if not filings:
        print(" No filings found for the specified form type")
        return {}, {}, {}, {}

    full_history, weights_history, top_history, dates = {}, {}, {}, {}
    processed_count = 0
    idx = 0
    max_attempts = min(len(filings), filings_count * 2)  # Try up to 2x requested filings in case of errors

    while processed_count < filings_count and idx < max_attempts:
        try:
            filing = filings[idx]
            filing_date = filing.filing_date
            print(f"\nProcessing filing {idx + 1} (saving as f{processed_count}): {filing_date}")
            
            # Get the filing data
            tf = filing.obj()
            df = tf.infotable.copy()
            print(f"Found {len(df)} holdings in this filing")
            
            # Calculate percentage allocation
            df['pct_alloc'] = df['Value'] / df['Value'].sum()
            
            # Create the key for this filing period
            key = f'f{processed_count}'
            
            # Store the data
            full_history[key] = df
            weights_history[key] = df[['Ticker', 'pct_alloc']].reset_index(drop=True)
            top_history[key] = df.nlargest(top_n, 'Value')[['Ticker', 'pct_alloc']].reset_index(drop=True)
            dates[key] = pd.to_datetime(filing_date)
            
            # Print details about this filing
            print(f"\n Filing {key} ({filing_date}) - Top {min(top_n, len(df))} holdings:")
            print(top_history[key].to_string(index=False))
            print(f"\n Summary for {key}:")
            print(f"- Total number of tickers: {len(weights_history[key])}")
            print(f"- Total allocation: {weights_history[key]['pct_alloc'].sum():.2%}")
            print(f"- Date range: {filing_date}")
            print(f"{'='*50}")
            
            processed_count += 1
            
        except Exception as e:
            print(f"\n Error processing filing at index {idx}: {str(e)}")
            print(f"Skipping this filing and trying the next one...")
        
        idx += 1

    # Print summary of all processed filings
    print(f"\n{'='*50}")
    if processed_count > 0:
        print(f" Successfully processed {processed_count} out of requested {filings_count} filings")
        print(f"Final periods: {sorted(weights_history.keys())}")
        
        # Print detailed weights for each period
        for key in sorted(weights_history.keys()):
            print(f"\n Weights for {key} ({dates[key].strftime('%Y-%m-%d')}):")
            print(weights_history[key].head(10).to_string(index=False))  # Show first 10 holdings
            if len(weights_history[key]) > 10:
                print(f"... and {len(weights_history[key]) - 10} more tickers")
    else:
        print(" No filings were successfully processed")
    
    print(f"\n{'='*50}")
    print("Returning data structures with the following shapes:")
    print(f"- full_history: {len(full_history)} periods")
    print(f"- weights_history: {len(weights_history)} periods")
    print(f"- top_history: {len(top_history)} periods")
    print(f"- dates: {len(dates)} periods")
    print(f"{'='*50}\n")
    
    return full_history, weights_history, top_history, dates