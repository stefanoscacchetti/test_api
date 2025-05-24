from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import base64
from io import BytesIO
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from edgar import set_identity, Company
import plotly.express as px
import re
import requests
import logging

# Configurazione logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="13F Portfolio Analyzer API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class AnalysisResponse(BaseModel):
    treemap_plot: str
    performance_plot: str
    comparison_plot: Optional[str] = None

def clean_tickers(df: pd.DataFrame) -> pd.DataFrame:
    """Pulizia avanzata dei ticker e rimozione duplicati"""
    try:
        df['Ticker'] = (
            df['Ticker']
            .str.upper()
            .str.replace(r'[^A-Z0-9-]', '', regex=True)
            .str.strip()
        )
        return df.groupby('Ticker', as_index=False).agg({'pct_alloc': 'sum'})
    except Exception as e:
        logger.error(f"Error cleaning tickers: {str(e)}")
        return pd.DataFrame()

async def get_portfolio_returns(weights: dict, dates: dict) -> pd.Series:
    """Calcola i rendimenti del portafoglio con gestione duplicati"""
    all_returns = []
    keys = sorted(weights.keys(), key=lambda x: dates[x])

    for idx, key in enumerate(keys):
        try:
            df = weights[key]
            df_clean = clean_tickers(df)
            if df_clean.empty:
                continue

            tickers = df_clean['Ticker'].tolist()
            weights_series = df_clean.set_index('Ticker')['pct_alloc']

            end = dates[key]
            start = dates[keys[idx-1]] if idx > 0 else end - relativedelta(months=3)

            data = yf.download(
                tickers,
                start=start,
                end=end,
                progress=False,
                auto_adjust=True,
                group_by='ticker'
            )

            if data.empty:
                continue

            # Gestione colonne MultiIndex
            if isinstance(data.columns, pd.MultiIndex):
                closes = data.xs('Close', axis=1, level=1, drop_level=False)
                closes.columns = closes.columns.droplevel(0)
            else:
                closes = data['Close'] if 'Close' in data else pd.DataFrame()

            # Rimozione duplicati e colonne vuote
            closes = closes.loc[:, ~closes.columns.duplicated(keep='first')]
            closes = closes.dropna(axis=1, how='all')

            # Allineamento pesi con dati disponibili
            valid_tickers = closes.columns.intersection(weights_series.index)
            if not valid_tickers:
                continue

            closes = closes[valid_tickers]
            weights_aligned = weights_series[valid_tickers].fillna(0)

            # Normalizzazione pesi
            weights_normalized = weights_aligned / weights_aligned.sum()

            # Calcolo rendimenti
            returns = closes.pct_change().fillna(0)
            port_ret = returns.dot(weights_normalized)
            all_returns.append(port_ret)

        except Exception as e:
            logger.error(f"Error processing period {key}: {str(e)}")
            continue

    return pd.concat(all_returns[::-1]).sort_index()

@app.get("/analyze", response_model=AnalysisResponse)
async def analyze_portfolio(
    cik: str = Query(..., description="Fund CIK number"),
    filings_count: int = Query(4, ge=1, le=10),
    top_n: int = Query(5, ge=1, le=20),
    user_agent: str = Query(..., description="User-Agent header for SEC compliance"),
    compare_sp500: bool = Query(False)
):
    try:
        # Caricamento dati
        _, weights, top, dates = await load_data(
            cik=cik,
            filings_count=filings_count,
            identity=user_agent,
            top_n=top_n
        )

        # Generazione grafici
        return await generate_plots(weights, dates, top, compare_sp500)

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def generate_plots(weights, dates, top, compare_sp500):
    # Calcolo rendimenti
    returns = await get_portfolio_returns(weights, dates)
    
    # Creazione grafici
    return {
        "treemap_plot": await generate_treemap(top, dates),
        "performance_plot": await generate_performance_plot(returns),
        "comparison_plot": await generate_comparison_plot(returns) if compare_sp500 else None
    }

async def generate_treemap(top_history, dates):
    try:
        df = top_history['f0'].copy()
        df = clean_tickers(df)
        
        start_date = dates.get('f1', dates['f0'] - relativedelta(months=3))
        end_date = dates['f0']
        
        df['performance'] = df['Ticker'].apply(
            lambda t: get_n_month_return(t, start_date, end_date)
        )
        
        fig = px.treemap(
            df,
            path=['Ticker'],
            values='pct_alloc',
            color='performance',
            color_continuous_scale='RdYlGn',
            title="Portfolio Performance"
        )
        
        buf = BytesIO()
        fig.write_image(buf, format="png")
        return base64.b64encode(buf.getvalue()).decode("utf-8")
    except Exception as e:
        logger.error(f"Treemap error: {str(e)}")
        return ""

async def generate_performance_plot(returns):
    try:
        plt.figure(figsize=(12, 6))
        cum_perf = (1 + returns).cumprod()
        plt.plot(cum_perf.index, cum_perf, label='Portfolio Performance')
        plt.title('Historical Performance')
        plt.xlabel('Date')
        plt.ylabel('Cumulative Return')
        plt.grid(True)
        plt.tight_layout()
        
        buf = BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight')
        plt.close()
        return base64.b64encode(buf.getvalue()).decode('utf-8')
    except Exception as e:
        logger.error(f"Performance plot error: {str(e)}")
        return ""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)