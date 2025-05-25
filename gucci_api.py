from fastapi import FastAPI, HTTPException, Query, Response, File, UploadFile, status, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Set, Tuple, Dict, Any
import pickle
import pandas as pd
import plotly.express as px
import io
import os
from plotly.io import to_image
import base64
import subprocess
from pathlib import Path
from typing import List, Dict, Any

def _get_performance_color(performance: float) -> str:
    """Convert performance percentage to a color between red and green.
    
    Args:
        performance: Performance as a decimal (e.g., 0.1 for 10%)
        
    Returns:
        str: Hex color code
    """
    # Ensure performance is between -1 and 1
    performance = max(-1, min(1, performance))
    
    if performance >= 0:
        # Green gradient (lighter to darker)
        intensity = int(200 + (55 * (1 - performance)))
        return f'#00{intensity:02x}00'
    else:
        # Red gradient (darker to lighter)
        intensity = int(200 + (55 * (1 - abs(performance))))
        return f'#{intensity:02x}0000'

app = FastAPI()

# Configure CORS
origins = [
    "http://localhost",
    "http://localhost:3000",
    "https://app.flutterflow.io",
    "https://*.flutterflow.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add middleware to log requests for debugging
@app.middleware("http")
async def add_cors_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

# Helper function to load database
def load_database():
    try:
        with open('database.pickle', 'rb') as data:
            return pickle.load(data)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Database not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading database: {str(e)}")

# Get all data for a specific CIK
@app.get("/cik={cik}")
def get_cik_data(cik: str):
    db = load_database()
    if cik not in db:
        raise HTTPException(status_code=404, detail="CIK not found in database")
    return db[cik]

# Get specific trimester data for a CIK
@app.get("/cik/{cik}/trimester/{trimester}")
def get_cik_trimester(cik: str, trimester: str):
    db = load_database()
    if cik not in db:
        raise HTTPException(status_code=404, detail="CIK not found in database")
    if trimester not in db[cik]:
        raise HTTPException(status_code=404, detail=f"Trimester {trimester} not found for CIK {cik}")
    return db[cik][trimester]

# Get multiple CIKs by names or first X entries
def is_valid_cik(cik: str) -> bool:
    """Check if a string is a valid CIK (10 digits, no spaces)."""
    return cik.strip().isdigit() and len(cik.strip()) == 10

def read_cik_file() -> Set[str]:
    """Read existing CIKs from file, skipping comments and empty lines."""
    cik_file = Path('cik.txt')
    if not cik_file.exists():
        return set()
    
    with open(cik_file, 'r') as f:
        return {
            line.strip() 
            for line in f 
            if line.strip() and not line.strip().startswith('#')
        }

def write_cik_file(ciks: Set[str]) -> None:
    """Write CIKs to file, one per line with comments."""
    with open('cik.txt', 'w') as f:
        f.write("# Save all CIKs, one per line without spaces\n")
        for cik in sorted(ciks):
            f.write(f"{cik}\n")

@app.post("/update_ciks/")
#to make the request of update the database
#curl.exe -X 'POST' 'http://localhost:8000/update_ciks/' -F 'file=@new_ciks.txt'
async def update_ciks(file: UploadFile = File(...)):
    """
    Update the CIK list from an uploaded file and trigger database update.
    The file should contain one CIK (10 digits) per line.
    """
    try:
        # Read and validate the uploaded file
        content = await file.read()
        try:
            # Decode and split into lines
            new_ciks = content.decode('utf-8').splitlines()
        except UnicodeDecodeError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid file encoding. Please use UTF-8 text file."
            )
        
        # Process and validate CIKs
        valid_ciks = set()
        invalid_lines = []
        
        for i, line in enumerate(new_ciks, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue  # Skip empty lines and comments
                
            if is_valid_cik(line):
                valid_ciks.add(line)
            else:
                invalid_lines.append((i, line))
        
        if not valid_ciks and not invalid_lines:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid CIKs found in the uploaded file"
            )
        
        # Merge with existing CIKs
        existing_ciks = read_cik_file()
        updated_ciks = existing_ciks.union(valid_ciks)
        
        # Only update if there are new CIKs
        new_ciks_added = valid_ciks - existing_ciks
        if not new_ciks_added:
            return {
                "status": "success",
                "message": "No new CIKs to add",
                "total_ciks": len(updated_ciks)
            }
        
        # Write updated CIKs to file
        write_cik_file(updated_ciks)
        
        # Trigger database update
        try:
            subprocess.Popen(
                ["python", "database_update.py"],
                cwd=os.getcwd(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        except Exception as e:
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={
                    "status": "warning",
                    "message": "CIKs updated but database update failed to start",
                    "details": str(e),
                    "new_ciks_added": list(new_ciks_added),
                    "total_ciks": len(updated_ciks)
                }
            )
        
        return {
            "status": "success",
            "message": f"Successfully added {len(new_ciks_added)} new CIKs",
            "new_ciks_added": list(new_ciks_added),
            "total_ciks": len(updated_ciks),
            "database_update": "started"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing file: {str(e)}"
        )

@app.get("/ciks/")
def get_multiple_ciks(
    names: Optional[str] = Query(None, description="Comma-separated list of CIKs"),
    first: Optional[int] = Query(None, description="Number of first CIKs to return", ge=1)
):
    db = load_database()
    all_ciks = list(db.keys())
    
    if names and first:
        raise HTTPException(status_code=400, detail="Use either 'names' or 'first', not both")
    
    if names:
        requested_ciks = names.split(',')
        result = {cik: db[cik] for cik in requested_ciks if cik in db}
        if not result:
            raise HTTPException(status_code=404, detail="None of the requested CIKs were found")
        return result
    
    if first:
        if first > len(all_ciks):
            raise HTTPException(
                status_code=400, 
                detail=f"Database contains only {len(all_ciks)} CIKs"
            )
        return {cik: db[cik] for cik in all_ciks[:first]}
    
    raise HTTPException(status_code=400, detail="Must provide either 'names' or 'first' parameter")

# Get treemap visualization for a specific CIK and trimester
@app.get("/cik/{cik}/treemap/{trimester}")
async def get_treemap_visualization(cik: str, trimester: str):
    """
    Generate and return treemap data for a specific CIK and trimester.
    Returns a JSON object optimized for Flutter applications.
    The treemap shows portfolio allocation colored by stock performance.
    """
    # Load the database
    db = load_database()
    
    # Validate CIK and trimester
    if cik not in db:
        raise HTTPException(status_code=404, detail="CIK not found in database")
    if trimester not in db[cik]:
        raise HTTPException(status_code=404, detail=f"Trimester {trimester} not found for CIK {cik}")
    
    try:
        # Get the data for the specified trimester
        trimester_data = db[cik][trimester]
        
        # Get tickers and allocations (exclude non-ticker keys)
        tickers = [k for k in trimester_data.keys() if k not in ['stock_returns', 'filing_date']]
        pct_alloc = [trimester_data[t] for t in tickers]
        
        # Get stock returns and filter to only include tickers we have allocations for
        if 'stock_returns' not in trimester_data:
            raise HTTPException(status_code=404, detail="Stock returns data not available for this trimester")
            
        t = trimester_data['stock_returns']
        valid_tickers = [ticker for ticker in tickers if ticker in t.columns]
        valid_allocations = [trimester_data[ticker] for ticker in valid_tickers]
        
        # Calculate performance for valid tickers
        performance_last_trimester = []
        for stock in valid_tickers:
            series = t[stock].dropna()
            if len(series) >= 2:
                start = series.iloc[0]
                end = series.iloc[-1]
                performance = (end - start) / start if start != 0 else 0
                performance_last_trimester.append(performance)
            else:
                performance_last_trimester.append(0)
        
        # Create DataFrame
        data = pd.DataFrame({
            'tickers': valid_tickers,
            'pct_alloc': valid_allocations,
            'performance': performance_last_trimester,
            'hover_text': [
                f"Ticker: {ticker}<br>"
                f"Allocation: {alloc:.2f}%<br>"
                f"Performance: {perf:.2%}"
                for ticker, alloc, perf in zip(valid_tickers, valid_allocations, performance_last_trimester)
            ]
        })
        
        # Prepare Flutter-friendly response
        response_data = {
            'title': f'Portfolio Allocation - {trimester}',
            'data': [],
            'total_allocation': sum(valid_allocations),
            'total_performance': sum(p * a for p, a in zip(performance_last_trimester, valid_allocations)) / sum(valid_allocations) if valid_allocations else 0
        }
        
        # Add each stock's data
        for i, (ticker, alloc, perf) in enumerate(zip(valid_tickers, valid_allocations, performance_last_trimester)):
            response_data['data'].append({
                'id': i,
                'ticker': ticker,
                'allocation': float(alloc),
                'performance': float(perf),
                'color': _get_performance_color(perf)
            })
        
        # Sort by allocation (descending)
        response_data['data'].sort(key=lambda x: x['allocation'], reverse=True)
        
        return JSONResponse(content=response_data)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating treemap: {str(e)}")



#endpoint to return the graph data to the forntend
@app.get("/cik/{cik}/performance")
async def get_portfolio_performance(cik: str):
    """
    Get portfolio performance data for a specific CIK.
    Returns performance data in a format suitable for FlutterFlow graph widget.
    """
    try:
        # Load the database
        db = load_database()
        
        # Validate CIK exists in database
        if cik not in db:
            raise HTTPException(status_code=404, detail="CIK not found in database")
            
        # Get the portfolio data for the CIK
        portfolio_data = db[cik]
        
        # Check if we have performance data
        if 'overall_performances' not in portfolio_data:
            raise HTTPException(
                status_code=404,
                detail="No performance data available for this CIK"
            )
            
        # Get the performance data
        performance_data = portfolio_data['overall_performances']
        
        # Convert the performance data to a format suitable for FlutterFlow
        # The exact format depends on your data structure, but here's an example:
        performance_points = []
        
        # Example: If performance_data is a pandas Series with dates as index
        if hasattr(performance_data, 'index') and hasattr(performance_data, 'values'):
            for date, value in zip(performance_data.index, performance_data.values):
                performance_points.append({
                    'date': str(date),  # Convert date to string
                    'value': float(value)  # Ensure value is serializable
                })
        # If it's a dictionary with date: value pairs
        elif isinstance(performance_data, dict):
            for date, value in performance_data.items():
                performance_points.append({
                    'date': str(date),
                    'value': float(value)
                })
        else:
            # Handle other data formats as needed
            raise HTTPException(
                status_code=500,
                detail="Unexpected performance data format"
            )
        
        # Get S&P 500 performance if available
        sp500_points = []
        if 'sp500_performances' in portfolio_data:
            sp500_data = portfolio_data['sp500_performances']
            if hasattr(sp500_data, 'index') and hasattr(sp500_data, 'values'):
                for date, value in zip(sp500_data.index, sp500_data.values):
                    sp500_points.append({
                        'date': str(date),
                        'value': float(value)
                    })
            elif isinstance(sp500_data, dict):
                for date, value in sp500_data.items():
                    sp500_points.append({
                        'date': str(date),
                        'value': float(value)
                    })
        
        # Return the data in a format suitable for FlutterFlow
        return {
            'portfolio': {
                'name': 'Portfolio',
                'data': performance_points
            },
            'sp500': {
                'name': 'S&P 500',
                'data': sp500_points
            } if sp500_points else None
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving performance data: {str(e)}"
        )