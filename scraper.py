import pandas as pd
import requests
import time
from datetime import datetime
import json
import random

def fetch_historical_esg(tickers, delay=2):
    """
    Fetch historical ESG data for a list of tickers using Yahoo Finance API.
    Returns a pandas DataFrame with historical ESG scores.

    :param tickers: list of ticker symbols (strings).
    :param delay: number of seconds to wait between each request (avoid rate-limiting).
    :return: DataFrame with historical ESG scores per ticker.
    """
    # Yahoo Finance ESG endpoint
    url = "https://query2.finance.yahoo.com/v1/finance/esgChart"
    
    # Headers to mimic a browser request, without this, request will be blocked
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }
    
    all_data = []
    successful_tickers = 0
    failed_tickers = 0
    
    print(f"\nStarting to fetch data for {len(tickers)} tickers...")
    
    for i, ticker in enumerate(tickers, 1):
        print(f"\nProcessing ticker {i}/{len(tickers)}: {ticker}")
        
        # Add random delay between requests (2-5 seconds)
        time.sleep(delay + random.uniform(0, 3))
        
        try:
            response = requests.get(
                url, 
                params={"symbol": ticker},
                headers=headers,
                timeout=10
            )
            
            if response.ok:
                try:
                    data = response.json()
                    if "esgChart" in data and "result" in data["esgChart"]:
                        # Extract the ESG series data
                        esg_data = data["esgChart"]["result"][0]["symbolSeries"]
                        
                        # Convert to DataFrame
                        df = pd.DataFrame(esg_data)
                        df["ticker"] = ticker
                        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
                        
                        # Keep only the columns we want
                        columns_to_keep = ["timestamp", "ticker", "esgScore", "governanceScore", 
                                         "environmentScore", "socialScore"]
                        df = df[columns_to_keep]
                        
                        all_data.append(df)
                        successful_tickers += 1
                        print(f"Successfully fetched data for {ticker}")
                    else:
                        print(f"No ESG data available for {ticker}")
                        failed_tickers += 1
                except json.JSONDecodeError:
                    print(f"Invalid JSON response for {ticker}")
                    failed_tickers += 1
            elif response.status_code == 429:
                print(f"Rate limited for {ticker}, waiting longer...")
                time.sleep(10)  # Increased wait time for rate limiting
                continue
            else:
                print(f"Error fetching data for {ticker}: {response.status_code}")
                failed_tickers += 1
                
        except requests.exceptions.RequestException as e:
            print(f"Request error for {ticker}: {e}")
            failed_tickers += 1
            continue
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            failed_tickers += 1
            continue
    
    print(f"\nFetch completed:")
    print(f"Successfully processed: {successful_tickers} tickers")
    print(f"Failed to process: {failed_tickers} tickers")
    
    if all_data:
        # Combine all dataframes
        final_df = pd.concat(all_data, ignore_index=True)
        print(f"\nTotal rows in final dataset: {len(final_df)}")
        print(f"Number of unique tickers in final dataset: {final_df['ticker'].nunique()}")
        return final_df
    else:
        print("\nNo data was successfully fetched for any ticker")
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=['ticker', 'timestamp', 'last_processing_date', 
                                   'total_score', 'environment_score', 'social_score', 'governance_score'])

def format_esg_data(esg_df):
    """
    Renames columns to a standardized format and adds a processing date column.
    """
    if esg_df.empty:
        return esg_df
        
    # Rename columns to match our desired format
    column_mapping = {
        'esgScore': 'total_score',
        'governanceScore': 'governance_score',
        'environmentScore': 'environment_score',
        'socialScore': 'social_score'
    }
    
    esg_df = esg_df.rename(columns=column_mapping)
    
    # Add processing date
    esg_df['last_processing_date'] = datetime.now().strftime('%Y-%m-%d')
    
    # Reorder columns
    cols = ['ticker', 'timestamp', 'last_processing_date']
    for col in ['total_score', 'environment_score', 'social_score', 'governance_score']:
        if col in esg_df.columns:
            cols.append(col)
    
    return esg_df[cols]

if __name__ == "__main__":
    # 1. Read your CSV file containing tickers
    csv_file = "sp500_tickers.csv"
    # Read the CSV file as a simple list of tickers without headers
    df = pd.read_csv(csv_file, header=None, names=['Symbol'])
    
    # Get unique tickers
    tickers = df['Symbol'].unique().tolist()
    print(f"Found {len(tickers)} unique tickers in {csv_file}.")

    # Process all tickers
    test_mode = False  # Set to False for full run
    if test_mode:
        test_tickers = tickers[:5]  # Just process the first 5 tickers
        print(f"TEST MODE: Only processing {len(test_tickers)} tickers: {test_tickers}")
        tickers = test_tickers

    # Fetch historical ESG data for all tickers
    # Using a longer delay (5 seconds) to avoid rate limiting
    raw_esg_df = fetch_historical_esg(tickers, delay=5)

    # Format the ESG data
    final_esg_df = format_esg_data(raw_esg_df)

    # Print detailed information about the final dataset
    print(f"\nFinal dataset information:")
    print(f"Total rows: {len(final_esg_df)}")
    print(f"Number of unique tickers: {final_esg_df['ticker'].nunique()}")
    print(f"Date range: {final_esg_df['timestamp'].min()} to {final_esg_df['timestamp'].max()}")
    print(f"\nSample of historical ESG data (first 5 rows):")
    print(final_esg_df.head())

    # Save to a CSV file
    output_file = "historical_esg_data.csv"
    final_esg_df.to_csv(output_file, index=False)
    print(f"\nHistorical ESG data saved to {output_file}!")
