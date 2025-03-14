# ESG Yahoo Finance Scraper

A Python script to fetch historical Environmental, Social, and Governance (ESG) data for stock tickers using the Yahoo Finance API.

## Features

- Fetches historical ESG data including:
  - Total ESG Score
  - Environmental Score
  - Social Score
  - Governance Score
- Handles rate limiting with automatic retries
- Processes multiple tickers with error handling
- Saves data to CSV format

## Requirements

```
pandas
requests
```

## Usage

1. Prepare your input file:
   - Create a CSV file named `sp500_tickers.csv` with one ticker symbol per line
   - No headers required

2. Run the script:
```bash
python scraper.py
```

3. Output:
   - The script will create `historical_esg_data.csv` with the following columns:
     - ticker
     - timestamp
     - last_processing_date
     - total_score
     - environment_score
     - social_score
     - governance_score

## Error Handling

- The script includes robust error handling for:
  - Rate limiting (429 errors)
  - Missing ESG data
  - Network errors
  - Invalid responses

## Notes

- Uses random delays between requests to avoid rate limiting
- Includes proper headers to mimic browser requests
- Provides detailed progress tracking during execution 