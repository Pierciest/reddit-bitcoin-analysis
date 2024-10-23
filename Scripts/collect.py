import pandas as pd
from pytrends.request import TrendReq
import time
import random

def create_monthly_date_ranges(start_date, end_date):
    """Generates a list of monthly date ranges from start_date to end_date."""
    date_ranges = pd.date_range(start=start_date, end=end_date, freq='M').tolist()
    return [(str(date_range.replace(day=1).date()), str(date_range.date())) for date_range in date_ranges]

def fetch_monthly_trends(keyword, start_date, end_date, geo='US', sleep_time=(10, 20)):
    """Fetches Google Trends data using pytrends with a sliding window by month."""
    pytrends = TrendReq(hl='en-US', tz=360)  # Initialize pytrends request object
    date_ranges = create_monthly_date_ranges(start_date, end_date)
    all_data = pd.DataFrame()

    for start, end in date_ranges:
        print(f"Fetching data for {keyword} from {start} to {end}...")
        try:
            pytrends.build_payload([keyword], cat=0, timeframe=f'{start} {end}', geo=geo)
            data = pytrends.interest_over_time()
            if not data.empty:
                data = data.drop(columns=['isPartial'])
                all_data = pd.concat([all_data, data])
            else:
                print(f"No data for {keyword} from {start} to {end}.")
            time.sleep(random.uniform(*sleep_time))  # Add random delay to avoid rate limits
        except Exception as e:
            print(f"Error fetching data for {keyword} from {start} to {end}: {e}")
    
    return all_data

def fetch_overall_trends(keyword, start_date, end_date, geo='US', sleep_time=(10, 20)):
    """Fetches Google Trends data using pytrends without a sliding window method to understand what each day actually corresponds to."""
    pytrends = TrendReq(hl='en-US', tz=360)  # Initialize pytrends request object
    all_data = pd.DataFrame()
    try:
        pytrends.build_payload([keyword], cat=0, timeframe=f'{start_date} {end_date}', geo=geo)
        data = pytrends.interest_over_time()
        if not data.empty:
            data = data.drop(columns=['isPartial'])
            all_data = pd.concat([all_data, data])
        else:
            print(f"No data for {keyword} from {start_date} to {end_date}.")
        time.sleep(random.uniform(*sleep_time))  # Add random delay to avoid rate limits
    except Exception as e:
        print(f"Error fetching data for {keyword} from {start_date} to {end_date}: {e}")
    
    return all_data

def transform_actual_volume(daily_trends, monthly_trends):
    """Adjusts daily trends based on the corresponding monthly trend values."""
    
    # Ensure 'date' column is in datetime format
    daily_trends['date'] = pd.to_datetime(daily_trends['date'])
    monthly_trends['date'] = pd.to_datetime(monthly_trends['date'])
    
    # Create a 'month' column for both daily and monthly trends
    daily_trends['month'] = daily_trends['date'].dt.to_period('M')
    monthly_trends['month'] = monthly_trends['date'].dt.to_period('M')
    
    # Merge daily and monthly data based on the 'month' column
    merged_data = pd.merge(daily_trends, monthly_trends[['month', 'bitcoin']], on='month', how='left', suffixes=('_daily', '_monthly'))
    
    # Adjust daily trend values by multiplying them with the corresponding monthly trend value
    merged_data['adjusted_value'] = merged_data['bitcoin_daily'] * merged_data['bitcoin_monthly']
    
    return merged_data[['date', 'adjusted_value']]

def save_to_csv(df, filename):
    """Saves the DataFrame to a CSV file."""
    if not df.empty:
        df.to_csv(filename, index=True)
        print(f"Data saved to {filename}")
    else:
        print("No data to save.")

# Example usage
if __name__ == "__main__":
    keyword = "bitcoin"
    start_date = "2016-01-01"
    end_date = "2024-12-31"
    
    # Fetch daily trends data (sliding window)
    daily_trends = fetch_monthly_trends(keyword, start_date, end_date, geo='US')
    
    # Fetch overall monthly trends
    monthly_trends = fetch_overall_trends(keyword, start_date, end_date, geo='US')
    
    # Transform daily trends based on monthly trends
    adjusted_trends = transform_actual_volume(daily_trends, monthly_trends)
    
    # Save the adjusted trends to a CSV file
    save_to_csv(adjusted_trends, '../Data/Google_Trends/adjusted_trends_bitcoin.csv')
