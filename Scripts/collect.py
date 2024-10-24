import pandas as pd
from pytrends.request import TrendReq
import time
import random

def create_monthly_date_ranges(start_date, end_date):
    """Generates a list of monthly date ranges from start_date to end_date."""
    date_ranges = pd.date_range(start=start_date, end=end_date, freq='M').tolist()
    return [(str(date_range.replace(day=1).date()), str(date_range.date())) for date_range in date_ranges]

def fetch_monthly_trends(keyword, start_date, end_date, geo='US', sleep_time=(0, 1)):
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
    """Adjusts daily trends by multiplying each day with the corresponding monthly trend value."""
    
    # Reset index in case 'date' is the index for daily_trends
    if 'date' not in daily_trends.columns:
        daily_trends = daily_trends.reset_index()

    # Reset index in case 'date' is the index for monthly_trends
    if 'date' not in monthly_trends.columns:
        monthly_trends = monthly_trends.reset_index()

    # Ensure 'date' column is in datetime format for both daily and monthly data
    daily_trends['date'] = pd.to_datetime(daily_trends['date'])
    monthly_trends['date'] = pd.to_datetime(monthly_trends['date'])
    
    # Extract year-month for both daily and monthly trends for correct matching
    daily_trends['year_month'] = daily_trends['date'].dt.to_period('M')
    monthly_trends['year_month'] = monthly_trends['date'].dt.to_period('M')
    
    # Convert monthly trends into a dictionary with 'year_month' as the key for easy lookup
    monthly_dict = monthly_trends.set_index('year_month')['bitcoin'].to_dict()
    
    # Apply the scaling by multiplying daily trends by the corresponding monthly value
    daily_trends['adjusted_value'] = daily_trends.apply(
        lambda row: row['bitcoin'] * monthly_dict.get(row['year_month'], 1), axis=1
    )
    
    return daily_trends[['date', 'adjusted_value']]





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
    end_date = "2021-12-31"
    
    # Fetch daily trends data (sliding window)
    daily_trends = fetch_monthly_trends(keyword, start_date, end_date, geo='US')
    
    # Fetch overall monthly trends
    monthly_trends = fetch_overall_trends(keyword, start_date, end_date, geo='US')
    
    # Transform daily trends based on monthly trends
    adjusted_trends = transform_actual_volume(daily_trends, monthly_trends)
    
    # Save the adjusted trends to a CSV file
    save_to_csv(adjusted_trends, './Data/Google_Trends/adjusted_trends_bitcoin.csv')
