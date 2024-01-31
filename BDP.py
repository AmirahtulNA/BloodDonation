import pandas as pd
import requests
from io import StringIO, BytesIO
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
from telegram import Bot
from decouple import config
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# URLs for CSV and Parquet files
data_urls = {
    'donations_state.csv': 'https://raw.githubusercontent.com/MoH-Malaysia/data-darah-public/main/donations_state.csv',
    'blood_donation_retention_2024.parquet': 'https://dub.sh/ds-data-granular'
}

# Create an empty dictionary to store DataFrames
dfs = {}

# Function to fetch data from URLs
def fetch_data(url, file_format='csv'):
    response = requests.get(url)
    if file_format == 'csv':
        return pd.read_csv(StringIO(response.text))
    elif file_format == 'parquet':
        return pd.read_parquet(BytesIO(response.content))
    else:
        raise ValueError("Unsupported file format")

# Fetch and store each CSV or Parquet file directly from URLs
for file_name, url in data_urls.items():
    file_format = 'csv' if file_name.endswith('.csv') else 'parquet'
    dfs[file_name] = fetch_data(url, file_format)

# Q1. How are blood donations in Malaysia/states trending?
df_state = dfs ['donations_state.csv']
df_state['Date'] = pd.to_datetime(df_state['date'])
selected_states = ['Malaysia', 'Johor', 'Kedah', 'Kelantan', 'Melaka', 'Negeri Sembilan', 'Pahang', 'Perak', 'Pulau Pinang', 'Sabah', 'Sarawak', 'Selangor', 'Terengganu', 'W.P. Kuala Lumpur']

# Loop through each state
for state in selected_states:
    # Filter data for the selected state
    df_state_selected = df_state[df_state['state'] == state]

    # Group by Date and sum the daily donation numbers
    df_state_trends = df_state_selected.groupby('Date')['daily'].sum()

    # Filter data and resample to daily, past 12 months, and yearly
    current_month = datetime.now().month
    current_year = datetime.now().year
    start_date = datetime(current_year, current_month, 1)

    df_state_trends_daily = df_state_trends[start_date:].resample('D').sum()
    df_state_trends_monthly = df_state_trends[start_date - pd.DateOffset(months=12):].resample('D').sum().resample('MS').sum()
    df_state_trends_yearly = df_state_trends[df_state_trends.index.year < current_year].resample('YE').sum()

    # Create subplots for daily, monthly, and yearly
    fig, axs = plt.subplots(1, 3, figsize=(15, 5))

    # Daily subplot
    axs[0].plot(df_state_trends_daily.index, df_state_trends_daily.values, linestyle='-', color='b')
    axs[0].set_title(f'{state} Daily Blood Donation Trends', fontsize=10)
    axs[0].set_xlabel('Date')
    axs[0].set_ylabel('Total Daily Blood Donations')
    axs[0].tick_params(axis='x', labelsize=6)
    for j, value in enumerate(df_state_trends_daily.values):
        axs[0].annotate(str(value), (df_state_trends_daily.index[j], value), textcoords="offset points",
                        xytext=(0, 10), ha='center', fontsize=6)

    # Monthly subplot
    axs[1].plot(df_state_trends_monthly.index, df_state_trends_monthly.values, linestyle='-', color='b')
    axs[1].set_title(f'{state} Monthly Blood Donation Trends (Past 12 Months)', fontsize=10)
    axs[1].set_xlabel('Date')
    axs[1].set_ylabel('Total Monthly Blood Donations')
    axs[1].tick_params(axis='x', labelsize=6)
    for j, value in enumerate(df_state_trends_monthly.values):
        axs[1].annotate(str(value), (df_state_trends_monthly.index[j], value), textcoords="offset points",
                        xytext=(0, 10), ha='center', fontsize=6)

    # Yearly subplot
    axs[2].plot(df_state_trends_yearly.index, df_state_trends_yearly.values, linestyle='-', color='b')
    axs[2].set_title(f'{state} Yearly Blood Donation Trends', fontsize=10)
    axs[2].set_xlabel('Date')
    axs[2].set_ylabel('Total Yearly Blood Donations')
    axs[2].tick_params(axis='x', labelsize=6)
    for j, value in enumerate(df_state_trends_yearly.values):
        axs[2].annotate(str(value), (df_state_trends_yearly.index[j], value), textcoords="offset points",
                        xytext=(0, 10), ha='center', fontsize=6)

    # Adjust layout and save the plot
    plt.tight_layout()
    plt_path_q1 = f'plot_q1_{state}.png'
    plt.savefig(plt_path_q1)
    plt.close()

# Question 2: Analyzing Donor Retention Trends Over Time
df = dfs ['blood_donation_retention_2024.parquet']

# Convert 'visit_date' to datetime type
df['visit_date'] = pd.to_datetime(df['visit_date'])

# Sort data based on 'visit_date'
df.sort_values(by='visit_date', inplace=True)

# Create a new column to identify the year
df['year'] = df['visit_date'].dt.year

# Initialize a dictionary to store unique donor_ids for each year
unique_donors_by_year = {}

# Loop through each year to identify unique donor_ids
for year in range(df['year'].min(), df['year'].max() + 1):
    unique_donors_by_year[year] = df[df['year'] == year]['donor_id'].unique()

# Initialize lists to store retention rates and years
retention_rates = []
years = []

# Calculate yearly retention rate
for year in range(df['year'].min() + 1, df['year'].max() + 1):
    # Identify common donor_ids from the previous year
    common_donors = np.intersect1d(unique_donors_by_year[year], unique_donors_by_year[year - 1])

    # Calculate retention rate
    retention_rate = len(common_donors) / len(unique_donors_by_year[year - 1])
    retention_rates.append(retention_rate)
    years.append(year)

# Plot the retention rates over the years
plt.figure(figsize=(10, 6))
plt.plot(years, retention_rates, marker='o', linestyle='-')
plt.xlabel('Year')
plt.ylabel('Retention Rate')
plt.title('Yearly Donor Retention Rate')

# Save the plot as an image
plt_path_q2 = 'plot_q2.png'
plt.savefig(plt_path_q2)
plt.close()

# Function to send the plot to Telegram
async def send_plots_to_telegram():
    # Get Telegram bot token and chat ID from environment variables
    bot_token = config('TELEGRAM_BOT_TOKEN')
    chat_id = config('TELEGRAM_CHAT_ID')

    # Initialize the Telegram bot
    bot = Bot(token=bot_token)

    # Send the plot images for Question 1
    for state in selected_states:
        plt_path_q1 = f'plot_q1_{state}.png'
        with open(plt_path_q1, 'rb') as photo_q1:
            await bot.send_photo(chat_id=chat_id, photo=photo_q1)

    # Send the plot image for Question 2
    with open(plt_path_q2, 'rb') as photo_q2:
        await bot.send_photo(chat_id=chat_id, photo=photo_q2)

# Create a scheduler
scheduler = AsyncIOScheduler()

# Schedule the job to run every day at 10.30 am
scheduler.add_job(send_plots_to_telegram, 'cron', hour=10, minute=30, second=0)

# Start the scheduler
scheduler.start()

# Keep the program running
try:
    asyncio.get_event_loop().run_forever()
except (KeyboardInterrupt, SystemExit):
    # Shut down the scheduler cleanly when exiting
    scheduler.shutdown()
