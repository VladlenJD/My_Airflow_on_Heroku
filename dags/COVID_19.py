from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import random
from urllib.parse import urlencode
import os
import psycopg2


default_args = {
    'owner': 'vl.stratu',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 0
}

dag = DAG('daily_report_for_COVID_19',
          default_args=default_args,
          catchup=False,
          schedule_interval='50 05 * * *')

# Yesterday

yesterday = datetime.strftime(datetime.now() - timedelta(1), '%m-%d-%Y')

# The_Day_Before_yesterday

tdb_yesterday = datetime.strftime(datetime.now() - timedelta(2), '%m-%d-%Y')

# Read data for yesterday and tdb_yesterday
covid_yesterday = pd.read_csv(f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/'
                              f'master/csse_covid_19_data/csse_covid_19_daily_reports/{yesterday}.csv')
covid_tdb_yesterday = pd.read_csv(f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/'
                                  f'master/csse_covid_19_data/csse_covid_19_daily_reports/{tdb_yesterday}.csv')
print('Data read')

# Remove unnecessary columns

covid_yesterday.drop(
    ['FIPS', 'Admin2', 'Province_State', 'Last_Update', 'Lat', 'Long_', 'Combined_Key', 'Case_Fatality_Ratio',
     'Incident_Rate'], axis=1, inplace=True)
covid_tdb_yesterday.drop(
    ['FIPS', 'Admin2', 'Province_State', 'Last_Update', 'Lat', 'Long_', 'Combined_Key', 'Case_Fatality_Ratio',
     'Incident_Rate'], axis=1, inplace=True)

# Grouping by country

covid_yesterday = covid_yesterday \
    .groupby('Country_Region', as_index=False) \
    .agg({'Confirmed': 'sum',
          'Deaths': 'sum',
          'Recovered': 'sum',
          'Active': 'sum'})

covid_tdb_yesterday = covid_tdb_yesterday \
    .groupby('Country_Region', as_index=False) \
    .agg({'Confirmed': 'sum',
          'Deaths': 'sum',
          'Recovered': 'sum',
          'Active': 'sum'})

# Add column Case_Fatality_Ratio

covid_tdb_yesterday['Case_Fatality_Ratio'] = covid_tdb_yesterday.Deaths / covid_tdb_yesterday.Confirmed * 100
covid_yesterday['Case_Fatality_Ratio'] = covid_yesterday.Deaths / covid_yesterday.Confirmed * 100

# Create new dataframe and add columns

covid_progress = covid_yesterday.merge(covid_tdb_yesterday, on='Country_Region')
covid_progress = covid_progress.assign(Confirmed_add = covid_progress.Confirmed_x - covid_progress.Confirmed_y,
                                       Deaths_add    = covid_progress.Deaths_x - covid_progress.Deaths_y,
                                       Recovered_add = covid_progress.Recovered_x - covid_progress.Recovered_y,
                                       Active_add    = covid_progress.Active_x - covid_progress.Active_y,
                          Case_Fatality_Ratio_Change = covid_progress.Case_Fatality_Ratio_x - covid_progress.Case_Fatality_Ratio_y)

# Create dataframe with TOP_10 countries with the worst distribution dynamics of COVID

TOP_10 = covid_progress.sort_values(['Confirmed_add', 'Recovered_add', 'Deaths_add', 'Active_add'],
                                    ascending=False).head(10)
TOP_10 = TOP_10.rename(columns={'Country_Region': 'Country'})
TOP_10.reset_index(drop=True, inplace=True)


# Create a variable 'top' for mailing to VK and Telegram
top = TOP_10.rename(columns={'Confirmed_add': 'Growth'})[['Country', 'Growth']].to_csv(sep='➨', index=False)

today = datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y')
print('Data transform')

# Report to Telegram

def daily_report_to_telegram():
    # Create message for Telegram
    message_telegram = f''' TOP_10 countries with the worst distribution dynamics of COVID-19 \n\nReport for: {today}\n\n{top}'''
    # Send message to Telegram
    token = '1488870824:AAEI-SDeHqyUAkL0mYs4yw6NHWq2PXNZTnc'
    # chat_id = 127585XXXX  # my chat id

    message = message_telegram  # message which I want to send
    url_get = 'https://api.telegram.org/bot1488870824:AAEI-SDeHqyUAkL0mYs4yw6NHWq2PXNZTnc/getUpdates'
    response = requests.get(url_get)

    # Add new users
    new_chats = []
    if len(response.json()['result']) != 0:
        for i in response.json()['result']:
            if int(i['message']['chat']['id']) not in new_chats:
                new_chats.append(int(i['message']['chat']['id']))

    DATABASE_URL = os.environ['DATABASE_URL']
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')

    cur = conn.cursor()
    cur.execute("SELECT * FROM bots_users.covid")
    rows = cur.fetchall()
    conn.close()

    chats = [int(i) for sub in rows for i in sub]

    if len(new_chats) != 0:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cur = conn.cursor()

        for c in new_chats:
            if c not in chats:
                chats.append(c)
                cur.execute(f"INSERT INTO bots_users.covid (chat_id) VALUES ({c})")
        conn.commit()
        conn.close()

    for chat in chats:
        params = {'chat_id': chat, 'text': message}

        base_url = f'https://api.telegram.org/bot{token}/'
        url = base_url + 'sendMessage?' + urlencode(params)

        resp = requests.get(url)
        
    print('Report send')

t2 = PythonOperator(task_id='report_to_telegram', python_callable=daily_report_to_telegram, dag=dag)
