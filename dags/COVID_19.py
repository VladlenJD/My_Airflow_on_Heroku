from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import random
#import vk_api
from urllib.parse import urlencode
#import matplotlib.pyplot as plt
#from matplotlib.ticker import FuncFormatter
#import seaborn as sns


default_args = {
    'owner': 'vl.stratu',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 0
}

dag = DAG('daily_report_for_COVID_19',
          default_args=default_args,
          catchup=False,
          schedule_interval='00 06,12 * * *')

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
TOP_10.set_index('Country', inplace=True)

# Create a variable 'top' for mailing to VK and Telegram
top = TOP_10.rename(columns={'Confirmed_add': 'Growth'})['Growth'].to_csv(sep='➨')

today = datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y')
print('Data transform')

# Read timeseries_confirmed_global

#confirmed = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')

# Let's take the last 2 weeks
#confirmed_by_days = confirmed[confirmed.columns[-15:]]

# Beautiful charts settings

#plt.style.use('ggplot')                       # Красивые графики
#plt.rcParams['figure.figsize'] = (30, 20)

# Dataframe for vizualization

#viz = confirmed_by_days.sum().to_frame('confirmed_viz').reset_index(drop=False)
#viz = viz.rename(columns={'index': 'date'})
#vizual = pd.DataFrame({'date': viz.date, 'confirmed_count': viz.confirmed_viz - viz.confirmed_viz.shift(1)})
#vizual.drop(0, inplace=True)
#vizual.date = pd.to_datetime(vizual.date)
#vizual['date'] = vizual['date'].dt.strftime('%d-%b')


#fig = plt.figure()
#fig.patch.set_facecolor('#CCFFFF')
#plt.title('Growth by World', fontsize=100, pad=60)

# Building a barplot

#ax = sns.barplot(x = vizual.date, y = 'confirmed_count', data = vizual, palette='ch:s=-.2,r=.6')

#plt.subplots_adjust(left=0.10, right=0.95, hspace=0.25, wspace=0.35)

#plt.xlabel('Daily Cases', fontsize=80, labelpad=50)
#plt.ylabel('Confirmed', fontsize=60, labelpad=30)

#plt.xticks(fontsize=30)
#plt.yticks(fontsize=30)

#ax.yaxis.set_major_formatter(FuncFormatter(lambda y, p: '{:,g}k'.format(y/1000)))


#ax.set_facecolor('#CCFFFF')
#ax.grid(color='grey', linewidth=2, linestyle='--')

#fig.savefig('Add_Confirmed', facecolor=fig.get_facecolor())

#ax.set_frame_on(False)

# Report to VK

#def daily_report_to_vk():

#    app_token = '6ceed695e050149c36b705006d51139574adade15213093ceda7f5b2fe40f8f3a85e00d2ddad94a5fcb0c'
#    vk_session = vk_api.VkApi(token=app_token)
#    vk = vk_session.get_api()

    # Create message for VK
#    message_vk = f''' TOP_10 countries with the worst distribution dynamics of COVID \n\n Report for: {today}\n\n
#                                {top}'''

    #   Send message to VK
#    vk.messages.send(
#            chat_id=1,
#            random_id=random.randint(1, 2 ** 31),
#            message=message_vk
#        )
#    print('Report send')

# Report to Telegram

def daily_report_to_telegram():
    # Create message for Telegram
    message_telegram = f''' TOP_10 countries with the worst distribution dynamics of COVID-19 \n\n Report for: {today}\n\n
                    {top}'''
    # Send message to Telegram
    token = '1075693341:AAENYAcHF5KewVM-xYl6xVkQu2sj9YFkHdo'
    # chat_id = 127585XXXX  # my chat id

    message = message_telegram  # message which I want to send
    chats = [1275857904]
    url_get = 'https://api.telegram.org/bot1127113079:AAFeKXAd0ZtO6J7VLKXUOzYEoAawQEVeSEk/getUpdates'
    response = requests.get(url_get)

    # Add new users
    new_chats = []
    if len(response.json()['result']) != 0:
        for i in response.json()['result']:
            if i['message']['chat']['id'] not in new_chats:
                new_chats.append(i['message']['chat']['id'])
    try:
        with open('CHATS_C', 'r') as f:
            for line in f:
                chats.append(int(line.strip()))
        with open('CHATS_C', 'a') as f:
            for c in new_chats:
                if c not in chats:
                    chats.append(c)
                    f.write(str(c) + '\n')
    except:
        with open('CHATS_C', 'w') as f:
            for c in new_chats:
                if c not in chats:
                    chats.append(c)
                    f.write(str(c) + '\n')
                
    for chat in chats:
        params = {'chat_id': chat, 'text': message}

        base_url = f'https://api.telegram.org/bot{token}/'
        url = base_url + 'sendMessage?' + urlencode(params)

        #url_photo = base_url + 'sendPhoto'

        #files = {'photo': open('Add_Confirmed.png', 'rb')}
       # data = {'chat_id': chat}

        resp = requests.get(url)
        #r = requests.post(url_photo, files=files, data=data)
    print('Report send')


#t1 = PythonOperator(task_id='report_to_vk', python_callable=daily_report_to_vk, dag=dag)
t2 = PythonOperator(task_id='report_to_telegram', python_callable=daily_report_to_telegram, dag=dag)

#t1 >> t2
t2
