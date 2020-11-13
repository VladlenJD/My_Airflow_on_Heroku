from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from urllib.parse import urlencode
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import seaborn as sns
import os
import psycopg2


default_args = {
    'owner': 'vl.stratu',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 0
}

dag = DAG('daily_report_for_COVID_19_graph',
          default_args=default_args,
          catchup=False,
          schedule_interval='05 06,12 * * *')


# Read timeseries_confirmed_global

confirmed = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')

# Let's take the last 2 weeks
confirmed_by_days = confirmed[confirmed.columns[-15:]]

# Beautiful charts settings

plt.style.use('ggplot')                       # Красивые графики
plt.rcParams['figure.figsize'] = (30, 20)

# Dataframe for vizualization

viz = confirmed_by_days.sum().to_frame('confirmed_viz').reset_index(drop=False)
viz = viz.rename(columns={'index': 'date'})
vizual = pd.DataFrame({'date': viz.date, 'confirmed_count': viz.confirmed_viz - viz.confirmed_viz.shift(1)})
vizual.drop(0, inplace=True)
vizual.date = pd.to_datetime(vizual.date)
vizual['date'] = vizual['date'].dt.strftime('%d-%b')


fig = plt.figure()
fig.patch.set_facecolor('#CCFFFF')
plt.title('Growth by World', fontsize=100, pad=60)

# Building a barplot

ax = sns.barplot(x = vizual.date, y = 'confirmed_count', data = vizual, palette='ch:s=-.2,r=.6')

plt.subplots_adjust(left=0.10, right=0.95, hspace=0.25, wspace=0.35)

plt.xlabel('Daily Cases', fontsize=80, labelpad=50)
plt.ylabel('Confirmed', fontsize=60, labelpad=30)

plt.xticks(fontsize=30)
plt.yticks(fontsize=30)

ax.yaxis.set_major_formatter(FuncFormatter(lambda y, p: '{:,g}k'.format(y/1000)))


ax.set_facecolor('#CCFFFF')
ax.grid(color='grey', linewidth=2, linestyle='--')

fig.savefig('Add_Confirmed', facecolor=fig.get_facecolor())

ax.set_frame_on(False)


# Report to Telegram

def daily_report_to_telegram():

    token = '1075693341:AAENYAcHF5KewVM-xYl6xVkQu2sj9YFkHdo'
    # chat_id = 127585XXXX  # my chat id

    chats = [1275857904]
    url_get = 'https://api.telegram.org/bot1075693341:AAENYAcHF5KewVM-xYl6xVkQu2sj9YFkHdo/getUpdates'
    response = requests.get(url_get)

    # Add new users
    # new_chats = []
    # if len(response.json()['result']) != 0:
    #     for i in response.json()['result']:
    #         if i['message']['chat']['id'] not in new_chats:
    #             new_chats.append(i['message']['chat']['id'])
    # try:
    #     with open('CHATS_C', 'r') as f:
    #         for line in f:
    #             chats.append(int(line.strip()))
    #     with open('CHATS_C', 'a') as f:
    #         for c in new_chats:
    #             if c not in chats:
    #                 chats.append(c)
    #                 f.write(str(c) + '\n')
    # except:
    #     with open('CHATS_C', 'w') as f:
    #         for c in new_chats:
    #             if c not in chats:
    #                 chats.append(c)
    #                 f.write(str(c) + '\n')

    for chat in chats:

        base_url = f'https://api.telegram.org/bot{token}/'

        url_photo = base_url + 'sendPhoto'

        files = {'photo': open('Add_Confirmed.png', 'rb')}
        data = {'chat_id': chat}

        r = requests.post(url_photo, files=files, data=data)
    print('Report send')


# t1 = PythonOperator(task_id='report_to_vk', python_callable=daily_report_to_vk, dag=dag)
t2 = PythonOperator(task_id='report_to_telegram', python_callable=daily_report_to_telegram, dag=dag)

