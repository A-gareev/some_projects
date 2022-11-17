import io
import matplotlib.pyplot as plt
import pandas as pd
import pandahouse as ph
import seaborn as sns
import telegram

from airflow.decorators import dag, task
from datetime import date, datetime, timedelta

# Поключаюсь к боту
token = '5689194653:AAH82Xa08CIc6y3Cd2guKncM8vito3r3Iv4'
bot = telegram.Bot(token=token)
chat_id = '-817148946'

# Дата вчерашнего дня, пригодится при отправке сообщений
yesterday_date = date.today() - timedelta(days=1)
yesterday_date = yesterday_date.strftime(format='%d-%m-%Y')

# Данные для подлючения к кликхаус
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221020',
    'user': 'student',
    'password': 'dpo_python_2020'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-gareev-12',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 8)
}

# Интервал запуска DAG
schedule_interval = '59 10 * * *'  # Каждый день в 10:59 :) 

def get_application_metrics(chat_id=None):
    '''
    Функция принимает chat_id и отправляет в него текстовые
    сообщения с метриками приложения за вчерашний день
    
    Функция возвращает None
    '''
    # Считаю базовые показатели приложения
    query = '''
    /*
    Запрос выводит суммарную активность приложения за
    вчерашний день
    */
    SELECT SUM(activity) AS activity_sum
    FROM
    (SELECT COUNT(*) AS activity
    FROM simulator_20221020.feed_actions
    WHERE toDate(time) = toDate(today()) - 1

    UNION ALL 

    SELECT COUNT(*) AS activity
    FROM simulator_20221020.message_actions
    WHERE toDate(time) = toDate(today()) - 1)
    '''

    # Выполняю запрос
    both_activity = ph.read_clickhouse(query, connection=connection)
    both_activity = both_activity.unstack()[0]

    query = '''
    /*
    Запрос выводит DAU приложения за вчерашний день
    */
    SELECT length(arrayConcat(feed, msg)) -
        length(arrayIntersect(feed, msg)) AS DAU
    FROM

    (SELECT groupUniqArray(user_id) AS feed
    FROM simulator_20221020.feed_actions
    WHERE toDate(time) = toDate(today()) - 1) t1

    JOIN

    (SELECT groupUniqArray(user_id) AS msg
    FROM simulator_20221020.message_actions
    WHERE toDate(time) = toDate(today()) - 1) t2

    ON 1 = 1
    '''

    # Выполняю запрос
    both_dau = ph.read_clickhouse(query, connection=connection)
    both_dau = both_dau.unstack()[0]

    query = '''
    /*
    Запрос выводит количество новых пользователей приложения
    за вчерашний день
    */
    SELECT length(arrayConcat(feed, msg)) -
        length(arrayIntersect(feed, msg)) AS new_users
    FROM

    (SELECT groupUniqArray(user_id) AS feed
    FROM 
    (SELECT user_id
    FROM simulator_20221020.feed_actions
    GROUP BY user_id
    HAVING MIN(toDate(time)) = toDate(today()) - 1)) feed

    JOIN 

    (SELECT groupUniqArray(user_id) AS msg
    FROM 
    (SELECT user_id
    FROM simulator_20221020.message_actions
    GROUP BY user_id
    HAVING MIN(toDate(time)) = toDate(today()) - 1)) msg

    ON 1 = 1
    '''

    # Выполняю запросы
    both_new_users = ph.read_clickhouse(query, connection=connection)
    both_new_users = both_new_users.unstack()[0]

    base_metrics = '\n'.join([
        f'Отчет за {yesterday_date}', ' -' * 17,
        'Базовые метрики приложения', ' -' * 17,
        f'DAU приложения: {both_dau}',
        f'Новых пользователей: {both_new_users}',
        f'Активность: {both_activity}'
    ])

    # Считаю метрики в разрезе сервисов
    query = '''
    /*
    Запрос выводит DAU приложения в разрезе
    сервисов за предыдущий день
    */
    SELECT * FROM

    (SELECT uniq(user_id) AS feed_DAU
    FROM simulator_20221020.feed_actions
    WHERE toDate(time) = toDate(today()) - 1) feed

    JOIN

    (SELECT uniq(user_id) AS message_DAU
    FROM simulator_20221020.message_actions
    WHERE toDate(time) = toDate(today()) - 1) msg

    ON 1 = 1

    JOIN

    (SELECT uniq(user_id) AS message_DAU
    FROM simulator_20221020.message_actions
    WHERE user_id IN (
        SELECT DISTINCT user_id 
        FROM simulator_20221020.feed_actions
        WHERE toDate(time) = toDate(today()) - 1)
        AND toDate(time) = toDate(today()) - 1) AS both

    ON 1 = 1
    '''

    # Выполняю запросы
    current_day_dau = ph.read_clickhouse(
        query, connection=connection)

    prev_day_dau = ph.read_clickhouse(
        query.replace('- 1', '- 2'), connection=connection)

    prev_week_dau = ph.read_clickhouse(
        query.replace('- 1', '- 8'), connection=connection)

    current = current_day_dau.unstack()
    yesterday = prev_day_dau.unstack()
    prev_week = prev_week_dau.unstack()
    # Формирую сообщение
    dau = '\n'.join(
        ['DAU - активные пользователи',
         ' - - - - - - - - - - - - - - - - - ',
         f'Feed: {current[0]} {yesterday[0], prev_week[0]}',
         f'Msg: {current[1]} {yesterday[1], prev_week[1]}',
         f'Both: {current[2]} {yesterday[2], prev_week[2]}']
    )
    
    # Пишу sql запрос
    query = '''
    /*
    Запрос выводит активность пользователей в разрезе
    сервисов за предыдущий день
    */
    SELECT * FROM

    (SELECT COUNT(*) / uniq(user_id) AS feed_activity
    FROM simulator_20221020.feed_actions
    WHERE toDate(time) = toDate(today()) - 1) feed

    JOIN

    (SELECT COUNT(*) / uniq(user_id) AS msg_activity
    FROM simulator_20221020.message_actions
    WHERE toDate(time) = toDate(today()) - 1) msg

    ON 1 = 1

    JOIN

    (SELECT COUNT(*) / uniq(user_id) AS both_activity
    FROM (
        SELECT user_id 
        FROM simulator_20221020.message_actions
        WHERE toDate(time) = toDate(today()) - 1

        UNION ALL 

        SELECT user_id 
        FROM simulator_20221020.feed_actions
        WHERE toDate(time) = toDate(today()) - 1
    )) AS both

    ON 1 = 1
    '''

    # Выполняю запросы
    current_day_activity = ph.read_clickhouse(
        query, connection=connection)

    prev_day_activity = ph.read_clickhouse(
        query.replace('- 1', '- 2'), connection=connection)

    prev_week_activity = ph.read_clickhouse(
        query.replace('- 1', '- 8'), connection=connection)

    current = current_day_activity.round(2).unstack()
    yesterday = prev_day_activity.round(2).unstack()
    prev_week = prev_week_activity.round(2).unstack()
    # Формирую сообщение
    activity = '\n'.join(
        ['Средняя активность пользователей',
         ' - - - - - - - - - - - - - - - - - ',
         f'Feed: {current[0]} {yesterday[0], prev_week[0]}',
         f'Msg: {current[1]} {yesterday[1], prev_week[1]}',
         f'Both: {current[2]} {yesterday[2], prev_week[2]}']
    )
    
    # Пишу sql запрос
    query = '''
    /*
    Запрос выводит количество новых пользователей в разрезе
    сервисов за предыдущий день
    */
    SELECT length(feed_news) AS feed_new_users,
        length(msg_news) AS msg_new_users,
        length(arrayIntersect(feed_news, msg_news)) AS both_news
    FROM

    (SELECT groupUniqArray(user_id) AS feed_news FROM
    (SELECT user_id
    FROM simulator_20221020.feed_actions
    GROUP BY user_id
    HAVING MIN(toDate(time)) = toDate(today()) - 1)) feed

    JOIN

    (SELECT groupUniqArray(user_id) AS msg_news FROM
    (SELECT user_id
    FROM simulator_20221020.message_actions
    GROUP BY user_id
    HAVING MIN(toDate(time)) = toDate(today()) - 1)) msg

    ON 1 = 1
    '''

    # Выполняю запросы
    current_day_new = ph.read_clickhouse(
        query, connection=connection)
    prev_day_new = ph.read_clickhouse(
        query.replace('- 1', '- 2'), connection=connection)

    prev_week_new = ph.read_clickhouse(
        query.replace('- 1', '- 8'), connection=connection)

    current = current_day_new.unstack()
    yesterday = prev_day_new.unstack()
    prev_week = prev_week_new.unstack()
    # Формирую сообщение
    new_users = '\n'.join(
        ['Количество новых пользователей',
         ' - - - - - - - - - - - - - - - - - ',
         f'Feed: {current[0]} {yesterday[0], prev_week[0]}',
         f'Msg: {current[1]} {yesterday[1], prev_week[1]}',
         f'Both: {current[2]} {yesterday[2], prev_week[2]}']
    )
    
    # Формирую главное сообщение
    main_msg = '\n'.join([
    'Метрики приложения по сервисам.',
    '(В скобках для сравнения '
    'указаны значения день/неделю назад.)'
    ])
    sep = '\n' + ' -' * 17 + ' \n'
    full_message = sep.join([base_metrics, main_msg, dau, new_users, activity])
    return full_message
    
    
def get_application_plot():
    '''
    Функция формирует график по метриками приложения
    
    Функция возвращает plot object
    '''
    query = '''
    /*
    Запрос выводит retention сервисов приложения
    для пришедших неделю назад пользователей
    */
    SELECT *,
        'feed' AS service
    FROM

    (SELECT toDate(time) AS day,
        uniq(user_id) AS retained
    FROM simulator_20221020.feed_actions
    WHERE day < toDate(today())
        AND user_id IN(
        SELECT user_id
        FROM simulator_20221020.feed_actions
        GROUP BY user_id
        HAVING MIN(toDate(time)) = toDate(today()) - 7
    )
    GROUP BY day) AS feed
    ORDER BY day

    UNION ALL

    SELECT *,
        'msg' AS service
    FROM

    (SELECT toDate(time) AS day,
        uniq(user_id) AS retained
    FROM simulator_20221020.message_actions
    WHERE day < toDate(today())
        AND user_id IN(
        SELECT user_id
        FROM simulator_20221020.message_actions
        GROUP BY user_id
        HAVING MIN(toDate(time)) = toDate(today()) - 7
    )
    GROUP BY day) AS msg
    ORDER BY day
    '''

    cur_retention = ph.read_clickhouse(query, connection=connection)

    query = '''
    /*
    Запрос выводит retention сервисов приложения'
    для пришедших неделю назад пользователей
    */
    SELECT *,
        'feed' AS service
    FROM

    (SELECT toDate(time) AS day,
        uniq(user_id) AS retained
    FROM simulator_20221020.feed_actions
    WHERE day < toDate(today()) - 7
        AND day > toDate(today()) - 15
        AND user_id IN(
        SELECT user_id
        FROM simulator_20221020.feed_actions
        GROUP BY user_id
        HAVING MIN(toDate(time)) = toDate(today()) - 14
    )
    GROUP BY day) AS feed
    ORDER BY day

    UNION ALL

    SELECT *,
        'msg' AS service
    FROM

    (SELECT toDate(time) AS day,
        uniq(user_id) AS retained
    FROM simulator_20221020.message_actions
    WHERE day < toDate(today()) - 7
        AND day > toDate(today()) - 15
        AND user_id IN(
        SELECT user_id
        FROM simulator_20221020.message_actions
        GROUP BY user_id
        HAVING MIN(toDate(time)) = toDate(today()) - 14
    )
    GROUP BY day) AS msg
    ORDER BY day
    '''

    prev_retention = ph.read_clickhouse(query, connection=connection)
    cur_retention['short_date'] =\
        cur_retention['day'].dt.strftime('%d-%m')
    prev_retention['short_date'] = cur_retention['short_date']
    cur_retention['week'] = 'current'
    prev_retention['week'] = 'previous'
    retention = pd.concat([cur_retention, prev_retention], axis=0)
    divs = retention['retained'][::7].to_list()
    ret = []

    # Считаю процентные значения retention
    for i in range(4):
        for j in range(7):
            x = retention.iloc[i * 7 + j, 1] / divs[i]
            ret.append(round(x, 4))

    retention['norm_retention'] = ret

    query = '''
    /*
    Запрос выводит количество новых пользователей
    по сервисам приложения в разрезе источника трафика
    */
    SELECT start_date,
        SUM(source = 'ads') AS ads,
        SUM(source = 'organic') AS organic
    FROM
    (SELECT user_id, source,
        MIN(toDate(time)) AS start_date
    FROM simulator_20221020.feed_actions
    GROUP BY user_id, source) AS t1
    WHERE start_date = toDate(today()) - 1
        OR start_date = toDate(today()) - 2
        OR start_date = toDate(today()) - 8
    GROUP BY start_date
    ORDER BY start_date
    '''

    # Выполняю запрос для ленты новостей
    new_users_feed = ph.read_clickhouse(
        query, connection=connection
    )

    # Выполняю запрос для сообщений
    new_users_msg = ph.read_clickhouse(
        query.replace(
            'feed_actions','message_actions'
        ), connection=connection
    )

    # Поменяю даты на понятные названия
    day_titles = ['вчера', 'позавчера', 'неделю назад']
    new_users_msg['start_date'] = day_titles
    new_users_feed['start_date'] = day_titles

    query = '''
    /*
    Запрос выводит дневную активность пользователей
    приложения в разрезе ОС за вчерашний день 
    */
    SELECT toStartOfHour(time) as time,
        os, uniq(user_id) AS active_users,
        'feed' AS service
    FROM simulator_20221020.feed_actions
    WHERE toDate(time) = toDate(today()) - 1
    GROUP BY time, os
    ORDER BY time

    UNION ALL 

    SELECT toStartOfHour(time) as time,
        os, uniq(user_id) AS active_users,
        'msg' AS service
    FROM simulator_20221020.message_actions
    WHERE toDate(time) = toDate(today()) - 1
    GROUP BY time, os
    ORDER BY time
    '''

    daily_active = ph.read_clickhouse(query, connection=connection)
    daily_active['time'] = daily_active['time'].dt.hour
    # Создаю plot object
    plot_object = io.BytesIO()
    # Визуализирую результаты
    sns.set_theme()
    fig = plt.figure(figsize=(12, 12), dpi=300)
    plot_object = io.BytesIO()
    grid = plt.GridSpec(3, 4, hspace=0.2, wspace=0.2)
    ax1 = fig.add_subplot(grid[:1, :2])
    ax2 = fig.add_subplot(grid[:1, 2:])
    ax3 = fig.add_subplot(grid[1:2, :])
    ax4 = fig.add_subplot(grid[2:, :])
    colors = sns.color_palette('viridis')

    # График новых пользователей новостей в разрезе ОС
    new_users_feed.set_index('start_date').plot(
        kind='bar', stacked=True,
        color=[colors[0], colors[3]], ax=ax1)

    # График новых пользователей сообщений в разрезе ОС
    new_users_msg.set_index('start_date').plot(
        kind='bar', stacked=True, 
        color=[colors[0], colors[3]], ax=ax2)

    # График Retention в разрезе сервисов
    sns.lineplot(data=retention, x='short_date', y='norm_retention',
                 palette='viridis', linewidth=2, hue='service',
                 style='week', ax=ax3)

    # График дневной активности в разрезе сервисов
    sns.lineplot(data=daily_active, x='time', y='active_users',
                 palette='viridis', linewidth=2, hue='service',
                 style='os', ax=ax4)

    # Названия графиков
    titles = ['Каналы привлечения пользователей новостей',
              'Каналы привлечения пользователей сообщений',
              '7-дневный Retention по сервисам приложения',
              'Количество активных пользователей в течение дня']

    for i, ax in enumerate([ax1, ax2, ax3, ax4]):
        ax.tick_params('x', labelrotation=0)
        ax.set_xlabel('')
        ax.set_ylabel('')
        ax.title.set_text(titles[i])

    ax4.set_xlabel('Время')

    # Общее название графика    
    fig.suptitle(f'Метрики приложения за {yesterday_date}', fontsize=20)    
    plt.savefig(plot_object)
    # Задаю имя графика с текущей датой
    plot_object.name = f'{yesterday_date}_app.png'
    plot_object.seek(0)
    plt.close()
    return plot_object
   
    
# Создаю dag для автоматизации
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)  
def agareev_app_report_dag():
    
    @task
    def daily_report():
        full_message = get_application_metrics()
        plot_object = get_application_plot()
        # Отправляю сообщение
        bot.sendMessage(chat_id=chat_id, text=full_message)
         # Отправляю сообщение в чат
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    daily_report()
    
    
agareev_app_report_dag = agareev_app_report_dag()
