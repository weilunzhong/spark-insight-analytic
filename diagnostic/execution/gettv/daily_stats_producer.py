from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
from diagnostic.calculation.user import *
from diagnostic.calculation.utils import normalize
from datetime import datetime, timedelta
import rethinkdb as r
import pytz
import os


RDB_HOST = os.getenv('RDB_HOST', 'localhost')
RDB_PORT = os.getenv('RDB_PORT', 28015)
r.connect(host=RDB_HOST, port=RDB_PORT).repl()
spark_io = SparkParquetIO()


def run_daily_kpis(timestamp, users):
    daily_ucis = spark_io.get_daily_interactions(timestamp)
    week_ucis = spark_io.get_weekly_interactions(timestamp)
    last_week_ucis = spark_io.get_weekly_interactions(timestamp - timedelta(days=7))
    date_string = timestamp.strftime('%Y-%m-%d')
    started_views = view_count(daily_ucis)
    daily_active_user = user_number(daily_ucis)
    weekly_active_user = user_number(week_ucis)
    user_viewtime = avg_user_viewtime(daily_ucis)
    play_time = user_viewtime * daily_active_user
    user_complete_views = avg_finished_program_by_user(daily_ucis)
    # completion_ratio = avg_completion_ratio(daily_ucis)
    complete_views = completed_views(daily_ucis)
    runtime_views = views_with_runtime(daily_ucis)
    weekly_hibernation = user_hibernation(week_ucis, last_week_ucis)
    new_users = weekly_new_user(users, timestamp)
    total_users = unique_user(users, timestamp)
    hour_of_day = view_count_by_hour_of_day(daily_ucis)
    res = {
        "id": date_string,
        "tag": "service",
        "datetime": timestamp.replace(tzinfo=pytz.UTC),
        "new-user": new_users,
        "unique-user": total_users,
        "viewing-time": user_viewtime,
        "daily-active-user": daily_active_user,
        "weekly-active-user": weekly_active_user,
        "started-views": started_views,
        "user-complete-views": user_complete_views,
        "playtime": play_time,
        "complete-views": complete_views,
        "views-with-runtime": runtime_views,
        "user-hibernation": weekly_hibernation,
        "hour-of-day": hour_of_day,
    }
    print res
    r.db('gettv_insight_test')\
        .table('service')\
        .insert([res], conflict='replace').run()
    print date_string
    print "#"*10

def daily_trigger(start_date, end_date):
    users = spark_io.get_users()
    dt = start_date
    while dt <= end_date:
        run_daily_kpis(dt, users)
        dt += timedelta(days=1)

if __name__ == '__main__':
    dt_start = datetime(2017, 4, 1)
    dt_end = datetime(2017, 9, 7)
    daily_trigger(dt_start, dt_end)

