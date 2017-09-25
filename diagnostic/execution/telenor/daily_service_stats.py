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
    date_string = timestamp.strftime('%Y-%m-%d')
    started_views = view_count(daily_ucis)
    daily_active_user = user_number(daily_ucis)
    user_viewtime = avg_user_viewtime(daily_ucis)
    views_by_action = normalize(action_type_view_count(daily_ucis), started_views)
    complete_views = avg_finished_program_by_user(daily_ucis)
    completion_ratio = avg_completion_ratio(daily_ucis)
    daily_top_program = top_programs_by_view_count(daily_ucis, 10)
    daily_top_genre = normalize(top_tag_by_view_count(daily_ucis, 'category', 10), started_views)
    daily_top_channel = normalize(top_tag_by_view_count(daily_ucis, 'channelName', 10), started_views)
    week_ucis = spark_io.get_weekly_interactions(timestamp)
    last_week_ucis = spark_io.get_weekly_interactions(timestamp - timedelta(days=7))
    weekly_hibernation = user_hibernation(week_ucis, last_week_ucis)
    new_users = weekly_new_user(users, timestamp)
    total_users = unique_user(users, timestamp)
    res = {
        "id": date_string,
        "datetime": timestamp.replace(tzinfo=pytz.UTC),
        "new-user": new_users,
        "unique-user": total_users,
        "viewing-time": user_viewtime,
        "daily-active-user": daily_active_user,
        "started-views": started_views,
        "views-by-action": views_by_action,
        "complete-views": complete_views,
        "content-completion": completion_ratio,
        "user-hibernation": weekly_hibernation,
        "top-programs": daily_top_program,
        "top-genres": daily_top_genre,
        "top-channels": daily_top_channel,
    }
    print res
    r.db('telenortv_insight_api')\
        .table('by_day')\
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
    dt_start = datetime(2016, 12, 10)
    dt_end = datetime(2017, 6, 27)
    daily_trigger(dt_start, dt_end)

