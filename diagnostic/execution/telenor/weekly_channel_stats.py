from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
from diagnostic.calculation.utils import normalize
from diagnostic.execution.utils import channel_ids
from datetime import datetime, timedelta
import rethinkdb as r
import pytz
import os


RDB_HOST = os.getenv('RDB_HOST', 'localhost')
RDB_PORT = os.getenv('RDB_PORT', 28015)
r.connect(host=RDB_HOST, port=RDB_PORT).repl()


def run_daily_kpis(timestamp, week_ucis, channel_id):
    date_string = timestamp.strftime('%Y-%m-%d')
    started_views = view_count(week_ucis)
    weekly_active_user = user_number(week_ucis)
    user_viewtime = avg_user_viewtime(week_ucis)
    views_by_action = normalize(action_type_view_count(week_ucis), started_views, 'count')
    complete_views = avg_finished_program_by_user(week_ucis)
    completion_ratio = avg_completion_ratio(week_ucis)
    weekly_top_program = top_programs_by_view_count(week_ucis, 10)
    weekly_top_genre = normalize(top_genre_by_view_count(week_ucis, 10), started_views, 'count')
    res = {
        "id": date_string + '_' +channel_id,
        'channelID': channel_id,
        "datetime": timestamp.replace(tzinfo=pytz.UTC),
        "viewing-time": user_viewtime,
        "weekly-active-user": weekly_active_user,
        "started-views": started_views,
        "comlete-views": complete_views,
        "content-completion": completion_ratio,
        "top-programs": weekly_top_program,
        "top-genres": weekly_top_genre
    }
    print "#"*10
    print channel_id
    print res
    r.db('telenortv_insight_api').table('channel_by_week').insert([res]).run()
    print date_string
    print "#"*10

def channel_trigger(dt):
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_weekly_interactions(dt)
    for channel_id in channel_ids:
        run_daily_kpis(
            dt,
            week_ucis.filter(week_ucis.channelID==channel_id),
            channel_id
        )

def weekly_channel_trigger(start_date, end_date):
    dt = start_date
    while dt <= end_date:
        print "#"*10
        print dt
        channel_trigger(dt)
        dt += timedelta(days=7)


if __name__ == '__main__':
    dt_start = datetime(2016, 12, 10)
    dt_end = datetime(2017, 6, 23)
    weekly_channel_trigger(dt_start, dt_end)

