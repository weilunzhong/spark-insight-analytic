from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
from diagnostic.calculation.utils import normalize
from diagnostic.execution.gettv.all_titles import all_titles
from datetime import datetime, timedelta
import rethinkdb as r
import pytz
import os


RDB_HOST = os.getenv('RDB_HOST', 'localhost')
RDB_PORT = os.getenv('RDB_PORT', 28015)
r.connect(host=RDB_HOST, port=RDB_PORT).repl()


def run_weekly_kpis(timestamp, week_ucis, content_id, title):
    date_string = timestamp.strftime('%Y-%m-%d')
    started_views = view_count(week_ucis)
    print started_views, "started views"
    weekly_active_user = user_number(week_ucis)
    user_viewtime = avg_user_viewtime(week_ucis)
    views_by_action = normalize(action_type_view_count(week_ucis), started_views)
    complete_views = avg_finished_program_by_user(week_ucis)
    completion_ratio = avg_completion_ratio(week_ucis)
    res = {
        "id": date_string + '_' + content_id,
        "title": title,
        'inventoryID': content_id,
        "datetime": timestamp.replace(tzinfo=pytz.UTC),
        "viewing-time": user_viewtime,
        "weekly-active-user": weekly_active_user,
        "started-views": started_views,
        "comlete-views": complete_views,
        "content-completion": completion_ratio,
    }
    print "#"*10
    print content_id
    print res
    r.db('gettv_insight_api').table('content_by_week').insert([res], conflict='replace').run()
    print date_string
    print "#"*10

def content_trigger(dt):
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_weekly_interactions(dt)
    for title in all_titles:
        print title
        run_weekly_kpis(
            dt,
            week_ucis.filter(week_ucis.title==title),
            all_titles[title],
            title
        )

def weekly_content_trigger(start_date, end_date):
    dt = start_date
    while dt <= end_date:
        print "#"*10
        print dt
        content_trigger(dt)
        dt += timedelta(days=7)


if __name__ == '__main__':
    dt_start = datetime(2017, 9, 1)
    dt_end = datetime(2017, 9, 1)
    weekly_content_trigger(dt_start, dt_end)

