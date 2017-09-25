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

def query_content_weekly(content_id):
    return r.db('gettv_insight_api').table('content_by_week')\
        .filter({'inventoryID': content_id}).run()

def content_overview(dt, week_ucis, content_id, title):
    content_by_week = query_content_weekly(content_id)
    content_data = [(x['started-views'], int(x['viewing-time'] * x['weekly-active-user']))
        for x in content_by_week]
    total_views = sum([x[0] for x in content_data])
    # this is already measured in minutes
    total_viewtime = sum([x[1] for x in content_data])
    started_views_this_week = week_ucis.count()
    content_hour_of_day = normalize(view_count_by_hour_of_day(week_ucis), started_views_this_week)
    content_day_of_week = normalize(view_count_by_day_of_week(week_ucis), started_views_this_week)
    weekly_active_user = user_number(week_ucis)
    completion_ratio = avg_completion_ratio(week_ucis)
    user_viewtime = avg_user_viewtime(week_ucis)
    device_type = top_tag_by_view_count(week_ucis, 'device')
    res = {
        "id": content_id,
        'inventoryID': content_id,
        'title': title,
        "datetime": dt.replace(tzinfo=pytz.UTC),
        "content-total-viewing-time": total_viewtime,
        "content-total-started-views": total_views,
        "hour-of-day": content_hour_of_day,
        "day-of-week": content_day_of_week,
        "weekly-active-user": weekly_active_user,
        "content-completion": completion_ratio,
        "viewing-time": user_viewtime,
        "device-type": device_type,
    }
    print "#"*10
    print content_id
    print res
    r.db('gettv_insight_api').table('content_overview').insert([res], conflict='replace').run()



def content_overview_trigger(dt):
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_weekly_interactions(dt)
    for title in all_titles:
        content_ucis = content_overview(
            dt,
            week_ucis.filter(week_ucis.title==title),
            all_titles[title],
            title
        )


if __name__ == '__main__':
    dt = datetime(2017, 8, 30)
    content_overview_trigger(dt)
