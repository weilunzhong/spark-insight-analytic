from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
from diagnostic.calculation.user import *
from pyspark.sql import functions as func
from diagnostic.calculation.utils import normalize
from diagnostic.execution.utils import channel_ids
from datetime import datetime, timedelta
from componentmodels.models import TableRow, Table, NumberComponent
from pymongo import MongoClient
import rethinkdb as r
import pytz
import os


RDB_HOST = os.getenv('RDB_HOST', 'localhost')
RDB_PORT = os.getenv('RDB_PORT', 28015)
r.connect(host=RDB_HOST, port=RDB_PORT).repl()

def top_programs_in_vod(df, row_limit=20):
    top_programs = df\
        .filter(df.inventoryID.isNotNull())\
        .select('inventoryID', 'channelName')\
        .groupBy('inventoryID', 'channelName')\
        .count()\
        .orderBy(func.desc('count'))\
        .limit(row_limit)\
        .rdd\
        .collect()
    col = MongoClient().telenor.metadata
    top_programs = [
            {"inventoryID": x['inventoryID'], "channelName": x['channelName'], "viewCount": x['count']}
        for x in top_programs]
    res = []
    for d in top_programs:
        doc = col.find_one({'_id': d['inventoryID']})
        if doc:
            d['title'] = doc['title']
            res.append(d)
    return top_programs

def total_viewing_time(df):
    viewtime = df\
        .select('duration')\
        .agg(func.sum('duration'))\
        .first()  # return a list of dataframe
    return viewtime['sum(duration)'] / 60

def run_vod_kpis(ucis, view_type):
    started_views = view_count(ucis)
    week_ucis = ucis.filter(
        (dt_end-timedelta(days=6) < ucis.firstEvent) & (ucis.firstEvent <dt_end+timedelta(days=1))
    )
    week_ago_ucis = ucis.filter(
        (dt_end-timedelta(days=13) < ucis.firstEvent) & (ucis.firstEvent <dt_end-timedelta(days=6))
    )
    weekly_active_user = user_number(week_ucis)
    total_active_user = user_number(ucis)
    total_viewtime = total_viewing_time(ucis)
    user_viewtime = avg_user_viewtime(week_ucis)
    weekly_hibernation = user_hibernation(week_ucis, week_ago_ucis)
    top_program = top_programs_in_vod(ucis, 20)
    top_channel = normalize(top_tag_by_view_count(ucis, 'channelName'), started_views)
    hour_of_day = normalize(view_count_by_hour_of_day(ucis), started_views)
    day_of_week = normalize(view_count_by_day_of_week(ucis), started_views)
    tag_user_package, user_package = users_package_overview(ucis)
    package_overview = {"{} user".format(view_type):tag_user_package,
            "linear TV user": user_package}
    res = [
        {"title": 'started-views', "id": 'started-views', "started-views": started_views},
        {"title": 'weekly-active-user', "id": 'weekly-active-user', "weekly-active-user": weekly_active_user},
        {"title": 'total-active-user', "id": 'total-active-user', "total-active-user": total_active_user},
        {"title": 'total-viewing-time', "id": 'total-viewing-time', "total-viewing-time": total_viewtime},
        {"title": 'viewing-time', "id": 'viewing-time', "viewing-time": user_viewtime},
        {"title": 'user-hibernation', "id": 'user-hibernation', "user-hibernation": weekly_hibernation},
        {"title": 'top-programs', "id":  'top-programs', "data": top_program},
        {"title": 'top-provider', "id": 'top-provider', "data": top_channel},
        {"title": 'hour-of-day', "id": 'hour-of-day', "data": hour_of_day},
        {"title": 'day-of-week', "id": 'day-of-week', "data": day_of_week },
        {"title": 'package-overview', "id": 'package-overview', "data": package_overview}
    ]
    r.db('telenortv_insight_api').table(view_type).insert(res, conflict='replace').run()



if __name__ == '__main__':
    dt_start = datetime(2016, 12, 10)
    dt_end = datetime(2017, 6, 27)
    view_type = 'tvod'
    spark_io = SparkParquetIO()
    vod_ucis = spark_io.get_all_interactions()
    vod_ucis = vod_ucis.filter(vod_ucis.actionType==view_type)
    # week_ucis = vod_ucis.filter(
    #     (dt_end-timedelta(days=6) < vod_ucis.firstEvent) & (vod_ucis.firstEvent <dt_end+timedelta(days=1))
    # )
    # week_ago_ucis = vod_ucis.filter(
    #     (dt_end-timedelta(days=13) < vod_ucis.firstEvent) & (vod_ucis.firstEvent <dt_end-timedelta(days=6))
    # )
    run_vod_kpis(vod_ucis, view_type)
