from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
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
            {"inventoryID": x['inventoryID'], "channelName": x['channelName'], "count": x['count']}
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

def run_svod_kpis(ucis, week_ucis, weeks_btn):
    started_views = view_count(ucis)
    weekly_active_user = user_number(week_ucis)
    total_active_user = user_number(ucis)
    total_viewtime = total_viewing_time(ucis)
    user_viewtime = avg_user_viewtime(ucis) / weeks_btn
    top_program = top_programs_in_vod(ucis, 20)
    top_channel = normalize(top_tag_by_view_count(ucis, 'channelName'), started_views, 'count')
    hour_of_day = normalize(view_count_by_hour_of_day(ucis), started_views, 'count')
    day_of_week = normalize(view_count_by_day_of_week(ucis), started_views, 'count')
    res = [
        {"title": 'started-views', "id": 'started-views', "data": started_views},
        {"title": 'weekly-active-user', "id": 'weekly-active-user', "data": weekly_active_user},
        {"title": 'total-active-user', "id": 'total-active-user', "data": total_active_user},
        {"title": 'total-viewing-time', "id":  'total-viewing-time', "data": total_viewtime},
        {"title": 'user-viewing-time', "id":  'user-viewing-time', "data": user_viewtime},
        {"title": 'top-program', "id":  'top-program', "data": top_program},
        {"title": 'top-provider', "id":  'top-provider', "data": top_channel},
        {"title": 'typical-day', "id":  'typical-day', "data": hour_of_day},
        {"title": 'day-of-week', "id":  'day-of-week', "data": day_of_week }
    ]
    r.db('telenortv_insight_api').table('tvod').insert(res, conflict='replace').run()



if __name__ == '__main__':
    dt_start = datetime(2016, 12, 10)
    dt_end = datetime(2017, 6, 27)
    weeks_btn = (dt_end - dt_start).days / 7.0
    spark_io = SparkParquetIO()
    svod_ucis = spark_io.get_interactions(dt_start, dt_end)
    svod_ucis = svod_ucis.filter(svod_ucis.actionType=='tvod')
    week_ucis = svod_ucis.filter(
        (dt_end-timedelta(days=6) <svod_ucis.firstEvent) & (svod_ucis.firstEvent <dt_end+timedelta(days=1))
    )
    run_svod_kpis(svod_ucis, week_ucis, weeks_btn)
