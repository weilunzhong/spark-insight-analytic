from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
from diagnostic.calculation.user import *
from diagnostic.calculation.utils import normalize, float_devision, finished
from datetime import datetime, timedelta
from pyspark.sql import functions as func
import rethinkdb as r
import pytz
import os
import re


RDB_HOST = os.getenv('RDB_HOST', 'localhost')
RDB_PORT = os.getenv('RDB_PORT', 28015)
r.connect(host=RDB_HOST, port=RDB_PORT).repl()
spark_io = SparkParquetIO()

def init_tag_doc_template(timestamp, tag_name, tag_string):
    date_string = timestamp.strftime('%Y-%m-%d')
    tagID = tag_string.replace("(", "").replace(")", "").strip().lower().replace(' ', '-')
    res = {
        "id": date_string + '-' + tagID,
        "tag": tagID, #should be indexed
        "datetime": timestamp.replace(tzinfo=pytz.UTC), # should be indexed
        "daily-active-user": 0,
        "started-views": 0,
        "complete-views": 0,
        "views-with-runtime": 0,
        "playtime": 0,
        "hour-of-day": [0]*24,
    }
    return res

def update_dict(d, key, line, agg_name):
    if key not in d:
        raise KeyError("Field {0} is not in dict.", format(key))
    d[key] = line[agg_name]

def update_hourly_dict(d, key, line):
    if key not in d:
        raise KeyError("Field {0} is not in dict.", format(key))
    d[key][int(line['hour'])] = line['count']

def run_tags(daily_ucis, timestamp, tag_name):
    daily_ucis = daily_ucis.filter(daily_ucis[tag_name].isNotNull())
    all_tags = daily_ucis.groupBy(daily_ucis[tag_name]).count().collect()
    all_tags = [x[tag_name] for x in all_tags]
    daily_tag_docs = {tag_string: init_tag_doc_template(timestamp, tag_name, tag_string) for tag_string in all_tags}

    started_views = daily_ucis.groupBy(daily_ucis[tag_name]).count().collect()
    [update_dict(daily_tag_docs[x[tag_name]], 'started-views', x, 'count') for x in started_views]
    daily_active_user = daily_ucis.groupBy(daily_ucis[tag_name])\
        .agg(func.countDistinct('userID')).collect()

    [update_dict(daily_tag_docs[x[tag_name]], 'daily-active-user', x, 'count(DISTINCT userID)') for x in daily_active_user]
    play_time = daily_ucis.groupBy(daily_ucis[tag_name])\
        .agg(func.sum('duration')).collect()
    [update_dict(daily_tag_docs[x[tag_name]], 'playtime', x, 'sum(duration)') for x in play_time]

    complete_views = daily_ucis\
        .filter(daily_ucis.runtime.isNotNull())\
        .filter(finished(daily_ucis.duration, daily_ucis.runtime))\
        .groupBy(daily_ucis[tag_name])\
        .count().collect()
    [update_dict(daily_tag_docs[x[tag_name]], 'complete-views', x, 'count') for x in complete_views]

    runtime_views = daily_ucis\
        .filter(daily_ucis.runtime.isNotNull())\
        .groupBy(daily_ucis[tag_name])\
        .count().collect()
    [update_dict(daily_tag_docs[x[tag_name]], 'views-with-runtime', x, 'count') for x in runtime_views]

    hour_of_day = daily_ucis\
        .groupBy(daily_ucis[tag_name], func.hour('firstEvent').alias('hour'))\
        .count().collect()
    [update_hourly_dict(daily_tag_docs[x[tag_name]], 'hour-of-day', x) for x in hour_of_day]
    res =[daily_tag_docs[k] for k in  daily_tag_docs]
    print res
    r.db('gettv_insight_test')\
        .table(tag_name)\
        .insert(res, conflict='replace').run()
    print "#"*10

def run_daily_kpis(timestamp, tag_name):
    daily_ucis = spark_io.get_daily_interactions(timestamp)
    run_tags(daily_ucis, timestamp, tag_name)


def daily_trigger(start_date, end_date, tag_name):
    dt = start_date
    while dt <= end_date:
        run_daily_kpis(dt, tag_name)
        dt += timedelta(days=1)

if __name__ == '__main__':
    dt_start = datetime(2017, 4, 1)
    dt_end = datetime(2017, 9, 7)
    tag_name = 'device'
    daily_trigger(dt_start, dt_end, tag_name)

