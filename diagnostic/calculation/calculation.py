from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.utils import *
from pyspark.sql import functions as func
from datetime import datetime
from itertools import groupby as gb
from functional import seq


__all__ = [
    'view_count', 'user_number', 'avg_user_viewtime',
    'avg_user_view_count', 'avg_finished_program_by_user',
    'avg_completion_ratio', 'action_type_view_count',
    'user_hibernation', 'new_user', 'top_programs_by_view_count',
    'top_tag_by_view_count', 'top_tag_by_total_viewtime',
    'view_count_by_hour_of_day', 'view_count_by_day_of_week',
    'top_tag_by_user_viewtime', 'user_by_complete_views',
    'user_by_viewtime', 'top_tag_by_completion_ratio'
]

def view_count(df):
    return df.count()

def user_number(df):
    user_num = df.select('userID').distinct().count()
    return user_num

def avg_user_viewtime(df):
    # measured in minutes
    user_viewing_time = df\
        .select('duration')\
        .agg(func.sum('duration'))\
        .first()  # return a list of dataframe
    total_viewing_time =  user_viewing_time['sum(duration)']
    return float_devision(total_viewing_time, (user_number(df) * 60))

def avg_user_view_count(df):
    return float_devision(view_count(df), user_number(df))

def avg_finished_program_by_user(df):
    finished_program = df\
        .filter(df.runtime.isNotNull())\
        .filter(finished(df.duration, df.runtime))\
        .count()
    return float_devision(finished_program, user_number(df))

def avg_completion_ratio(df):
    completion_titles = df\
        .select('duration', 'runtime')\
        .filter(df.runtime.isNotNull())\
        .filter(finished(df.duration, df.runtime))\
        .count()
    titles_with_runtime = df\
        .select('duration', 'runtime')\
        .filter(df.runtime.isNotNull())\
        .count()
    return float_devision(completion_titles, titles_with_runtime)

def user_by_complete_views(df):
    hist = df\
        .filter(df.runtime.isNotNull())\
        .filter(finished(df.duration, df.runtime))\
        .groupBy('userID')\
        .count()\
        .select(func.col('count').alias('userViews'))\
        .groupBy('userViews')\
        .count()\
        .collect()
    program_count_hist = [(program_count_mapper(x['userViews']), x['count']) for x in hist]
    res = seq(program_count_hist)\
        .group_by(lambda(category, count): category)\
        .map(lambda (category, count_list): (category, sum([x[1] for x in count_list])))\
        .order_by(lambda (category, count): category)\
        .map(lambda(category, count):
            {"category": counter_mapper_doc[category], "count": count}
        )\
        .to_list()
    return res

def user_by_viewtime(df, interval):
    # NOTE that interval must be send to ensure the mapper works
    hist = df\
        .groupBy('userID')\
        .agg(func.sum('duration'))\
        .select(func.col('sum(duration)').alias('viewtime'))\
        .collect()
    user_viewtime = seq([x['viewtime'] / interval for x in hist])\
        .map(lambda x: viewtime_mapper(x))\
        .group_by(lambda x: x)\
        .map(lambda (category, users): (category, len(users)))\
        .order_by(lambda (category, count): category)\
        .map(lambda (category, count):
                {"category": viewtime_mapper_doc[category], "count": count}
        )\
        .to_list()
    return user_viewtime


def top_tag_by_view_count(df, tag_name, row_limit=10):
    top_tag = df\
        .select(tag_name)\
        .groupBy(tag_name)\
        .count()\
        .orderBy(func.desc('count'))\
        .limit(row_limit)\
        .rdd\
        .collect()
    top_tag = [
        {tag_name: x[tag_name], "count": x['count']}
        for x in top_tag]
    return top_tag

def top_tag_by_total_viewtime(df, tag_name, row_limit=10):
    top_tag = df\
        .select(tag_name, 'duration')\
        .groupBy(tag_name)\
        .agg(func.sum('duration'))\
        .sort(func.desc('sum(duration)'))\
        .limit(row_limit)\
        .rdd.collect()
    top_tag = [
        {tag_name: x[tag_name], "viewtime": x['sum(duration)'] / 60}
        for x in top_tag]
    return top_tag

def top_tag_by_completion_ratio(df, tag_name, row_limit=10, tag_count_limit=100):
    tag_by_views = df\
        .filter(df.runtime.isNotNull())\
        .groupBy(tag_name)\
        .count()\
        .rdd\
        .collectAsMap()
    tag_by_finished_views = df\
        .filter(df.runtime.isNotNull())\
        .filter(finished(df.duration, df.runtime))\
        .groupBy(tag_name)\
        .count()\
        .collect()
    res = [
        {
            tag_name: x[tag_name],
            'contentCompletion': float_devision(x['count'], tag_by_views[x[tag_name]])
        } for x in tag_by_finished_views if x['count'] > tag_count_limit
    ]
    return sorted(res, key=lambda x: x['contentCompletion'], reverse=True)[:row_limit]

def top_tag_by_user_viewtime(df, tag_name, row_limit=10):
    top_channel = df\
        .groupBy('userID', tag_name)\
        .sum('duration')\
        .groupBy(tag_name)\
        .avg('sum(duration)')\
        .sort(func.desc('avg(sum(duration))'))\
        .limit(row_limit)\
        .rdd.collect()
    top_channel = [
        {tag_name: x[tag_name], "viewtime": x['avg(sum(duration))'] / 60}
        for x in top_channel]
    return top_channel

def top_programs_by_view_count(df, row_limit=10):
    top_programs = df\
        .filter(df.title.isNotNull())\
        .select('title', 'channelName')\
        .groupBy('channelName', 'title')\
        .count()\
        .orderBy(func.desc('count'))\
        .limit(row_limit)\
        .rdd\
        .collect()
    top_programs = [
        {"title": x['title'], "channelName": x['channelName'], "count": x['count']}
        for x in top_programs]
    return top_programs

def action_type_view_count(df):
    action_type = df\
        .select('actionType')\
        .groupBy('actionType')\
        .count()\
        .orderBy(func.desc('count'))\
        .rdd.collect()
    action_type = [
        {"actionType": x['actionType'], "count": x['count']}
        for x in action_type]
    return action_type

def view_count_by_hour_of_day(df):
    hour_of_day = df\
        .groupBy(func.hour('firstEvent').alias('hour'))\
        .count()\
        .sort('hour')\
        .collect()
    hourly_bucket = [{'hour':x, 'count': 0} for x in range(24)]
    for d in hour_of_day:
        hourly_bucket[d['hour']]['count'] = d['count']
    return hourly_bucket

def view_count_by_day_of_week(df):
    day_of_week = df\
        .groupBy(func.date_format('firstEvent', 'E').alias('weekday'))\
        .count()\
        .collect()
    week_bucket = {x: 0 for x in WEEKDAYS}
    for d in day_of_week:
        week_bucket[d['weekday']] = d['count']
    day_of_week = [{'weekday': x, 'count': week_bucket[x]} for x in WEEKDAYS]
    return day_of_week

def user_hibernation(df, pre_df):
    # df and pre_df should have the same days of interaction in it
    users = df.select('userID').distinct().rdd.map(lambda x: x['userID']).collect()
    pre_users = pre_df.select('userID').distinct().rdd.map(lambda x: x['userID']).collect()
    # here user set operation since list comprehension is too slow
    missing_user = len(set(users + pre_users)) - len(users)
    return float_devision(float(missing_user), len(pre_users))

def new_user(df):
    pass


if __name__ == '__main__':
    from datetime import timedelta
    timestamp = datetime(2017, 6, 13)
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_weekly_interactions(timestamp)
    print top_tag_by_user_viewtime(week_ucis, 'actionType')
