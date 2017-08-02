from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.utils import *
from pyspark.sql import functions as func
from datetime import datetime


__all__ = [
    'view_count', 'user_number', 'avg_user_viewtime',
    'avg_user_view_count', 'avg_finished_program_by_user',
    'avg_completion_ratio', 'action_type_view_count',
    'user_hibernation', 'new_user', 'top_programs_by_view_count',
    'top_genre_by_view_count', 'top_channel_by_view_count'
]

def ucis_completion_ratio(ucis):
    return len([x for x in ucis if finished(x['duration'], x['runtime'])]) / float(len(ucis))

def duration_mapper(duration):
    return "{0} hours".format(str(duration / 3600))

def finished(duration, runtime):
    # NOTE completion is defined as 90% finished
    return duration > 0.9 * runtime

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

def top_channel_by_view_count(df, row_limit=10):
    top_channel = df\
        .select('channelID')\
        .groupBy('channelID')\
        .count()\
        .orderBy(func.desc('count'))\
        .limit(row_limit)\
        .rdd\
        .collect()
    top_channel = [
        {"channelID": x['channelID'], "count": x['count']}
        for x in top_channel]
    return top_channel

def top_channel_by_viewing_time(df, row_limit):
    top_channel = df\
        .select('channelName', 'duration')\
        .groupBy('channelName')\
        .agg(func.sum('duration'))\
        .sort(func.desc('sum(duration)'))\
        .limit(row_limit)\
        .rdd.collect()
    top_channel = [
        {"channelName": x['channelName'], "viewingTime": x['sum(duration)']}
        for x in top_channel]
    return top_channel

def top_genre_by_view_count(df, row_limit=10):
    top_genre = df\
        .select('category')\
        .filter(df.category.isNotNull())\
        .groupBy('category')\
        .count()\
        .orderBy(func.desc('count'))\
        .limit(row_limit)\
        .rdd\
        .collect()
    top_genre = [
        {"genre": x['category'], "count": x['count']}
        for x in top_genre]
    return top_genre

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
        .rdd\
        .groupBy(lambda x: x['firstEvent'].hour)\
        .map(lambda (hour, ucis): (hour, len(ucis)))\
        .sortBy(lambda(hour, count): hour)\
        .collect()
    print hour_of_day

def view_count_by_day_of_week(df):
    hour_of_day = df.rdd\
        .groupBy(lambda x: x['firstEvent'].weekday())\
        .map(lambda (weekday, ucis): (weekday, len(ucis)))\
        .sortBy(lambda(weekday, count):weekday)\
        .collect()
    print hour_of_day


def channel_trigger(df):
    channels = df\
        .where(df.actionType.isin(['tvod', 'svod']))\
        .select('channelName')\
        .distinct()\
        .rdd\
        .map(lambda x: x['channelName'])\
        .collect()
    print channels
    # for c in channels:
    #     print  c
    #     channel_df = df.filter(df.channelName==c)
    #     # print top_programs_by_view_count(channel_df)
    #     top_genre_by_view_count(channel_df)

def user_hibernation(df, pre_df):
    # df and pre_df should have the same days of interaction in it
    users = df.select('userID').distinct().rdd.map(lambda x: x['userID']).collect()
    pre_users = pre_df.select('userID').distinct().rdd.map(lambda x: x['userID']).collect()
    # here user set operation since list comprehension is too slow
    recurring_user = len(users) + len(pre_users) - len(set(users + pre_users))
    return 1 - float_devision(float(recurring_user), len(pre_users))

def new_user(df):
    pass


if __name__ == '__main__':
    d1 = datetime(2017, 4, 20)
    d2 = datetime(2017, 4, 13)
    spark_io = SparkParquetIO()
    interactions = spark_io.get_daily_interactions(d1)
    print action_type_view_count(interactions)
