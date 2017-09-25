from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.utils import *
from diagnostic.calculation.utils import normalize
from pyspark.sql import functions as func
from pyspark.sql.types import BooleanType, StringType
from datetime import datetime, timedelta
from functional import seq


__all__ = [
    'weekly_new_user', 'unique_user',
    'users_package_overview', 'basic_additional_package_overview',
    'basic_package_user_viewing_time', 'primeium_package_user_viewing_time'
]
spark_io = SparkParquetIO()

def transform_name(name_list):
    if len(name_list) == 0:
        return 'None'
    else:
        return ' & '.join(name_list)

def weekly_new_user(df, dt):
    return df\
        .filter(
            (df.firstActivity>dt-timedelta(days=6)) & (df.firstActivity<dt+timedelta(days=1))
        )\
        .count()

def unique_user(df, dt):
    return df\
        .filter(df.firstActivity<dt+timedelta(days=1))\
        .count()

def package_overview(df):
    return df\
        .groupBy('basicPackage')\
        .count()\
        .show()

def basic_additional_package_overview():
    def sort_package(d):
        return [(x, d[x]) for x in ["T1", "T2", "T3"]]
    users = spark_io.get_users()
    cross_packages = users\
        .groupBy(['basicPackage', 'additionalPackage'])\
        .count()\
        .collect()
    cross_packages = [(x['basicPackage'], transform_name(x['additionalPackage']), x['count'])
        for x in cross_packages]
    package_dict = {}
    for p_tuple in cross_packages:
        if p_tuple[1] in package_dict:
            package_dict[p_tuple[1]][p_tuple[0]] = p_tuple[2]
        else:
            package_dict[p_tuple[1]] = {p_tuple[0]: p_tuple[2]}
    res = {k: sort_package(package_dict[k]) for k in package_dict}
    return res

def users_package_overview(ucis):
    """
    send in ucis and get its users, then group by package info
    """
    package_names = ["T1", "T2", "T3"]
    users = spark_io.get_users()
    user_num = users.count()
    user_packages = users\
        .groupBy('basicPackage')\
        .count()\
        .collect()
    user_packages = {x['basicPackage']: x['count'] for x in user_packages}

    tag_users = ucis\
        .select('userID')\
        .distinct()\
        .collect()

    tag_users = set([x['userID'] for x in tag_users])

    #defined udf for filter
    is_in_tag_user = func.udf(lambda x: x in tag_users, BooleanType())

    tag_user_packages = users\
        .filter(is_in_tag_user(users.userID))\
        .groupBy('basicPackage')\
        .count()\
        .collect()
    tag_user_packages = {x['basicPackage']: x['count'] for x in tag_user_packages}
    tag_user_packages_norm = normalize([(x, tag_user_packages[x]) for x in package_names], len(tag_users))
    user_packages_norm = normalize([(x, user_packages[x] - tag_user_packages[x]) for x in package_names], user_num - len(tag_users))
    return tag_user_packages_norm, user_packages_norm

def primeium_packages(df):
    is_not_empty = func.udf(lambda x: len(x) != 0, BooleanType())
    print df\
        .filter(is_not_empty(df.additionalPackage))\
        .groupBy(df.additionalPackage)\
        .count()\
        .show()
def basic_package_user_viewing_time(df):
    package_info = get_package(build_user_package())
    package_udf = func.udf(lambda x: user_to_basic_package(x, package_info), StringType())
    packages = df\
        .groupBy(df.userID, package_udf(df.userID))\
        .sum('duration')\
        .groupBy(package_udf(df.userID))\
        .avg('sum(duration)')\
        .collect()
    viewing_time_dict =  {x['<lambda>(userID)']: x['avg(sum(duration))'] / 60 for x in packages}
    return [(x, viewing_time_dict[x]) for x in ["T1", "T2", "T3"]]

def primeium_package_user_viewing_time(df):
    package_info = get_package(build_user_package())
    package_udf = func.udf(lambda x: user_to_additional_package(x, package_info), StringType())
    packages = df\
        .groupBy(df.userID, package_udf(df.userID))\
        .sum('duration')\
        .groupBy(package_udf(df.userID))\
        .avg('sum(duration)')\
        .collect()
    viewing_time_dict =  {x['<lambda>(userID)']: x['avg(sum(duration))'] / 60 for x in packages}
    return [(x, viewing_time_dict[x]) for x in ["None", "VIASAT", "CMORE", "VIASAT & CMORE"]]


if __name__ == '__main__':
    timestamp = datetime(2017, 6, 13)
    spark_io = SparkParquetIO()
    ucis = spark_io.get_weekly_interactions(timestamp)
    basic_package_user_viewing_time(ucis)
