from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
from diagnostic.calculation.user import *
from diagnostic.calculation.utils import normalize, float_devision
from datetime import datetime, timedelta
import rethinkdb as r


spark_io = SparkParquetIO()

def user_pool(content_title):
    ucis = spark_io.get_all_interactions()
    users = ucis\
        .filter(ucis.title==content_title)\
        .filter(ucis.duration>1800)\
        .groupBy(ucis.userID)\
        .count()\
        .collect()
    engaged_users = [x['userID'] for x in users if x['count'] > 3]
    return engaged_users

def exit_user_pool(content_title):
    ucis = spark_io.get_interactions(datetime(2017, 8, 27), datetime(2017, 9, 7))
    users = ucis\
        .filter(ucis.title==content_title)\
        .filter(ucis.duration>1800)\
        .groupBy(ucis.userID)\
        .count()\
        .collect()
    engaged_users = [x['userID'] for x in users if x['count'] > 2]
    return engaged_users

def lead_in(content_title, users):
    lead_in_users = user_pool(content_title)
    overlaping_users = [x for x in lead_in_users if x in users]
    print"#"*10
    print "lead in: ", content_title
    print len(lead_in_users), len(overlaping_users), float_devision(len(overlaping_users), len(lead_in_users))

def lead_out(content_title, users):
    lead_out_users = exit_user_pool(content_title)
    overlaping_users = [x for x in lead_out_users if x in users]
    print"#"*10
    print "lead out: ", content_title
    print len(lead_out_users), len(overlaping_users), float_devision(len(overlaping_users), len(lead_out_users))


if __name__ == '__main__':
    dt_start = datetime(2017, 4, 1)
    dt_end = datetime(2017, 7, 16)
    game_of_throne_users = user_pool('Game of Thrones')
    all_titles = ['The Office (US)', 'Girls', 'Silicon Valley', 'Fargo', 'The Handmaid\'s Tale',
        'Westworld', 'Sex and The City', 'Billions', 'The Big Bang Theory',
        'Twin Peaks: The Return', 'Big Little Lies', 'Twin Peaks', 'Parks and Recreation',
        'The Voice - Norges beste stemme', 'Vendela + Petter', 'Ballers', 'Ray Donovan']
    for title in all_titles:
        lead_out(title, game_of_throne_users)
