import rethinkdb as r
import pytz
from datetime import datetime

class RethinkDao(object):

    def __init__(self, rethink_client, database):
        self.rethink_client = rethink_client
        self.db = database

    def filter(self, table_name, query):
        return r.db(self.db)\
            .table(table_name)\
            .filter(query)\
            .run(self.rethink_client)

    def filter_and_count(self, table_name, query):
        return r.db(self.db)\
            .table(table_name)\
            .filter(query)\
            .count()\
            .run(self.rethink_client)

    def has_fields(self, table_name, field_name):
        return r.db(self.db)\
            .table(table_name)\
            .has_fields(field_name)\
            .run(self.rethink_client)

    def get(self, table_name, query_id):
        return r.db(self.db)\
            .table(table_name)\
            .get(query_id)\
            .run(self.rethink_client)

    def get_document_by_id(self, table_name, inventory_id):
        return r.db(self.db)\
            .table(table_name)\
            .get_all(inventory_id, index="inventoryID")\
            .run(self.rethink_client)

    def count_document_by_id(self, table_name, inventory_id):
        return r.db(self.db)\
            .table(table_name)\
            .get_all(inventory_id, index="inventoryID")\
            .count()\
            .run(self.rethink_client)

    def get_document_by_time(self, table_name, index_name,
            start=datetime(2000,1,1).replace(tzinfo=pytz.UTC),
            end=datetime.now().replace(tzinfo=pytz.UTC)):
        return r.db(self.db)\
            .table(table_name)\
            .between(start, end, index=index_name)\
            .run(self.rethink_client)

    def get_document_by_user_in_time(self, table_name, index_name, user_ids,
            start=datetime(2000,1,1).replace(tzinfo=pytz.UTC),
            end=datetime.now().replace(tzinfo=pytz.UTC)):
        return r.db(self.db)\
            .table(table_name)\
            .between(start, end, index=index_name)\
            .filter(lambda i: r.expr(user_ids).contains(i['userID']))\
            .run(self.rethink_client)

    def get_document_count_by_time(self, table_name, index_name,
            start=datetime(2000,1,1).replace(tzinfo=pytz.UTC),
            end=datetime.now().replace(tzinfo=pytz.UTC)):
        return r.db(self.db)\
            .table(table_name)\
            .between(start, end, index=index_name)\
            .count()\
            .run(self.rethink_client)

    def active_user_count_in_time_range(self,
            start=datetime(2000,1,1).replace(tzinfo=pytz.UTC),
            end=datetime.now().replace(tzinfo=pytz.UTC)):
        return r.db(self.db)\
            .table('interaction')\
            .between(start, end, index='firstEvent')\
            .group('userID')\
            .count()\
            .ungroup()\
            .count()\
            .run(self.rethink_client, array_limit=200000) #this aray limit could be avoided

    def active_user_in_time_range(self,
            start=datetime(2000,1,1).replace(tzinfo=pytz.UTC),
            end=datetime.now().replace(tzinfo=pytz.UTC)):
        return r.db(self.db)\
            .table('interaction')\
            .between(start, end, index='firstEvent')\
            .group('userID')\
            .count()\
            .ungroup()\
            .map(lambda d: d['group'])\
            .run(self.rethink_client, array_limit=200000) #this aray limit could be avoided

    # this can do unique user count and new user count
    def new_user_count_in_time_range(self,
            start=datetime(2015,1,1).replace(tzinfo=pytz.UTC),
            end=datetime.now().replace(tzinfo=pytz.UTC)):
        return r.db(self.db)\
            .table('user')\
            .between(start, end, index='firstActivity')\
            .count()\
            .run(self.rethink_client)

