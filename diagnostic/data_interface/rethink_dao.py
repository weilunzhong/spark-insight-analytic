import rethinkdb as r
import pytz
from datetime import datetime

class RethinkDao(object):
    """ RethinkDB data access object"""

    def __init__(self, rethink_client, database):
        """Client is defined outside of the Dao for easy DB switch

        NOTE: here all query follows the RethinkDB python quey language
        """
        self.rethink_client = rethink_client
        self.db = database

    def filter(self, table_name, query):
        """Query db and filter on query

        Args:
            table_name: table name
            query: SQL expression which is used for filter

        Returns:
            RethinkDB default cursor, iterable

        """
        return r.db(self.db)\
            .table(table_name)\
            .filter(query)\
            .run(self.rethink_client)

    def filter_and_count(self, table_name, query):
        """ same as filter method, return a count of the docs"""
        return r.db(self.db)\
            .table(table_name)\
            .filter(query)\
            .count()\
            .run(self.rethink_client)

    def has_fields(self, table_name, field_name):
        """ has field function warpper"""
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
        """ get all documents with same inventory_id
        Note that here the field must be indexed for get_all to work

        Args:
            table_name
            inventory_id

        Returns:
            cursor with all the docs for inventory_id
        """
        return r.db(self.db)\
            .table(table_name)\
            .get_all(inventory_id, index="inventoryID")\
            .run(self.rethink_client)

    def count_document_by_id(self, table_name, inventory_id):
        """same as get, return a count of the num of docs"""
        return r.db(self.db)\
            .table(table_name)\
            .get_all(inventory_id, index="inventoryID")\
            .count()\
            .run(self.rethink_client)

    def get_document_by_time(self, table_name, index_name,
            start=datetime(2000,1,1).replace(tzinfo=pytz.UTC),
            end=datetime.now().replace(tzinfo=pytz.UTC)):
        """get docs between time, here start and end has default value"""
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
        # NOTE changing array limit might cause problems!!
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

