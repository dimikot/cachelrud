from . import *
import urlparse
import urllib
import pymongo
import re
import datetime

#
# Input parameters:
# - dsn
# - replicaSet (optional)
# - dbname
# - collection
# - timestampfield
#

class Storage(Base):
    def __init__(self, log, collection, timestampfield):
        """
        :type collection: pymongo.collection.Connection
        :type timestampfield: str
        """
        self._log = log
        self._collection = collection
        self._timestampfield = timestampfield

    @classmethod
    def get_instance(cls, log, params):
        """
        :type log: logging.Logger
        :type params: dict
        :rtype: Storage
        """
        dsn = params['dsn']
        dsn_parts = re.match(r'(?x)^ (\w+:// (?:[^@]+@)? ([^/]+) ) / ( [^?]* ) (?:\?(.*))? $', dsn)
        if dsn_parts is None:
            raise Exception("Cannot parse MongoDB DSN '%s'!" % dsn)
        mongo_dsn, mongo_dbname, mongo_qs = dsn_parts.group(1), dsn_parts.group(3), dsn_parts.group(4)
        mongo_qs = dict(urlparse.parse_qsl(mongo_qs))
        if 'replicaSet' in params:
            mongo_qs['replicaSet'] = params['replicaSet']
        if 'dbname' in params:
            mongo_dbname = params['dbname']
        mongo_dsn = mongo_dsn + "/" + mongo_dbname + "?" + urllib.urlencode(mongo_qs)
        log.info("Connecting to %s", mongo_dsn)
        client = pymongo.Connection(mongo_dsn)
        db = client[mongo_dbname]
        collection = db[params['collection']]
        return Storage(log, collection, params['timestampfield'])

    def touch_keys(self, keys):
        self._collection.update(
            {'_id': {'$in': keys}},
            {'$set': {self._timestampfield: datetime.datetime.utcnow()}},
            w=0
        )

    def get_stat(self):
        stat = self._collection.database.command('collStats', self._collection.name)
        return stat['size'], stat['count']

    def clean_oldest(self, count):
        self._collection.ensure_index(self._timestampfield)
        rows = self._collection.find(sort=[(self._timestampfield, 1)], limit=count, fields=['_id'])
        to_del = map(lambda row: row['_id'], rows)
        self._log.debug("IDs to delete: %s", to_del)
        if to_del:
            self._collection.remove({"_id": {"$in": to_del}})
        return len(to_del)
