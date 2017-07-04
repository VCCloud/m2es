from gevent import monkey
from gevent.pool import Pool
from math import ceil
import time

import elasticsearch

import config
import config.LOG as log
from . import DB

monkey.patch_socket()

LARGEST_SIZE = config.LARGEST_SIZE


class WorkerSync(object):

    def __init__(self, DATABASE=None, es=None, pool_size=3,
                 number=LARGEST_SIZE,
                 table=None, key_name=None,
                 setup_query=None,
                 after_query=None):

        self.__after_query = after_query

        if setup_query:
            self.__key_name, self.__query_limit = setup_query(key_name)

        else:
            self.__key_name = key_name
            self.__query_limit = "SELECT * FROM {table} LIMIT {offset}, {size}"

        self.__DATABASE = DATABASE
        self.__es = es
        self.__pool_size = pool_size
        self.__number = number
        self.__table = table
        self.__pool = Pool(self.pool_size)
        self.__paths = [i * LARGEST_SIZE for i in xrange(
            int(ceil(float(self.number) / LARGEST_SIZE)))]

    def __worker(self, start):
        actions = list()
        log.info("FROM %s TO %s", start, start + LARGEST_SIZE)
        statement = self.__query_limit.format(table=self.__table,
                                              offset=start,
                                              size=LARGEST_SIZE)
        db = DB(self.__DATABASE)
        datas = db.query(statement)
        action = {
            "_op_type": 'index',
            "_index": self.__DATABASE['db'].lower(),
            "_type": self.__table.lower()
        }

        for data in datas:
            log.info("------------------")
            log.info(data)
            log.info(self.__table)
            tem = dict(action)
            data = self.__after_query(data) if self.__after_query else data

            tem["_id"] = data.pop(self.__key_name)
            tem["_source"] = data
            actions.append(tem)
            log.info(tem)

        while True:
            try:
                success, error = self.es.bulk(actions)
                if error:
                    log.error('Error %s at path %s', error, start)
                    log.info('Try resend at path %s in 10s', start)
                    for i in xrange(10, 0, -1):
                        time.sleep(1)
                        log.info("%ds", i)
                else:
                    break

            except elasticsearch.BulkIndexError as e:
                log.error(e)
                log.info('Try resend at path %s in 10s', start)
                for i in xrange(10, 0, -1):
                    time.sleep(1)
                    log.info("%ds", i)

        log.info("Result %s at path %s", success, start)

    def run(self):
        self.__pool.map(self.__worker, self.__paths)
        self.__pool.join()
