import sys
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import ConnectionError
import urllib3
import time
import config

reload(sys)
sys.setdefaultencoding('utf8')

IGNORE_TABLES = config.IGNORE_TABLES
ALLOW_TABLES = config.ALLOW_TABLES
LARGEST_SIZE = 5000


class ES:

    def __init__(self, ELASTIC):
        self.first = True
        self.ELASTIC = ELASTIC
        if self.ELASTIC.get("username", None):
            self.ELASTIC_URI = (
                "{protocol}://{username}:{passwd}@{host}:{port}".format(
                    protocol=self.ELASTIC['protocol'],
                    username=self.ELASTIC['username'],
                    passwd=self.ELASTIC['password'],
                    host=self.ELASTIC['host'],
                    port=self.ELASTIC['port']
                ))

        else:
            self.ELASTIC_URI = ("{protocol}://{host}:{port}".format(
                protocol=self.ELASTIC['protocol'],
                host=self.ELASTIC['host'],
                port=self.ELASTIC['port']
            ))

    def init_es(self):

        while True:
            try:
                self.es = Elasticsearch(self.ELASTIC_URI)
                if self.es.ping() is False:
                    raise ConnectionError
                break

            except (ConnectionError,
                    urllib3.exceptions.ConnectTimeoutError):
                config.LOG.error('=============')
                config.LOG.error('Connection Elastic error')
                config.LOG.info('Trying reconnect in 10s')
                for i in xrange(10, 0, -1):
                    time.sleep(1)
                    config.LOG.info("%ds", i)

    def bulk(self, actions):
        try:
            if self.first:
                self.init_es()
                self.first = False

            return helpers.bulk(self.es, actions, stats_only=True,
                                raise_on_error=True)

        except (ConnectionError,
                AttributeError) as e:
            config.LOG.info('ERROR %s %s', e, type(e))
            self.init_es()
            return helpers.bulk(self.es, actions, stats_only=True,
                                raise_on_error=True)
