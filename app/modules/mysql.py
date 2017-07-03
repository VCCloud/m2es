import time
import config
import MySQLdb


class DB:

    def __init__(self, DATABASE):
        self.DATABASE = DATABASE
        self.first = True

    def init_db(self):
        while True:
            try:
                self.db = MySQLdb.connect(host=self.DATABASE['host'],
                                          port=self.DATABASE['port'],
                                          user=self.DATABASE['username'],
                                          passwd=self.DATABASE['password'],
                                          db=self.DATABASE['db'],
                                          charset='utf8')
                self.db.autocommit(True)
                break

            except MySQLdb.OperationalError as e:
                config.LOG.error(e)
                config.LOG.info('=============')
                config.LOG.info('Connection MySQL error')
                config.LOG.info('Trying reconnect in 10s')
                for i in xrange(10, 0, -1):
                    time.sleep(1)
                    config.LOG.info("%ds", i)

    def query(self, stmt):
        try:
            if self.first:
                self.init_db()
                self.first = False

            cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(stmt)
            data = cursor.fetchall()
            cursor.close()
            return data

        except (AttributeError, MySQLdb.OperationalError) as e:
            config.LOG.error(e)
            self.init_db()
            cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(stmt)
            data = cursor.fetchall()
            cursor.close()
            return data

    def close(self):
        self.db.close()
