import sys
import elasticsearch
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
import sched
import time
from math import ceil
import config
import config.LOG as log
import pymysql
from gevent import monkey
from app.modules import (DB, ES, binlog_streaming, WorkerSync)

reload(sys)
sys.setdefaultencoding('utf8')

monkey.patch_socket()

IGNORE_TABLES = config.IGNORE_TABLES
ALLOW_TABLES = config.ALLOW_TABLES
LARGEST_SIZE = config.LARGEST_SIZE


class SyncApp():

    def __init__(self, first_run=False,
                 setup_query=None,
                 after_query=None,
                 before_sync_while_logging=None):
        self.before_sync_while_logging = before_sync_while_logging
        self.setup_query = setup_query
        self.after_query = after_query
        self.first_run = first_run
        self.DATABASE = config.MYSQL_URI
        self.ELASTIC = config.ELASTIC
        self.SLAVE_SERVER = config.SLAVE_SERVER
        self.db = DB(self.DATABASE)
        self.es = ES(self.ELASTIC)
        self.last_log, self.last_pos = self.get_last_log()
        self.tables = self.get_tables()
        self.resume = True
        self.detail_tables = self.get_detail_tables()

        if self.first_run:
            self.first_sync()
        self.scheduler = sched.scheduler(time.time, time.sleep)
        log.info('START')
        log.info(self.last_log)
        log.info(self.last_pos)
        self.scheduler.enter(config.FREQUENCY, 1, self.sync_from_log, ())

    def run(self):
        self.scheduler.run()


    def get_detail_tables(self):
        tables = self.get_tables()
        detail_tables = []
        for table in tables[:]:
            if self.check_permission_table(table):
                key_name = self.get_master_key(table)
                detail = {
                    'table_name': table,
                    'primary_key': key_name
                }
                log.info(detail)
                detail['total_records'] = self.get_table_size(table, key_name)
                detail_tables.append(detail)

        return detail_tables

    def first_sync(self):
        for detail in self.detail_tables:
            WorkerSync(
                DATABASE=self.DATABASE,
                es=self.es,
                number=detail['total_records'],
                table=detail['table_name'],
                key_name=detail['primary_key'],
                setup_query=self.setup_query.get(detail['table_name'], None),
                after_query=self.after_query.get(detail['table_name'], None)
            ).run()
        self.first_run = False

    def get_table_size(self, table, key_name):
        records = "SELECT COUNT({key_name}) FROM {table}"
        return self.db.query(records.format(
            table=table, key_name=key_name))[0][
            'COUNT({key_name})'.format(key_name=key_name)]

    def get_master_key(self, table):
        key_statement = "SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'"
        return self.db.query(key_statement.format(table=table))[
            0]['Column_name']

    def get_tables(self):
        datas = self.db.query("SHOW TABLES")
        return [value for data in datas
                for value in data.values()]

    def check_permission_table(self, table):
        if (ALLOW_TABLES):
            return (table in ALLOW_TABLES and
                    table not in IGNORE_TABLES)

        return table not in IGNORE_TABLES

    def get_last_log(self):
        data = self.db.query("SHOW MASTER STATUS")[0]
        return data['File'], data['Position']

    def sync_from_log(self):
        actions = list()
        try:
            stream = binlog_streaming(self.DATABASE, self.SLAVE_SERVER,
                                      resume=self.resume,
                                      log_file=self.last_log,
                                      log_pos=self.last_pos)

            for binlogevent in stream:
                if not self.check_permission_table(binlogevent.table):
                    continue

                for row in binlogevent.rows:
                    action = {
                        "_index": self.DATABASE['db'].lower(),
                        "_type": binlogevent.table.lower()
                    }
                    log.info('-----------------------------')
                    for detail in self.detail_tables:
                        if detail['table_name'] == binlogevent.table:
                            before_sync = self.before_sync_while_logging.get(
                                binlogevent.table,
                                None)
                            primary_key = detail['primary_key']
                            break

                    try:
                        data = row["values"]
                    except BaseException:
                        data = row["after_values"]


                    if before_sync:
                        primary_key, data = before_sync(
                            primary_key=primary_key,
                            data=data)
                    if isinstance(binlogevent, DeleteRowsEvent):
                        action["_op_type"] = "delete"
                        action["_id"] = data.pop(primary_key)
                    elif (isinstance(binlogevent, UpdateRowsEvent) or
                            isinstance(binlogevent, WriteRowsEvent)):
                        action["_op_type"] = "index"
                        action["_id"] = data.pop(primary_key)
                        action["_source"] = data
                    else:
                        continue

                    actions.append(action)

            stream.close()
            log.info(actions)
            if len(actions) > LARGEST_SIZE:
                log.info(actions)
                parts = int(ceil(float(len(actions) / LARGEST_SIZE)))

                for part in range(parts - 1):
                    log.info(self.es.bulk(actions[part * LARGEST_SIZE:
                                                  (part + 1) * LARGEST_SIZE]))
            else:
                log.info(actions)
                success, error = self.es.bulk(actions)
                if error:
                    raise Warning(error)

            self.last_log, self.last_pos = self.get_last_log()
            log.info('RENEW LOG')
            log.info('last log: %s', self.last_log)
            log.info('last pos: %s', self.last_pos)

        except pymysql.err.OperationalError as e:
            log.error("Connection ERROR: %s", e)
            log.info("LAST_LOG: %s", self.last_log)
            log.info("LAST_POS: %s", self.last_pos)

        except elasticsearch.ElasticsearchException as e:
            log.error("Elasticsearch error: %s", e)
            log.info("LAST_LOG: %s", self.last_log)
            log.info("LAST_POS: %s", self.last_pos)

        except Warning as e:
            log.error("Elasticsearch error: %s", e)
            log.info("LAST_LOG: %s", self.last_log)
            log.info("LAST_POS: %s", self.last_pos)

        self.scheduler.enter(config.FREQUENCY, 1, self.sync_from_log, ())
