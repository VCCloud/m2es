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
from gevent.pool import Pool
from app.modules import (DB, ES, binlog_streaming)

reload(sys)
sys.setdefaultencoding('utf8')

monkey.patch_socket()

IGNORE_TABLES = config.IGNORE_TABLES
ALLOW_TABLES = config.ALLOW_TABLES
LARGEST_SIZE = 5000


def init_worker(DATABASE=None, es=None, pool_size=3, number=LARGEST_SIZE,
                table=None, key_name=None):

    pool = Pool(pool_size)
    query_limit = "SELECT * FROM {table} LIMIT {offset}, {size} "
    paths = []
    for i in xrange(int(ceil(float(number) / LARGEST_SIZE))):
        paths.append(i * LARGEST_SIZE)

    def worker(start):
        actions = list()
        log.info("FROM %s TO %s", start, start + LARGEST_SIZE)
        statement = query_limit.format(table=table, offset=start,
                                       size=LARGEST_SIZE)
        db = DB(DATABASE)
        datas = db.query(statement)
        action = {
            "_op_type": 'index',
            "_index": DATABASE['db'],
            "_type": table
        }

        for data in datas:
            log.info("------------------")
            log.info(data)
            log.info(table)
            tem = dict(action)
            tem["_id"] = data.pop(key_name)
            tem["_source"] = data
            actions.append(tem)
            log.info(tem)

        while True:
            try:
                success, error = es.bulk(actions)
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

    pool.map(worker, paths)
    pool.join()


def init_app(first_run=False):
    DATABASE = config.MYSQL_URI
    ELASTIC = config.ELASTIC
    SLAVE_SERVER = config.SLAVE_SERVER
    db = DB(DATABASE)
    es = ES(ELASTIC)
    key_statement = "SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'"
    records = "SELECT COUNT({key_name}) FROM {table}"

    data = db.query("SHOW MASTER STATUS")[0]
    last_log = data['File']
    last_pos = data['Position']
    resume = True
    del data

    datas = db.query("SHOW TABLES")
    tables = [value for data in datas
              for value in data.values()]

    detail_tables = []
    key_check = True
    for table in tables[:]:
        if (ALLOW_TABLES):
            if (table in ALLOW_TABLES and
                    table not in IGNORE_TABLES):
                key_check = True
            else:
                key_check = False

        elif table in IGNORE_TABLES:
            key_check = False

        if key_check:
            key_name = db.query(key_statement.format(table=table))[0][
                'Column_name']
            detail = {
                'table_name': table,
                'primary_key': key_name
            }
            log.info(detail)
            detail['total_records'] = db.query(
                records.format(table=table, key_name=key_name))[0][
                'COUNT({key_name})'.format(key_name=key_name)]
            detail_tables.append(detail)

        key_check = True

    if first_run:
        for detail in detail_tables:
            init_worker(
                DATABASE=DATABASE,
                es=es,
                number=detail['total_records'],
                table=detail['table_name'],
                key_name=detail['primary_key'])

        first_run = False

    scheduler = sched.scheduler(time.time, time.sleep)

    log.info('START')
    log.info(last_log)
    log.info(last_pos)

    def sync_from_log(DATABASE, SLAVE_SERVER, resume, last_log, last_pos):
        actions = list()
        try:
            stream = binlog_streaming(DATABASE, SLAVE_SERVER, resume=resume,
                                      log_file=last_log, log_pos=last_pos)

            for binlogevent in stream:
                if (binlogevent.table in IGNORE_TABLES):
                    continue

                for row in binlogevent.rows:
                    action = {
                        "_index": DATABASE['db'],
                        "_type": binlogevent.table
                    }
                    log.info('-----------------------------')
                    for detail in detail_tables:
                        if detail['table_name'] == binlogevent.table:
                            primary_key = detail['primary_key']
                    if isinstance(binlogevent, DeleteRowsEvent):
                        action["_op_type"] = "delete"
                        action["_id"] = row["values"].pop(primary_key)
                    elif isinstance(binlogevent, UpdateRowsEvent):
                        action["_op_type"] = "index"
                        action["_id"] = row["after_values"].pop(primary_key)
                        action["_source"] = row["after_values"]
                    elif isinstance(binlogevent, WriteRowsEvent):
                        action["_op_type"] = "index"
                        action["_id"] = row["values"].pop(primary_key)
                        action["_source"] = row["values"]
                    else:
                        continue
                    actions.append(action)

            stream.close()
            log.info(actions)
            if len(actions) > LARGEST_SIZE:
                log.info(actions)
                parts = int(ceil(float(len(actions) / LARGEST_SIZE)))

                for part in range(parts - 1):
                    log.info(es.bulk(actions[part * LARGEST_SIZE:
                                             (part + 1) * LARGEST_SIZE]))
            else:
                log.info(actions)
                success, error = es.bulk(actions)
                if error:
                    raise Warning(error)

            data = db.query("SHOW MASTER STATUS")[0]
            last_log = data['File']
            last_pos = data['Position']
            log.info('RENEW LOG')
            log.info('last log: %s', last_log)
            log.info('last pos: %s', last_pos)

        except pymysql.err.OperationalError as e:
            log.error("Connection ERROR: %s", e)
            log.info("LAST_LOG: %s", last_log)
            log.info("LAST_POS: %s", last_pos)

        except elasticsearch.BulkIndexError as e:
            log.error("Elasticsearch error: %s", e)
            log.info("LAST_LOG: %s", last_log)
            log.info("LAST_POS: %s", last_pos)

        except Warning as e:
            log.error("Elasticsearch error: %s", e)
            log.info("LAST_LOG: %s", last_log)
            log.info("LAST_POS: %s", last_pos)

        scheduler.enter(config.FREQUENCY, 1, sync_from_log, (DATABASE,
                                                             SLAVE_SERVER,
                                                             resume,
                                                             last_log,
                                                             last_pos))

    scheduler.enter(1, 1, sync_from_log, (DATABASE, SLAVE_SERVER, resume,
                                          last_log, last_pos))
    scheduler.run()
