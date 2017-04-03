import sys
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
import sched
import time
from math import ceil
import config
import MySQLdb
from gevent import monkey
from gevent.pool import Pool
import logging
import time

monkey.patch_socket()
reload(sys)
sys.setdefaultencoding('utf8')
IGNORE_TABLES = config.IGNORE_TABLES
ALLOW_TABLES = config.ALLOW_TABLES
LARGEST_SIZE = 5000
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


def init_db(DATABASE):
    db = MySQLdb.connect(host=DATABASE['host'],
                         port=DATABASE['port'],
                         user=DATABASE['username'],
                         passwd=DATABASE['password'],
                         db=DATABASE['db'])
    return db


def init_es(ELASTIC):
    if ELASTIC.get("username", None):
        ELASTIC_URI = ("{protocol}://{username}:{passwd}@{host}:{port}".format(
            protocol=ELASTIC['protocol'],
            username=ELASTIC['username'],
            passwd=ELASTIC['password'],
            host=ELASTIC['host'],
            port=ELASTIC['port']
        ))

    else:
        ELASTIC_URI = ("{protocol}://{host}:{port}".format(
            protocol=ELASTIC['protocol'],
            host=ELASTIC['host'],
            port=ELASTIC['port']
        ))

    return Elasticsearch(ELASTIC_URI)


def binlog_streaming(DATABASE, SLAVE_SERVER,
                     resume=False,
                     log_pos=None,
                     log_file=None):

    return BinLogStreamReader(
        connection_settings={
            "host": DATABASE['host'],
            "port": DATABASE['port'],
            "user": DATABASE['username'],
            "passwd": DATABASE['password']
        },
        server_id=SLAVE_SERVER['id'],
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
        log_pos=log_pos, log_file=log_file, only_schemas=DATABASE['db'],
        resume_stream=resume
    )


def init_worker(DATABASE=None, es=None, pool_size=3, number=LARGEST_SIZE,
                table=None, key_name=None):

    pool = Pool(pool_size)
    query_limit = "SELECT * FROM {table} LIMIT {offset}, {size} "
    paths = []
    for i in xrange(int(ceil(float(number) / LARGEST_SIZE))):
        paths.append(i * LARGEST_SIZE)

    def worker(start):
        actions = list()
        print "FROM {start} TO {stop}".format(start=start,
                                              stop=start + LARGEST_SIZE)
        statement = query_limit.format(table=table, offset=start,
                                       size=LARGEST_SIZE)
        db = init_db(DATABASE)
        cursor = db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(statement)
        datas = cursor.fetchall()
        action = {
            "_op_type": 'index',
            "_index": DATABASE['db'],
            "_type": table
        }

        for data in datas:
            print "------------------"
            print data
            print table
            tem = dict(action)
            tem["_id"] = data.pop(key_name)
            tem["_source"] = data
            actions.append(tem)
            log.info(tem)

        result = helpers.bulk(es, actions, stats_only=True)
        cursor.close()
        db.close()
        print "Result", result

    pool.map(worker, paths)
    pool.join()


def init_app(first_run=False):
    DATABASE = config.MYSQL_URI
    ELASTIC = config.ELASTIC
    SLAVE_SERVER = config.SLAVE_SERVER
    db = init_db(DATABASE)
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    es = init_es(ELASTIC)
    key_statement = "SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'"
    records = "SELECT COUNT({key_name}) FROM {table}"

    def query(statement):
        cursor.execute(statement)
        return cursor.fetchall()

    data = query("SHOW MASTER STATUS")[0]
    last_log = data['File']
    last_pos = data['Position']
    resume = True
    del data

    cursor.execute("SHOW TABLES")
    tables = [value for data in cursor.fetchall()
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
            key_name = query(key_statement.format(table=table))[0][
                'Column_name']
            detail = {
                'table_name': table,
                'primary_key': key_name
            }
            print detail
            detail['total_records'] = query(records.format(
                table=table,
                key_name=key_name))[0]['COUNT({key_name})'.format(
                    key_name=key_name)]
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
                    print '-----------------------------'
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
                    print helpers.bulk(
                        es,
                        actions[part * LARGEST_SIZE:
                                (part + 1) * LARGEST_SIZE],
                        stats_only=True)
            else:
                log.info(actions)
                print helpers.bulk(es, actions, stats_only=True)

            cursor.execute("SHOW MASTER STATUS")
            data = cursor.fetchall()[0]
            last_log = data['File']
            last_pos = data['Position']
            log.info(last_log)
            log.info(last_pos)
        except Exception:
            log.info(last_log)
            log.info(last_pos)

        scheduler.enter(config.FREQUENCY, 1, sync_from_log, (DATABASE,
                                                             SLAVE_SERVER,
                                                             resume,
                                                             last_log,
                                                             last_pos))

    scheduler.enter(1, 1, sync_from_log, (DATABASE, SLAVE_SERVER, resume,
                                          last_log, last_pos))
    scheduler.run()
