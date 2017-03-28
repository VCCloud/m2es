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

import config
import MySQLdb


def init_db(DATABASE):
    db = MySQLdb.connect(host=DATABASE['host'],
                         port=DATABASE['port'],
                         user=DATABASE['username'],
                         passwd=DATABASE['password'],
                         db=DATABASE['db'])
    return db.cursor(MySQLdb.cursors.DictCursor)


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
        log_pos=log_pos, log_file=log_file, only_schemas=DATABASE['db']
    )


def init_app(first_run=False):
    DATABASE = config.MYSQL_URI
    ELASTIC = config.ELASTIC
    SLAVE_SERVER = config.SLAVE_SERVER
    cursor = init_db(DATABASE)
    es = init_es(ELASTIC)
    LARGEST_SIZE = 5000

    if first_run:
        cursor.execute("show tables")
        tables = [value for data in cursor.fetchall()
                  for value in data.values()]
        query = "select * from {table}"
        for table in tables:
            if table == "alembic_version":
                continue
            statement = query.format(table=table)
            cursor.execute(statement)
            datas = cursor.fetchall()
            actions = []
            action = {
                "_op_type": 'index',
                "_index": DATABASE['db'],
                "_type": table
            }

            for data in datas:
                tem = dict(action)
                tem["_id"] = data.pop("id")
                tem["_source"] = data
                actions.append(tem)
                print tem

            if len(actions) > LARGEST_SIZE:
                parts = (len(actions) / LARGEST_SIZE
                         if (len(actions) % LARGEST_SIZE)
                         else len(actions) / LARGEST_SIZE + 1)

                for part in range(parts - 1):
                    helpers.bulk(
                        es,
                        actions[part * LARGEST_SIZE,
                                (part + 1) * LARGEST_SIZE],
                        stats_only=True)
            else:
                helpers.bulk(es, actions, stats_only=True)

        first_run = False

    scheduler = sched.scheduler(time.time, time.sleep)

    def sync_from_log(last_log=None, last_pos=None, resume=False):
        stream = binlog_streaming(DATABASE, SLAVE_SERVER, resume=resume,
                                  log_file=last_log, log_pos=last_pos)

        cursor.execute("SHOW MASTER STATUS")
        data = cursor.fetchall()[0]
        last_log = data['File']
        last_pos = data['Position']
        resume = True
        actions = []

        for binlogevent in stream:
            if binlogevent.table == 'alembic_version':
                continue

            for row in binlogevent.rows:
                action = {
                    "_index": DATABASE['db'],
                    "_type": binlogevent.table
                }
                print '-----------------------------'
                print table
                print row
                if isinstance(binlogevent, DeleteRowsEvent):
                    action["_op_type"] = "delete"
                    action["_id"] = row["values"].pop("id")
                elif isinstance(binlogevent, UpdateRowsEvent):
                    action["_op_type"] = "index"
                    action["_id"] = row["after_values"].pop("id")
                    action["_source"] = row["after_values"]
                elif isinstance(binlogevent, WriteRowsEvent):
                    action["_op_type"] = "index"
                    print (row["values"])
                    action["_id"] = row["values"].pop("id")
                    action["_source"] = row["values"]
                actions.append(action)

        if len(actions) > LARGEST_SIZE:
            parts = (len(actions) / LARGEST_SIZE
                     if (len(actions) % LARGEST_SIZE)
                     else len(actions) / LARGEST_SIZE + 1)

            for part in range(parts - 1):
                helpers.bulk(
                    es,
                    actions[part * LARGEST_SIZE,
                            (part + 1) * LARGEST_SIZE],
                    stats_only=True)
        else:
            helpers.bulk(es, actions, stats_only=True)

        scheduler.enter(config.FREQUENCY, 1, sync_from_log, (last_log,
                                                             last_pos, True))

    scheduler.enter(1, 1, sync_from_log, ())
    scheduler.run()
