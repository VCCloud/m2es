from elasticsearch import Elasticsearch
from elasticsearch import helpers
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


def init_app():
    DATABASE = config.MYSQL_URI
    ELASTIC = config.ELASTIC
    cursor = init_db(DATABASE)
    es = init_es(ELASTIC)

    cursor.execute("show tables")
    tables = [value for data in cursor.fetchall() for value in data.values()]
    print tables
    query = "select * from {table}"
    for table in tables:
        statement = query.format(table=table)
        cursor.execute(statement)
        datas = cursor.fetchall()
        actions = []
        action = {
            "_index": DATABASE['db'],
            "_type": table
        }
        for data in datas:
            tem = dict(action)
            tem["_id"] = data.pop("id")
            tem["_source"] = data
            actions.append(tem)

        helpers.bulk(es, actions, stats_only=True)

if __name__ == "__main__":
    init_app()
