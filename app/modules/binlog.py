from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)


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
