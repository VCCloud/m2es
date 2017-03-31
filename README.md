This is elastic search from mysql to elastic
```
+-------------------+          +--------------------+           +---------------------+
|                   |          |                    |           |                     |
|                   +---------->                    +----------->                     |
|    Mysql-server   |          |     Sync-Engine    |           |     Elasticsearch   |
|                   |          |                    |           |                     |
|                   +---------->                    +----------->                     |
|                   |          |                    |           |                     |
+-------------------+          +--------------------+           +---------------------+

```

#### Deploy

```
pip install -r requirements.txt
cd app
mv config.py.example config.py
vim config.py
cd ..
python2 run.py
```
NOTE: if app in run.py = True it will sync all in database to elastic search by select * else will be only listening on binlog event

#### Note

```
Sync engine này chưa có check  chưa có filter các trường
```
