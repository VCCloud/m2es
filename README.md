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

#### Note

```
Sync engine này chưa có check updated at, và chưa có filter các trường, chưa có limit update ( dễ bị sập nếu update trên 5000 bản ghi một lúc )
```
