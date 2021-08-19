# HappyBase and AIOHappyBase

[HappyBase](https://happybase.readthedocs.io/en/latest/) and [AIOHappyBase](https://aiohappybase.readthedocs.io/en/latest/user.html) are two Python Hbase clients. 

## To execute the examples

- **Startup Hbase and Thrift server**

HappyBase and AIOHappyBase require the Hbase Thrift server to be up and running:

```
> cd hbase-installation
> ./bin/start-hbase.sh
> ./bin/hbase-daemon.sh start thrift
```

- **Install poetry**

The project uses [Poetry](https://python-poetry.org/) to manage the virtual environment and the project dependencies so it's recommended to install Poetry

- **Update dependencies**

To download the dependencies declared in `pyproject.toml` execute:

```
poetry update
```

- **Start shell**

To create or use a virtualenv execute:

```
poetry shell
```

Now you can start opening a Python shell or executing the Poetry scripts.

- **Execute Poetry scripts**

Check [script] section in pyproject.toml

```toml
[tool.poetry.scripts]
create = "hbasegs.synchronous:hbase_create"
insert = "hbasegs.synchronous:hbase_put"
search = "hbasegs.synchronous:hbase_scan"
search_more_filters = "hbasegs.synchronous:hbase_scan_more_filters"
async_insert = "hbasegs.asynchronous:hbase_put"
async_search = "hbasegs.asynchronous:hbase_aio_use_case_main"
```

These are the scripts you can execute directly from command line, for instance, to create the examples table in Hbase:

```
poetry run create
```

or insert 5M rows in your local standalone Hbase:

```
poetry run insert
```