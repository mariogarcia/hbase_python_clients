import random
import happybase
import datetime as dt

from hbasegs.util import time_it

@time_it
def hbase_create():
    connection = happybase.Connection('localhost')
    connection.create_table(
        'iot',
        {
            'device': dict(),
            'metric': dict({
                "COMPRESSION": "GZ"
            })
        })


@time_it
def hbase_put():
    connection = happybase.Connection('localhost')
    iot_table  = connection.table("iot")
    initial_dt = dt.datetime.now()

    with iot_table.batch(batch_size=128) as batch: # improving insertion by batching
        for second in range(0, 10000): # seconds
            for house in range(0, 100): # houses
                for device in range(0, 5): # devices    
                    moment   = (initial_dt + dt.timedelta(seconds=second)).strftime('%Y%m%d%H%M%S')
                    rowkey   = bytes("{0}-{1:03}-{2:02}".format(moment, house, device), 'UTF-8')
                    dev_ip   = "192.168.1.14{}".format(device)
                    dev_type = str(device)
                    dev_temp = str(random.randint(20, 30))
                    dev_humi = str(random.randint(30, 40) / 100)

                    batch.put(rowkey, {
                        b"device:ip": bytes(dev_ip, 'UTF-8'),
                        b"device:type": bytes(dev_type, 'UTF-8'),
                        b"metric:temp": bytes(dev_temp, 'UTF-8'),
                        b"metric:humidity": bytes(dev_humi, 'UTF-8')
                    })


@time_it
def hbase_scan():
    connection = happybase.Connection('localhost')
    iot_table  = connection.table("iot")
    initial_dt = dt.datetime.now()
    initial_dt_str = initial_dt.strftime('%Y%m%d%H%M%S')
    row_key_from = "{}-001-00".format(initial_dt_str)
    row_key_to   = "{}-001-05".format(initial_dt_str)

    results = iot_table.scan(
        limit=100,
        row_start=bytes(row_key_from, 'UTF-8'),
        row_stop=bytes(row_key_to, 'UTF-8'),
        columns=[b"device:ip", b"device:type", b"metric:temp"],
        filter=b"SingleColumnValueFilter('metric', 'temp', >, 'binary:25')"
    )

    for row in results:
        print(row)

@time_it
def hbase_scan_more_filters():
    connection = happybase.Connection('localhost')
    iot_table  = connection.table("iot")
    initial_dt = dt.datetime.now()
    initial_dt_str = initial_dt.strftime('%Y%m%d%H%M%S')
    row_key_from = "{}-001-00".format(initial_dt_str)
    row_key_to   = "{}-001-05".format(initial_dt_str)

    results = iot_table.scan(
        limit=100,
        row_start=bytes(row_key_from, 'UTF-8'),
        row_stop=bytes(row_key_to, 'UTF-8'),
        columns=[b"device:ip", b"device:type", b"metric:temp"],
        filter=b"""\
        SingleColumnValueFilter('metric', 'temp', >=, 'binary:28') AND \
        SingleColumnValueFilter('metric', 'temp', <=, 'binary:30') \
        """
    )

    for row in results:
        print(row)
