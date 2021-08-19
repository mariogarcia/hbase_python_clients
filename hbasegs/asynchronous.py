import asyncio
import random
import datetime as dt

from happybase import table

from hbasegs.util import time_it
from aiohappybase import Connection, ConnectionPool

@time_it
def hbase_create():
    connection = Connection('localhost')
    connection.create_table(
        'iot',
        {
            'device': dict(),
            'metric': dict()
        })


@time_it
def hbase_put():
    asyncio.run(get_results())


async def get_results():
    await asyncio.gather(hbase_put_asyn())


async def hbase_put_asyn():    
    initial_dt = dt.datetime.now()
    no_seconds = 10000
    no_houses  = 100
    no_devices = 5
    no_rows    = no_seconds * no_houses * no_devices

    async with ConnectionPool(host='localhost', size=10) as pool:        
        async with pool.connection() as connection:
            table  = connection.table("iot")
            print("INSERTING {} ROWS".format(no_rows))
            
            async with table.batch(batch_size=100) as batch: # improving insertion by batching                                        
                for i in range(0, no_seconds):
                    for h in range(0, no_houses): # number of houses
                        for d in range(0, no_devices):
                            moment   = (initial_dt + dt.timedelta(seconds=i)).strftime('%Y%m%d%H%M%S')
                            rowkey   = bytes("{0}-{1:03}-{2:02}".format(moment, h, d), 'UTF-8')
                            dev_ip   = "192.168.1.14{}".format(d)
                            dev_type = str(d)
                            dev_temp = str(random.randint(20, 30))
                            dev_humi = str(random.randint(30, 40) / 100)

                            await batch.put(rowkey, {
                                b"device:ip": bytes(dev_ip, 'UTF-8'),
                                b"device:type": bytes(dev_type, 'UTF-8'),
                                b"metric:temp": bytes(dev_temp, 'UTF-8'),
                                b"metric:humidity": bytes(dev_humi, 'UTF-8')
                            })                        
        print("INSERTION PROCESS FINISHED!")


async def query(pool, temperature, operation_name):
    print("{} executing!".format(operation_name))
    async with pool.connection() as conn:
        table = conn.table('iot')
        initial_dt = dt.datetime.now()
        initial_dt_str = initial_dt.strftime('%Y%m%d%H%M%S')
        row_key_from = "{}-001-00".format(initial_dt_str)
        row_key_to   = "{}-001-05".format(initial_dt_str)
        filter       = "SingleColumnValueFilter('metric', 'temp', >, 'binary:{}')".format(temperature)

        return [x async for x in table.scan(
            limit=1,
            row_start=bytes(row_key_from, 'UTF-8'),
            row_stop=bytes(row_key_to, 'UTF-8'),
            columns=[b"device:ip", b"device:type", b"metric:temp"],
            filter=bytes(filter, 'UTF-8')
        )]


def light_op_1(pool):
    return query(pool, 25, 'light_op_1')


def light_op_2(pool):
    return query(pool, 26, 'light_op_2')


async def heavy_operation_1(pool):
    await asyncio.sleep(2)    
    result = await query(pool, 27, 'heavy_operation_1')
    return result


def print_results(title, results):
    print("")
    title = "{} RESULTS".format(title)
    print(title)
    print("=" * len(title))
    if len(results) > 0:
        for r in results:
            print(r)
    else:
        print("NO RESULTS")


async def heavy_operation_aio():
    async with ConnectionPool(host="localhost", size=10) as pool:
        heavy_1_task = asyncio.Task(heavy_operation_1(pool))
        light_1_task = asyncio.Task(light_op_1(pool))
        light_2_task = asyncio.Task(light_op_2(pool))

        heavy_res, light_1_res, light_2_res = await asyncio.gather(
            heavy_1_task, 
            light_1_task, 
            light_2_task)

        print_results("HEAVY", heavy_res)
        print_results("LIGHT (1)", light_1_res)
        print_results("LIGHT (2)", light_2_res)


@time_it
def hbase_aio_use_case_main():
    asyncio.run(heavy_operation_aio())