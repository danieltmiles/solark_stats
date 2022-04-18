#!/usr/bin/env python
from pymodbus.client.sync import ModbusSerialClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.payload import BinaryPayloadBuilder

import datetime
import logging
import decimal
import re
import requests
import subprocess
import time
import urllib

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

client = ModbusSerialClient(
    method='rtu',
    port='/dev/ttyUSB0',
    baudrate=9600,
    timeout=10,
    parity='N',
    stopbits=1,
    bytesize=8
)
stats_to_get = {
    "battery_amps": {
        "registers": (191,),
        "factor": .01,
    },
    "battery_load": {
        "registers": (190,),
        "factor": 1,
    },
    "battery_soc_percent": {
        "registers": (184,),
        "factor": 1,
    },
    "daily_solar_power": {
        "registers": (108,),
        "factor": 100,
    },
    "grid": {
        "registers": (169,),
        "factor": 1,
    },
    "load": {
        "registers": (178,),
        "factor": 1,
    },
    "pv": {
        "registers": (186, 187),
        "factor": 1,
    },
}


def get_daily_stat(stat: str):
    now = datetime.datetime.now()
    start_ts = f"{now.year:04}-{now.month:02}-{now.day:02}T07:00:00Z"
    end_ts = f"{now.year:04}-{now.month:02}-{now.day+1:02}T06:59:59Z"
    args = {
        'epoch': 'ms',
        'db': 'solar',
        'q': f"SELECT value FROM \"{stat}\"WHERE time >= '{start_ts}' AND time <= '{end_ts}' fill(0)",
    }
    uri = f'http://localhost:8086/query?{urllib.parse.urlencode(args)}'
    resp = requests.post(uri)
    values = resp.json()['results'][0]['series'][0]['values']
    push_wh = 0.0
    draw_wh = 0.0
    for i, value in enumerate(values):
        if i < len(values)-1:
            duration = values[i+1][0] - value[0]
        else:
            duration = 1000
        watt_milliseconds = value[1] * duration
        watt_hours = watt_milliseconds / 1000 / 60 / 60
        if value[1] < 0:
            push_wh += watt_hours * -1
        else:
            draw_wh += watt_hours
    return draw_wh, push_wh

conn = client.connect()
if conn is not None and not isinstance(conn, Exception):
    count = 0
    pat = re.compile(r"^temp=(.*?)'C$")
    while True:
        count += 1
        count = count % 100
        for stat, details in stats_to_get.items():
            total_val = 0
            for register_number in details.get("registers"):
                res = client.read_holding_registers(address=register_number, count=1, unit=1)
                if res is None or isinstance(res, Exception):
                    print(res)
                    continue
                decoder = BinaryPayloadDecoder.fromRegisters(res.registers, byteorder=Endian.Big)
                val = decoder.decode_16bit_int()
                total_val += val
            total_val *= details.get('factor')
            #print(f"{details.get('registers')} {stat}: {total_val}")
            timestamp = decimal.Decimal(time.time() * 1000000000)
            line = f"{stat} value={total_val} {timestamp}"
            requests.post("http://localhost:8086/write?db=solar", data=line)
            if count % 5 == 0:
                logging.info(line)
        #if count % 99 == 0:
        if True:
            logging.info("logging daily grid")
            draw_wh, push_wh = get_daily_stat("grid")
            line = f"grid_draw_wh value={draw_wh} {timestamp}"
            requests.post("http://localhost:8086/write?db=solar", data=line)
            line = f"grid_push_wh value={push_wh} {timestamp}"
            requests.post("http://localhost:8086/write?db=solar", data=line)

            draw_wh, push_wh = get_daily_stat("battery_load")
            line = f"battery_draw_wh value={draw_wh} {timestamp}"
            requests.post("http://localhost:8086/write?db=solar", data=line)
            line = f"battery_charge_wh value={push_wh} {timestamp}"
            requests.post("http://localhost:8086/write?db=solar", data=line)

        try:
            proc = subprocess.Popen(["vcgencmd", "measure_temp"], stdout=subprocess.PIPE)
            out, err = proc.communicate()
            temp = pat.match(out.decode('utf-8')).group(1)
            timestamp = decimal.Decimal(time.time() * 1000000000)
            line = f"cpu_temp value={temp} {timestamp}"
            requests.post("http://localhost:8086/write?db=solar", data=line)
        except Exception as exp:
            logging.error(f"unable to get CPU temp: {exp}")
        time.sleep(1)

else:
    print("no")
