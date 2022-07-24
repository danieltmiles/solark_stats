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
    "pv1": {
        "registers": (186,),
        "factor": 1,
    },
    "pv2": {
        "registers": (187,),
        "factor": 1,
    },
    "grid_frequency": {
        "registers": (79,),
        "factor": 1,
    },
    "dc_dc_transformer_temperature": {
        "registers": (90,),
        "factor": 10,
    },
    "faults": {
        "registers": (103, 104, 105, 106),
        "factor": 1,
    },
    "dc_voltage_1": {
        "registers": (109,),
        "factor": 0.1,
    },
    "dc_current_1": {
        "registers": (110,),
        "factor": 0.1,
    },
    "dc_voltage_2": {
        "registers": (111,),
        "factor": 0.1,
    },
    "dc_current_2": {
        "registers": (112,),
        "factor": 0.1,
    },
    "grid_side_voltage_l1-n": {
        "registers": (150,),
        "factor": 0.1,
    },
    "grid_side_voltage_l2-n": {
        "registers": (151,),
        "factor": 0.1,
    },
    "grid_side_voltage_l1-l2": {
        "registers": (152,),
        "factor": 0.1,
    },
    "voltage_at_middle_side_of_relay_l1-l2": {
        "registers": (153,),
        "factor": 0.1,
    },
    "Inverter_output_voltage_l1-n ": {
        "registers": (154,),
        "factor": 0.1,
    },
    "Inverter_output_voltage_l2-n ": {
        "registers": (155,),
        "factor": 0.1,
    },
    "Inverter_output_voltage_l1-l2 ": {
        "registers": (156,),
        "factor": 0.1,
    },
    "grid_side_current_l1": {
        "registers": (160,),
        "factor": 0.01,
    },
}


def get_daily_stat(stat: str):
    now = datetime.datetime.now()
    start_ts = f"{now.year:04}-{now.month:02}-{now.day:02}T07:00:00Z"
    start_dt = datetime.datetime.strptime(start_ts, '%Y-%m-%dT%H:%M:%SZ')
    end_dt = start_dt + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
    end_ts = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    args = {
        'epoch': 'ms',
        'db': 'solar',
        'q': f"SELECT value FROM \"{stat}\"WHERE time >= '{start_ts}' AND time <= '{end_ts}' fill(0)",
    }
    uri = f'http://localhost:8086/query?{urllib.parse.urlencode(args)}'
    resp = requests.post(uri)
    try:
        values = resp.json()['results'][0]['series'][0]['values']
    except KeyError as kerr:
        logging.error("unable to parse influxdb response:\n%s\n" % resp.text)
        values = []
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

resp = requests.get('http://localhost:8086/ping')
while resp.status_code != 204:
    print("influx not up yet, sleeping...")
    time.sleep(1)
    resp = requests.get('http://localhost:8086/ping')

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
        if count % 99 == 0:
        #if True:
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

            draw_wh, _push_wh = get_daily_stat("load")
            logging.info(f"daily load: {draw_wh}")
            line = f"load_draw_wh value={draw_wh} {timestamp}"
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
