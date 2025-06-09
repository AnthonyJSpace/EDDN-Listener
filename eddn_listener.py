#!/usr/bin/python3

import zlib
import threading
import queue
import zmq
import re
import signal
import time
import os
import sqlite3
from pynput import keyboard
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from typing import Optional
from rich import print
from datetime import datetime

@dataclass_json
@dataclass
class Commodity:
    name: str
    meanPrice: int
    buyPrice: int
    stock: int
    sellPrice: int
    demand: int
    statusFlags: Optional[list[str]] = None

@dataclass_json
@dataclass
class Economy:
    name: str
    proportion: float

@dataclass_json
@dataclass
class CommodityMessage:
    systemName: str
    stationName: str
    marketId: int
    timestamp: str
    commodities: list[Commodity]
    stationType: Optional[str] = None
    carrierDockingAccess: Optional[str] = None
    horizons: Optional[bool] = None
    odyssey: Optional[bool] = None
    economies: Optional[list[Economy]] = None
    prohibited: Optional[list[str]] = None

@dataclass_json
@dataclass
class FSDJumpMessage:
    StarSystem: str
    event: Optional[str] = None
    timestamp: Optional[str] = None
    StarPos: Optional[list[float]] = None
    SystemAddress: Optional[int] = 0
    SystemAllegiance: Optional[str] = None
    SystemSecurity: Optional[str] = None
    Population: Optional[int] = 0
    Powers: Optional[list[str]] = None
    ControllingPower: Optional[str] = None
    PowerplayState: Optional[str] = None

@dataclass_json
@dataclass
class Header:
    uploaderID: str
    softwareName: str
    softwareVersion: str
    gameVersion: Optional[str] = None
    gameBuild: Optional[str] = None
    gatewayTimestamp: Optional[str] = None

@dataclass_json
@dataclass
class EDDNCommodityMessage:
    schema_ref: str = field(metadata=config(field_name="$schemaRef"))
    header: Header
    message: CommodityMessage

@dataclass_json
@dataclass
class EDDNFSDJumpMessage:
    schema_ref: str = field(metadata=config(field_name="$schemaRef"))
    header: Header
    message: FSDJumpMessage

message_queue = queue.Queue()
run_loop = True

def on_press(key):
    global run_loop
    try:
        if key == keyboard.Key.esc:
            print("ESC key pressed. Exiting...")
            run_loop = False
    except Exception as e:
        print(f"Error: {e}")

def handle_sigint(signal_received, frame):
    global run_loop
    print("\nCtrl+C detected. Exiting...")
    run_loop = False

def is_carrier(s):
    pattern = r'^[a-zA-Z0-9]{3}-[a-zA-Z0-9]{3}$'
    return bool(re.match(pattern, s))

def process_message():
    database_path = os.path.join(os.getcwd(), 'data', 'TradeDangerous.db')

    while run_loop:
        try:
            message = message_queue.get(timeout=0.1)  # wait for a message or timeout
            if message is None:
                break
            json_string = (zlib.decompress(message)).decode('utf-8')
            
            if 'commodity' in json_string:
                eddn_message = EDDNCommodityMessage.from_json(json_string)  # ignore yellow line in ide
                if not is_carrier(eddn_message.message.stationName):
                    print(f"[bold dark_cyan][Market][/] [bright_white]{eddn_message.message.systemName} - {eddn_message.message.stationName}[/] [italic bold grey50]({eddn_message.header.softwareName} {eddn_message.header.softwareVersion})")

                    # db
                    conn = sqlite3.connect(database_path)
                    cursor = conn.cursor()

                    # convert eddn timestamp from yyyy-mm-ddThh:mm:ssZ to yyyy-mm-dd hh:mm:ss as TD expects it
                    eddn_timestamp = datetime.strptime(eddn_message.message.timestamp, '%Y-%m-%dT%H:%M:%SZ')
                    db_timestamp = eddn_timestamp.strftime('%Y-%m-%d %H:%M:%S')

                    for commodity in eddn_message.message.commodities:
                        sql = """
                            SELECT item_id FROM Item
                            WHERE REPLACE(name, ' ', '') LIKE ?;
                        """
                        cursor.execute(sql, (commodity.name,))
                        commodity_id = cursor.fetchone()
                        if commodity_id is not None:
                            commodity_id = commodity_id[0]
                            
                            sql = """
                                        UPDATE StationItem SET demand_price = ?, demand_units = ?, demand_level = 0,
                                                                supply_price = ?, supply_units = ?, supply_level = 0,
                                                                modified = ?, from_live = 1
                                        WHERE station_id = ? AND item_id = ?;
                                """
                            cursor.execute(sql, (commodity.sellPrice, commodity.demand, 
                                                            commodity.buyPrice, commodity.stock, db_timestamp,
                                                            eddn_message.message.marketId, commodity_id))
                            
                            sql = """
                                UPDATE Item SET avg_price = ? WHERE item_id = ?
                            """
                            cursor.execute(sql, (commodity.meanPrice, commodity_id))
                    conn.commit()
                    conn.close()

            elif 'journal' in json_string and 'FSDJump' in json_string:
                eddn_message = EDDNFSDJumpMessage.from_json(json_string)
                if eddn_message.message.Population > 0 and (eddn_message.message.ControllingPower or eddn_message.message.PowerplayState == 'Unoccupied'):
                    print(f"[bold pale_turquoise1][System][/] [bright_white]{eddn_message.message.StarSystem}[/][bold dodger_blue1] {eddn_message.message.ControllingPower} - {eddn_message.message.PowerplayState}[/] [italic bold grey50]({eddn_message.header.softwareName} {eddn_message.header.softwareVersion})")

                    # db
                    conn = sqlite3.connect(database_path)
                    cursor = conn.cursor()

                    # convert eddn timestamp from yyyy-mm-ddThh:mm:ssZ to yyyy-mm-dd hh:mm:ss as TD expects it
                    eddn_timestamp = datetime.strptime(eddn_message.message.timestamp, '%Y-%m-%dT%H:%M:%SZ')
                    db_timestamp = eddn_timestamp.strftime('%Y-%m-%d %H:%M:%S')

                    sql = """
                            UPDATE System SET power = ?, modified = ?
                            WHERE name LIKE ?;
                        """
                    cursor.execute(sql, (eddn_message.message.ControllingPower, db_timestamp, eddn_message.message.StarSystem))

                    conn.commit()
                    conn.close()

        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error processing message: {e}")

def subscribe_to_eddn():
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)

    eddn_relay = "tcp://eddn.edcd.io:9500"
    subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
    subscriber.connect(eddn_relay)

    print(f"Subscribed to EDDN relay at {eddn_relay}")

    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)

    while run_loop:
        try:
            events = dict(poller.poll(timeout=1000))        #dict(poller.poll(timeout=60000))  # 60 sec timeout
            
            if not run_loop:
                break

            if subscriber in events:
                compressed_data = subscriber.recv()
                message_queue.put(compressed_data)
            else:
                time.sleep(0.1)  # testing needed, is this too long?
        except Exception as e:
            print(f"Error receiving message: {e}")
            time.sleep(1)

    subscriber.close()
    context.term()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sigint)

    listener = keyboard.Listener(on_press=on_press)
    listener.start()

    num_workers = 16
    workers = []
    for _ in range(num_workers):
        worker = threading.Thread(target=process_message, daemon=True)
        worker.start()
        workers.append(worker)

    eddn_thread = threading.Thread(target=subscribe_to_eddn, daemon=True)
    eddn_thread.start()

    eddn_thread.join()

    for _ in workers:
        message_queue.put(None)  # signal workers to stop

    for worker in workers:
        worker.join()

    listener.stop()
