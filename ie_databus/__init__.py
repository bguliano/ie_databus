from threading import Event, Lock
import paho.mqtt.client as mqtt
import json
from dataclasses import dataclass
from typing import Dict


@dataclass
class Sensor:
    name: str
    id: str
    data_type: str
    qc: int
    ts: str
    val: float


class IEDatabus:
    def __init__(self, username: str, password: str):
        self.client = mqtt.Client()
        self.client.username_pw_set(username, password)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect('ie-databus')

        self._sensors = {}
        self._sensors_lock = Lock()
        
        self.sensor_headers = {}
        self.ready_event = Event()

    def start(self):
        self.client.loop_start()
        self.ready_event.wait()

    def stop(self):
        self.client.loop_stop()

    @property
    def sensors(self) -> Dict[str, Sensor]:
        with self._sensors_lock:
            value = self._sensors.copy()
        return value

    @sensors.setter
    def sensors(self, value: Dict[str, Sensor]):
        with self._sensors_lock:
            self._sensors = value

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print('Connected successfully')
        else:
            print('Error: ' + str(rc))
        client.subscribe('ie/#')

    def on_message(self, client, userdata, msg):
        data = json.loads(msg.payload.decode())
        if len(self.sensor_headers) == 0:
            try:
                dpds = data['connections'][0]['dataPoints'][0]['dataPointDefinitions']
            except KeyError:
                pass
            else:
                for data_point in dpds:
                    self.sensor_headers[data_point['id']] = data_point
        else:
            # create sensors
            sensors = {}
            for value_dict in data['vals']:
                header = self.sensor_headers[value_dict['id']]
                sensors[header['name']] = Sensor(name=header['name'],
                                                 id=header['id'],
                                                 data_type=header['dataType'],
                                                 qc=value_dict['qc'],
                                                 ts=value_dict['ts'],
                                                 val=value_dict['val']
                                                )
            self.sensors = sensors
            self.ready_event.set()


if __name__ == '__main__':
    databus = IEDatabus()
    databus.start()
    
    for key, sensor in databus.sensors.items():
        print(f'{key}: {sensor.val}')
