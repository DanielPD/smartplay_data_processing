import asyncio
from bleak import BleakScanner
import time

async def main():
    while True:
        devices = await BleakScanner.discover()
        for device in devices:
            if device.name is not None and "Galaxy" in device.name:
                with open('devices.csv', 'a') as f:
                    timestamp = int(time.time() * 1000)
                    line = f"{timestamp},{device.name},{device.address}"
                    print(line)
                    f.write(line+"\n")
        await asyncio.sleep(120)


with open('devices.csv', 'a') as f:
    f.write("timestamp,name,address\n")

asyncio.run(main())