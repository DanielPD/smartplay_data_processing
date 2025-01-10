import csv
from datetime import datetime
import os

def read_csv(file_path):
    data = []
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file, delimiter=',')
        next(csv_reader) #skip headers
        for timestamp,*devices in csv_reader: #there could be multiple devices in a row, this loads them all in a list
            timestamp = datetime.fromtimestamp(int(timestamp) / 1000.0)
            data.append({'timestamp': timestamp,'devices': devices})	
    return data

def parse_bluetooth_devices(row):
    devices = {}
    for key, value in row.items():
        if key == 'devices': 
            for device in value:
                device_info = device.split('--')
                if len(device_info) == 2:
                    devices[device_info[0]] = 128-int(device_info[1]) #invert the signal strength so that higher number == better signal
    return devices

def parse_data(data):
    results = []
    for row in data:
        devices = parse_bluetooth_devices(row)
        results.append({
            'timestamp': row['timestamp'],
            'devices': devices
        })
    return results

def get_all_csv_files(directory):
    csv_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                subdirectory = os.path.relpath(root, directory)
                csv_files.append((os.path.join(root, file), subdirectory))
    return csv_files

def calc_cumulative_closeness_score(data):
    smartwatch_bt_whitelist = ['51:62:7F:43:DA:1A','3E:EE:A1:BF:47:DE','29:5A:3F:E1:97:2B']
    closeness_scores = {}
    for row in data:
        for device, signal_strength in row['devices'].items():
            if device not in smartwatch_bt_whitelist:
                continue
            
            if device in closeness_scores:
                closeness_scores[device]["RSSI"] += signal_strength
                closeness_scores[device]["TIR"] += 1
            else:
                closeness_scores.update(
                    {
                        device: {
                            "RSSI": signal_strength,
                            "TIR": 1
                        }
                    }
                )
    return closeness_scores


def main():
    
    file_path = 'data\\0MVY\\000_BT_efa4912b299d4a4e_1736261732558.csv'
    data = read_csv(file_path)
    results = parse_data(data)
    closeness_scores = calc_cumulative_closeness_score(results)
    for device,scores in closeness_scores.items():
        print(f"Device {device} was in range for {scores['TIR']} measurements with a total closeness score of {scores['RSSI']}")

if __name__ == "__main__":
    main()