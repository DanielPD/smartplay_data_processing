import csv
from datetime import datetime
import os

DATA_DIR = 'data'
OUTPUT_FILE='closeness_scores.csv'
SMARTWATCH_BT_WHITELIST = ['54:08:3B:C4:FC:64','51:62:7F:43:DA:1A','3E:EE:A1:BF:47:DE','29:5A:3F:E1:97:2B','74:40:BB:C2:DF:46','28:D3:3E:0A:80:93','4E:8A:2B:9E:F9:42']

def get_all_csv_files(directory, filename_pattern):
    csv_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv') and filename_pattern in file:
                csv_files.append(os.path.join(root, file))
    return csv_files

def read_bluetooth_csv(file_path):
    data = []
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file, delimiter=',')
        next(csv_reader) #skip headers
        for timestamp,*devices in csv_reader: #there could be multiple devices in a row, this loads them all in a list
            timestamp = datetime.fromtimestamp(int(timestamp) / 1000.0)
            data.append({'timestamp': timestamp,'devices': devices})	
    return data

def parse_bluetooth_data(data):
    results = []
    for row in data:
        devices = parse_bluetooth_devices(row)
        results.append({
            'timestamp': row['timestamp'],
            'devices': devices
        })
    return results

def parse_bluetooth_devices(row):
    devices = {}
    for key, value in row.items():
        if key == 'devices': 
            for device in value:
                device_info = device.split('--')
                if len(device_info) == 2:
                    devices[device_info[0]] = 128-int(device_info[1]) #invert the signal strength so that higher number == better signal
    return devices

def calc_cumulative_closeness_score(data):
    closeness_scores = {}
    for row in data:
        for device, signal_strength in row['devices'].items():
            if device not in SMARTWATCH_BT_WHITELIST:
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

def export_scores_to_csv(scores, output_file):
    with open(output_file, mode='w', newline='') as file:
        print(f"Storing data for watch ID {id}: Device {device} was in range for {scores['TIR']} measurements with a total closeness score of {scores['RSSI']}")
        writer = csv.writer(file)
        writer.writerow(['device', 'TIR', 'RSSI'])
        for device, scores in scores.items():
            writer.writerow([device, scores['TIR'], scores['RSSI']])

def main():
    closeness_scores = {}
    smartwatch_ids = next(os.walk(DATA_DIR))[1]
    for id in smartwatch_ids:
        print(f"Processing data for smartwatch {id}")
        bluetooth_log_files = get_all_csv_files(DATA_DIR+'\\'+id, '_BT_')
        bluetooth_data = []
        for file_path in bluetooth_log_files:
            print(f"Processing {file_path}")
            bluetooth_data += parse_bluetooth_data(read_bluetooth_csv(file_path))
        closeness_scores[id] = calc_cumulative_closeness_score(bluetooth_data)

    with open(OUTPUT_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['watch_id','bt_device', 'TIR', 'RSSI'])
        for id,scores in closeness_scores.items():
            for device, scores in scores.items():
                print(f"Storing data for watch ID {id}: Device {device} was in range for {scores['TIR']} measurements with a total closeness score of {scores['RSSI']}")
                writer.writerow([id, device, scores['TIR'], scores['RSSI']])



if __name__ == "__main__":
    main()