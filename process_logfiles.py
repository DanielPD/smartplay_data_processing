import csv
from datetime import datetime
import os

DATA_DIR = 'data'
CLOSENESS_OUTPUT_FILE='closeness_scores.csv'
ANSWERS_OUTPUT_FILE='beacon_answers.csv'
SMARTWATCH_BT_ADDR_WHITELIST = ['49:C6:9F:9F:62:8D','6E:4C:8D:9B:AD:6B','5B:82:E2:0C:0C:C0','68:C5:BE:7A:EC:C3','73:93:2F:43:7E:26','55:1A:39:67:D6:42','50:CA:24:F7:E0:D3','6E:2A:50:B1:32:EA','AC:80:FB:30:25:FB','70:8F:13:71:A8:6D','64:7E:C2:DF:48:5D','5D:DC:5D:24:D7:2F','4b:1a:24:ea:2e:6c','51:5e:d0:64:75:26','78:76:07:a8:50:3d','68:55:72:AC:67:C4','68:6B:78:CB:6D:8D','61:41:25:18:A0:FF','40:0E:19:95:AF:8C','55:C6:02:C7:03:1D','4B:63:A1:DF:24:2E','4C:6D:CD:79:12:3A']
BEACON_BT_ADDR = ['D5:0e:84:34:3A:3A','ED:E3:26:AF:5C:FE','E7:97:7E:A8:E3:F1','e0:df:98:16:0f:75','C7:75:AB:1D:6A:DE','DA:74:E4:CA:D5:8F']

def get_all_csv_files(directory, filename_pattern):
    """Recursively get all csv files in a directory that contain the provided pattern somewhere in the filename"""
    csv_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv') and filename_pattern in file:
                csv_files.append(os.path.join(root, file))
    return csv_files

def read_bluetooth_csv(file_path):
    """Read a csv file with bluetooth data and return a list of dictionaries with the unprocessed data of which bluetooth devices were detected at which timestamp"""
    data = []
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file, delimiter=',')
        next(csv_reader) #skip headers
        for timestamp,*devices in csv_reader: #there could be multiple devices in a row, this loads them all in a list
            timestamp = datetime.fromtimestamp(int(timestamp) / 1000.0)
            data.append({'timestamp': timestamp,'devices': devices})	
    return data

def read_answers_csv(file_path):
    """Read a csv file with answers data and return a list of dictionaries containing all questions asked and answers given"""
    data = []
    with open(file_path, mode='r', encoding='utf8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            row['timestamp'] = datetime.fromtimestamp(int(row['timestamp']) / 1000.0)
            data.append(row)
    return data

def parse_bluetooth_data(data):
    """Parse the unprocessed bluetooth data and return a list of dictionaries with the timestamp and a dictionary of devices and their signal strength"""
    results = []
    for row in data:
        devices = {}
        for key, value in row.items():
            if key == 'devices': 
                for device in value:
                    device_info = device.split('--')
                    if len(device_info) == 2:
                        devices[device_info[0]] = 128-int(device_info[1]) #invert the signal strength so that higher number == better signal
        
        results.append({
            'timestamp': row['timestamp'],
            'devices': devices
        })
    return results

def calc_cumulative_closeness_score(data):
    """Calculate the time-in-range and cumulative 'closeness' score for each device in the data"""
    closeness_scores = {}
    for row in data:
        for device, signal_strength in row['devices'].items():
            if device not in SMARTWATCH_BT_ADDR_WHITELIST:
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

def extract_beacon_visits(data):
    """Extract all visits to beacons from the bluetooth data, only keeping the first and last timestamp"""
    beacons = {}
    for row in data:
        for device, signal_strength in row['devices'].items():
            if device not in BEACON_BT_ADDR:
                continue

            if device in beacons:
                if row['timestamp'] < beacons[device]['first_seen']:
                    beacons[device]['first_seen'] = row['timestamp']
                if row['timestamp'] > beacons[device]['last_seen']:
                    beacons[device]['last_seen'] = row['timestamp']
            else:
                beacons[device] = {
                    'first_seen': row['timestamp'],
                    'last_seen': row['timestamp']
                }
    return beacons

def add_answers_to_beacons(beacons, answers):
    """Add the answers to the beacons that were in range when the answer was given"""
    for answer in answers:
        if answer['answer'] == 'ASKED':
            continue
        
        for beacon_id, visit in beacons.items():
            if visit['first_seen'] < answer['timestamp'] < visit['last_seen']:
                if 'answers' not in beacons[beacon_id]:
                    beacons[beacon_id]['answers'] = []
                beacons[beacon_id]['answers'].append(answer)
    return beacons

def main():
    """Main function that processes all data and stores the results in csv files"""	
    closeness_scores = {}
    beacon_answers = {}

    smartwatch_ids = next(os.walk(DATA_DIR))[1]
    for id in smartwatch_ids:
        print(f"Processing data for smartwatch {id}")

        # First extract and process all bluetooth device data
        bluetooth_log_files = get_all_csv_files(DATA_DIR+'\\'+id, '_BT_')
        bluetooth_data = []
        for file_path in bluetooth_log_files:
            print(f"Processing BT file {file_path}")
            bluetooth_data += parse_bluetooth_data(read_bluetooth_csv(file_path))

        # Calculate the cumulative closeness score for each device
        closeness_scores[id] = calc_cumulative_closeness_score(bluetooth_data)

        # Next extract and process all answers data
        answers_log_files = get_all_csv_files(DATA_DIR+'\\'+id, '_QUESTIONS_')
        answers_data = []
        for file_path in answers_log_files:
            print(f"Processing QUESTIONS file {file_path}")
            answers_data += read_answers_csv(file_path)

        # Now combine the bluetooth data with the answers data, storing which answers were given when a beacon was in range
        beacon_visits = extract_beacon_visits(bluetooth_data)
        beacon_answers[id] = add_answers_to_beacons(beacon_visits, answers_data)
        
    # Finally store the results in csv files
    with open(CLOSENESS_OUTPUT_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['watch_id','bt_device', 'TIR', 'RSSI'])
        for id,scores in closeness_scores.items():
            for device, scores in scores.items():
                print(f"Storing data for watch ID {id}: Device {device} was in range for {scores['TIR']} measurements with a total closeness score of {scores['RSSI']}")
                writer.writerow([id, device, scores['TIR'], scores['RSSI']])

    with open(ANSWERS_OUTPUT_FILE, mode='w', newline='', encoding="utf8") as file:
        writer = csv.writer(file)
        writer.writerow(['watch_id','beacon_id', 'questionID', 'questionText', 'answer'])
        for id,beacons in beacon_answers.items():
            for beacon_id, visit in beacons.items():
                if 'answers' not in visit:
                    continue

                for answer in visit['answers']:
                    print(f"Storing data for watch ID {id}: Near Beacon {beacon_id} question {answer['questionID']} was answered with {answer['answer']}")
                    writer.writerow([id, beacon_id, answer['questionID'], answer['questionText'], answer['answer']])

if __name__ == "__main__":
    main()