import csv
from datetime import datetime, timedelta
import os

DATA_DIR = 'data'
CLOSENESS_OUTPUT_FILE='closeness_scores.csv'
ANSWERS_OUTPUT_FILE='beacon_answers.csv'
BEACON_BT_ADDR = ['D5:0E:84:34:3A:3A','ED:E3:26:AF:5C:FE','E7:97:7E:48:E3:F1','E0:DF:98:16:0C:75','C7:75:AB:1D:6A:DE','DA:74:E4:CA:D5:8F']
BEACON_VISIT_WINDOW = 60 # seconds
BLE_ADDR_FILE = 'devices.csv'

def get_all_csv_files(directory, filename_pattern):
    """Recursively get all csv files in a directory that contain the provided pattern somewhere in the filename"""
    csv_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv') and filename_pattern in file:
                csv_files.append(os.path.join(root, file))
    return csv_files

def read_device_addr_csv(file_path):
    """Read a csv file with bluetooth device data and return a dictionary that maps device names to their BLE addresses"""
    devices = {}
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            timestamp, name, address = row
            name = name.split('(')[-1].split(')')[0]
            if name in devices:
                devices[name].add(address)
            else:
                devices[name] = {address}
    return devices

def invert_device_addr_lut(devices):
    """Invert the look-up-table that maps device names to their BLE addresses"""
    inverted = {}
    for name, addresses in devices.items():
        for address in addresses:
            inverted[address] = name
    return inverted

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
    addr_lut = invert_device_addr_lut(read_device_addr_csv(BLE_ADDR_FILE))

    closeness_scores = {}
    for row in data:
        for addr, signal_strength in row['devices'].items():
            if addr not in addr_lut:
                continue
            
            device = addr_lut[addr]
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

def extract_beacon_visits(bt_data):
    beacons = []

    for row in bt_data:
        devices = {}
        for device, signal_strength in row['devices'].items():
            if device not in BEACON_BT_ADDR:
                continue
            
            devices[device] = signal_strength
            print(f"Beacon {device} was in range with signal strength {signal_strength} at {row['timestamp']}")

        if len(devices) > 0:
            beacons.append({
                'timestamp': row['timestamp'],
                'devices': devices
            })

    return beacons

def search_beacon_visits(beacons_data, timestamp):
    """Search the beacons that were in range around the given timestamp"""
    beacons = []
    for row in beacons_data:
        for device, signal_strength in row['devices'].items():
            if device not in BEACON_BT_ADDR:
                continue

            if device not in beacons:
                lower_time = timestamp - timedelta(seconds=BEACON_VISIT_WINDOW)
                upper_time = timestamp + timedelta(seconds=BEACON_VISIT_WINDOW)
                if row['timestamp'] >= lower_time and row['timestamp'] <= upper_time:
                    beacons.append(device)
    return beacons


def add_answers_to_beacons(beacons_data, answers):
    """Add the answers to the beacons that were in range when the answer was given"""
    beacons_answers = {}
    for answer in answers:
        if answer['answer'] == 'ASKED':
            continue
        
        beacons_in_range = search_beacon_visits(beacons_data, answer['timestamp'])
        for beacon in beacons_in_range:
            if beacon not in beacons_answers:
                beacons_answers[beacon] = {}
            if 'answers' not in beacons_answers[beacon]:
                beacons_answers[beacon]['answers'] = []

            beacons_answers[beacon]['answers'].append(answer)
    return beacons_answers

def main():
    """Main function that processes all data and stores the results in csv files"""	
    
    closeness_scores = {}
    beacons_answers = {}

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
        beacons_data = extract_beacon_visits(bluetooth_data)
        beacons_answers[id] = add_answers_to_beacons(beacons_data, answers_data)
        
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
        for id,beacons in beacons_answers.items():
            for beacon_id, visit in beacons.items():
                if 'answers' not in visit:
                    continue

                for answer in visit['answers']:
                    print(f"Storing data for watch ID {id}: Near Beacon {beacon_id} question {answer['questionID']} was answered with {answer['answer']}")
                    writer.writerow([id, beacon_id, answer['questionID'], answer['questionText'], answer['answer']])

if __name__ == "__main__":
    main()