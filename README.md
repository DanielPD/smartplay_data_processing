# SmartPlay - Bluetooth Closeness and Answers Data Processing

## Description
This project is a component of the SmartPlay initiative, which involves distributing smartwatches to participants. These smartwatches periodically prompt users with questions regarding their current feelings. The objective is to correlate the participants' responses with specific locations (identified via Bluetooth beacons) and their social interactions (measured through closeness scores). This script represents an initial attempt to visualize the data collected from multiple smartwatches.

## Files
- `process_logfiles.py`: Main script to process the data and store the results in CSV files.
- `data`: Directory containing subdirectories for each smartwatch, each containing Bluetooth and questions data files.
- `beacon_answers.csv`: Output file containing the answers given when a beacon was in range.
- `closeness_scores.csv`: Output file containing the cumulative closeness scores for each Bluetooth device.

## Usage
1. Place the Bluetooth and questions data files in the appropriate subdirectories under the data directory.
2. Run the `process_logfiles.py` script to process the data and generate the output files.

## Output
- `beacon_answers.csv`: Contains the answers given when a beacon was in range.
- `closeness_scores.csv`: Contains the cumulative closeness scores for each Bluetooth device.

## License
This project is licensed under the MIT License.
