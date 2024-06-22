import re
import pandas as pd

def get_metadata(log_file_path,stats):
    file_path = log_file_path

    # Initialize lists to store object types, object names, statuses, and execution statuses
    object_types = []
    object_names = []
    statuses = []  # Modified: Changed the name to 'statuses' to store the combined status
    timestamps = []
    execution_statuses = []

    # Read the log file line by line
    with open(file_path, "r") as file:
        # Initialize variables to capture status lines
        capturing_status = False
        status_lines = []
        execution_status = ''
        timestamp = ''

        for line in file:
            # Extract timestamp
            timestamp_match = re.match(r'\[(.*?)\]', line)
            if timestamp_match:
                timestamp = timestamp_match.group(1)

            # Check if the line contains "Deploying_File_Name" and ".sql"
            if "Deploying_File_Name" in line and ".sql" in line:
                # Use regular expression to extract text inside quotes
                matches = re.findall(r'"(.*?\.sql)"', line)
                if matches:
                    # Extract the matched text inside quotes
                    matched_text = matches[0]
                    # Remove the '.sql' extension from the matched text
                    object_name = matched_text.replace('.sql', '')
                    # Split the matched text into object type and object name
                    parts = object_name.split('_')
                    if len(parts) >= 2:
                        object_type = parts[0]
                        object_name = "_".join(parts[1:])
                        # Append to respective lists
                        object_types.append(object_type)
                        object_names.append(object_name)

                        # Set the flag to start capturing status lines
                        capturing_status = True
            elif capturing_status:
                if 'Deployment completed' in line:
                    capturing_status = False
                    execution_status = 'SUCCESS'
                elif 'Deployment failed' in line:
                    capturing_status = False
                    execution_status = 'FAIL'
                else:
                    # Remove timestamp from the line and append it to status_lines
                    line_without_timestamp = re.sub(r'^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] ', '', line)
                    status_lines.append(line_without_timestamp.strip())

                if not capturing_status:
                    # Combine status lines and add to the statuses list
                    status = '\n'.join(status_lines)
                    statuses.append(status)
                    status_lines = []  # Reset status lines for the next object
                    timestamps.append(timestamp)
                    execution_statuses.append(execution_status)

    # Create a Pandas DataFrame
    deployment_stats = pd.DataFrame({
        'Timestamp': timestamps,
        'Object_name': object_names,
        'Object_type': object_types,
        'EXECUTION_STATUS': execution_statuses,
        'Status': statuses
    })

    # Save the DataFrame to a CSV file
    csv_file_path = f"../{stats}.csv"
    deployment_stats.to_csv(csv_file_path, index=False)

    print(f"Data saved to {csv_file_path}")
    return csv_file_path
