import pandas as pd
from datetime import timedelta

def channel_traffic_aggregation(data, channel_traffic):
    # Ensure StartTime is in datetime format
    data['StartTime'] = pd.to_datetime(data['StartTime'], errors='coerce')

    # Define the time window (24 hours)
    time_window = timedelta(hours=24)

    # Group traffic by SrcAddr and DstAddr (channels)
    group_channel = data.groupby(['SrcAddr', 'DstAddr'])

    # Initialize an empty list to store aggregated channel data
    aggregate_data = []

    for (src, dst), group in group_channel:
        # Sorting traffic by start time
        group = group.sort_values(by='StartTime')

        # Get the start time of the channel
        channel_start = group['StartTime'].min()

        window_start = group.iloc[0]['StartTime']
        window_end = window_start + time_window

        current_window = []

        for _, row in group.iterrows():
            if row['StartTime'] < window_end:  # Add the row to the current window
                current_window.append(row)
            else:
                # Aggregate the current window
                aggregate_data.append({
                    'SrcAddr': src,
                    'DstAddr': dst,
                    'ChannelStart': channel_start,
                    'WindowStart': window_start,
                    'WindowEnd': window_end,
                    'Duration': (window_end - window_start).total_seconds(),
                    'FlowCount': len(current_window),
                    'TotalDataSize': sum(flow['TotBytes'] for flow in current_window),
                    'UplinkDataSize': sum(flow['SrcBytes'] for flow in current_window),
                    'DownlinkDataSize': sum(flow['TotBytes'] - flow['SrcBytes'] for flow in current_window),
                    'Label': current_window[0]['label']  # Add the label column
                })
                # Start a new window
                window_start = row['StartTime']
                window_end = window_start + time_window
                current_window = [row]

        # Aggregate the last window if not empty
        if current_window:
            aggregate_data.append({
                'SrcAddr': src,
                'DstAddr': dst,
                'ChannelStart': channel_start,
                'WindowStart': window_start,
                'WindowEnd': window_end,
                'Duration': (window_end - window_start).total_seconds(),
                'FlowCount': len(current_window),
                'TotalDataSize': sum(flow['TotBytes'] for flow in current_window),
                'UplinkDataSize': sum(flow['SrcBytes'] for flow in current_window),
                'DownlinkDataSize': sum(flow['TotBytes'] - flow['SrcBytes'] for flow in current_window),
                'Label': current_window[0]['label']  # Add the label column
            })

    # Convert aggregated data to a DataFrame
    aggregated_df = pd.DataFrame(aggregate_data)

    # Save the aggregated data
    aggregated_df.to_csv(channel_traffic, index=False)

def generate_behavior_sequence(data, output_file):
    # Ensure StartTime is in datetime format
    data['StartTime'] = pd.to_datetime(data['StartTime'], errors='coerce')

    # Group by channels (SrcAddr and DstAddr)
    grouped_channels = data.groupby(['SrcAddr', 'DstAddr'])

    behavior_sequences = []

    for (src, dst), group in grouped_channels:
        # Sort by StartTime
        group = group.sort_values(by='StartTime')

        # Get the start time of the channel
        channel_start = group['StartTime'].min()

        # Generate sequences
        pn_sequence = list(group['TotPkts'])
        iat_sequence = list(group['StartTime'].diff().dt.total_seconds().fillna(0))
        sp_sequence = list(group['Sport'])
        dp_sequence = list(group['Dport'])
        label = group['label'].iloc[0]  # Take the label for the channel

        behavior_sequences.append({
            'SrcAddr': src,
            'DstAddr': dst,
            'ChannelStart': channel_start,
            'PNSequence': pn_sequence,
            'IATSequence': iat_sequence,
            'SPSequence': sp_sequence,
            'DPSequence': dp_sequence,
            'Label': label  # Add the label column
        })

    # Convert to DataFrame
    behavior_df = pd.DataFrame(behavior_sequences)

    # Save behavior sequences to output file
    behavior_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    # Load the input dataset
    data = pd.read_csv('C:/Users/ANAND NAIR/Desktop/me/stuff/drdo/CBSeq/src/data/test_dataset.csv')

    # Ensure the label column exists in the input dataset
    if 'label' not in data.columns:
        raise ValueError("The input dataset must contain a 'label' column.")

    # File paths
    channel_traffic = 'C:/Users/ANAND NAIR/Desktop/me/stuff/drdo/CBSeq/src/data/test/channel_traffic.csv'
    behavior_sequence_file = 'C:/Users/ANAND NAIR/Desktop/me/stuff/drdo/CBSeq/src/data/test/behavior_sequences.csv'

    # Perform channel traffic aggregation
    channel_traffic_aggregation(data, channel_traffic)
    print("Aggregated channel traffic saved to", channel_traffic)

    # Generate behavior sequences
    generate_behavior_sequence(data, behavior_sequence_file)
    print("Behavior sequences saved to", behavior_sequence_file)
