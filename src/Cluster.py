import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import numpy as np

# Load the preprocessed file in chunks (adjust chunk size as needed)
chunk_size = 10000
input_file = 'C:/Users/ANAND NAIR/Desktop/me/stuff/drdo/CBSeq/src/data/test/behavior_sequences.csv'
output_file = 'C:/Users/ANAND NAIR/Desktop/me/stuff/drdo/CBSeq/src/data/test/clustered_behavior_sequences.csv'

# Define a fixed length for truncating/padding sequences
fixed_length = 100

# Function to clean and process sequences
def process_sequence(sequence):
    """
    Cleans and converts sequence strings to lists of floats.
    Handles hexadecimal, quotes, and other inconsistencies.
    """
    seq = list(
        map(
            lambda val: float(int(val.strip(), 16)) if '0x' in val.strip().lower() else float(val.strip()),
            sequence.strip("[]").replace("'", "").replace('"', "").split(',')
        )
    )
    # Truncate or pad to the fixed length
    return seq[:fixed_length] + [0] * (fixed_length - len(seq))

# Process and cluster data in chunks
all_results = []

for chunk in pd.read_csv(input_file, chunksize=chunk_size):
    # Ensure 'Label' column exists in the chunk
    if 'Label' not in chunk.columns:
        raise ValueError("The 'Label' column is missing in the input data.")

    # Clean and process the sequences
    columns_to_clean = ['PNSequence', 'IATSequence', 'SPSequence', 'DPSequence']
    for column in columns_to_clean:
        chunk[column] = chunk[column].apply(process_sequence)
    
    # Combine the sequences into a single feature vector per row
    chunk['FeatureVector'] = chunk.apply(
        lambda row: row['PNSequence'] + row['IATSequence'] + row['SPSequence'] + row['DPSequence'], axis=1
    )
    
    # Convert feature vectors to a numpy array
    features = np.array(chunk['FeatureVector'].tolist())
    
    # Standardize the features
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    # Perform DBSCAN clustering
    eps_value = 1  # Adjust eps as per your requirement
    min_pts = 1    # Set to 1 as per your needs
    dbscan = DBSCAN(eps=eps_value, min_samples=min_pts)
    chunk['Cluster'] = dbscan.fit_predict(features_scaled)
    
    # Convert ChannelStart to datetime for processing
    chunk['ChannelStart'] = pd.to_datetime(chunk['ChannelStart'], errors='coerce')

    # Calculate ClusterStart (earliest ChannelStart per cluster)
    cluster_start_map = chunk.groupby('Cluster')['ChannelStart'].min().to_dict()
    chunk['ClusterStart'] = chunk['Cluster'].map(cluster_start_map)

    # Append processed chunk to results
    all_results.append(chunk)

# Combine all processed chunks into a single DataFrame
result_df = pd.concat(all_results, ignore_index=True)

# Ensure the Label column is preserved in the result
if 'Label' not in result_df.columns:
    raise ValueError("The 'Label' column is missing in the processed DataFrame.")

# Save the clustering result with the Label column
result_df.to_csv(output_file, index=False)
print(f"Clustering completed. Results saved to {output_file}")
