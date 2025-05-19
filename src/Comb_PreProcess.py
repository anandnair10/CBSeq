import pandas as pd

# Paths to the preprocessed datasets
benign_file = 'data/benign_preprocessed.csv'
malicious_file = 'data/malicious_preprocessed.csv'

# Load the preprocessed datasets
benign_data = pd.read_csv(benign_file)
malicious_data = pd.read_csv(malicious_file)

# Combine the datasets
combined_data = pd.concat([benign_data, malicious_data], ignore_index=True)

# Select only the required columns for training
required_columns = ['StartTime','Dur','Proto','SrcAddr','Sport','Dir','DstAddr','Dport','State','sTos','dTos','TotPkts','TotBytes','SrcBytes','label']
refined_data = combined_data[required_columns]

# Handle missing values (example: fill missing values with 0)
refined_data = refined_data.fillna(0)

# Save the refined dataset
refined_data.to_csv('data/refined_dataset.csv', index=False)
print("Refined dataset saved to 'data/refined_dataset.csv'")
