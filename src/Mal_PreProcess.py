import pandas as pd
import os

def process_malicious(folder_path):
    """
    Processes all malicious .binetflow files in the specified folder.
    """
    data = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.binetflow'):
                file_path = os.path.join(root, file)
                try:
                    # Read the file into a DataFrame
                    df = pd.read_csv(file_path, delimiter=',', header=0)
                    data.append(df)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

if __name__ == "__main__":
    # Path to the malicious data folder
    malicious_path = r"CBSeq\data\malicious"
    
    # Process malicious data
    malicious_data = process_malicious(malicious_path)
    
    # Add label for malicious data
    malicious_data['label'] = 1

    # Select only the required columns
    required_columns = [
        "StartTime", "Dur", "Proto", "SrcAddr", "Sport", "Dir", 
        "DstAddr", "Dport", "State", "sTos", "dTos", 
        "TotPkts", "TotBytes", "SrcBytes", "label"
    ]
    malicious_data = malicious_data[required_columns]

    # Save the preprocessed dataset
    malicious_data.to_csv('data/malicious_preprocessed.csv', index=False)
    print("Malicious data preprocessing complete. Saved to 'data/malicious_preprocessed.csv'.")
    print(malicious_data['label'].value_counts())
