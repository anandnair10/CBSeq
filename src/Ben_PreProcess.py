import pandas as pd
import os

def process_benign(folder_path):
    """
    Processes all benign .csv files in the specified folder, skipping problematic rows.
    """
    data = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    # Use `on_bad_lines='skip'` to skip problematic rows
                    df = pd.read_csv(file_path, delimiter=',', header=0, on_bad_lines='skip', engine='python')
                    data.append(df)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

if __name__ == "__main__":
    # Path to the benign data folder
    benign_path = r"C:\Users\ANAND NAIR\Desktop\me\stuff\drdo\CBSeq\data\benign"
    
    # Process benign data
    benign_data = process_benign(benign_path)
    
    # Add label for benign data
    benign_data['label'] = 0

    # Verify available columns
    print("Available columns in the dataset:")
    print(benign_data.columns)

    # Select only the required columns
    required_columns = [
        "StartTime", "Dur", "Proto", "SrcAddr", "Sport", "Dir", 
        "DstAddr", "Dport", "State", "sTos", "dTos", 
        "TotPkts", "TotBytes", "SrcBytes", "label"
    ]

    # Check if all required columns are present
    missing_columns = [col for col in required_columns if col not in benign_data.columns]
    if missing_columns:
        print(f"Error: The following required columns are missing: {missing_columns}")
    else:
        benign_data = benign_data[required_columns]

        # Save the preprocessed dataset
        os.makedirs("data", exist_ok=True)
        benign_data.to_csv('data/benign_preprocessed.csv', index=False)
        print("Benign data preprocessing complete. Saved to 'data/benign_preprocessed.csv'.")
   
