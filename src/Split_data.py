from sklearn.model_selection import train_test_split
import pandas as pd

# Paths to the preprocessed datasets

file_path = 'data/refined_dataset.csv'

# Load the datasets
# file path contains the refined datasets.
data = pd.read_csv(file_path)

data['StartTime'] = pd.to_datetime(data['StartTime'])
data['EndTime'] = data['StartTime'] + pd.to_timedelta(data['Dur'], unit='m')

# Separate features (X) and labels (y)
X = data.drop(columns=['label'])  # All columns except 'label'
y = data['label']  # 'label' column as target

# Split into training and testing sets (80% train, 20% test)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Add the label back to training and testing datasets for saving
train_data = pd.concat([X_train, y_train], axis=1)
test_data = pd.concat([X_test, y_test], axis=1)

# Save the training and testing datasets
train_data.to_csv('data/train_dataset.csv', index=False)
test_data.to_csv('data/test_dataset.csv', index=False)

print("Dataset split complete.")
print("Training set saved to 'data/train_dataset.csv")
print("Testing set saved to 'data/test_dataset.csv")
