import pandas as pd
import ast
import os
from gensim.models import Word2Vec

# File paths
input_file = 'C:/Users/ANAND NAIR/Desktop/me/stuff/drdo/CBSeq/src/data/bh_seq_aftercluster.csv'
output_model_dir = "C:/Users/ANAND NAIR/Desktop/me/stuff/drdo/CBSeq/models/model"

# Ensure output directory exists
os.makedirs(output_model_dir, exist_ok=True)

# Load the dataset
try:
    df = pd.read_csv(input_file, delimiter=',', quotechar='"', on_bad_lines='skip')
    print("CSV loaded successfully.")
except pd.errors.ParserError as e:
    print(f"Error reading the CSV file: {e}")
    exit()

# Verify required columns exist
sequence_columns = ['SPSequence', 'DPSequence'] #add PNSeq, IATSeq
for column in sequence_columns:
    if column not in df.columns:
        print(f"Column {column} is missing in the CSV file!")
        exit()

# Convert sequence columns from string representations to lists
for column in sequence_columns:
    df[column] = df[column].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

# Parameters
embedding_dim = 100  # Reduced dimension size for faster processing
window_size = 5
min_word_count = 1  # Skip training embeddings for rare tokens
sg = 0  # CBOW model (set sg=1 for Skip-gram)
chunk_size = 1000  # Process data in chunks to reduce memory usage

# Train separate Word2Vec models
for sequence_column in sequence_columns:
    print(f"Training Word2Vec model for {sequence_column}...")
    sentences = []

    # Process data in chunks
    for chunk in range(0, len(df), chunk_size):
        chunk_data = df[sequence_column].iloc[chunk:chunk + chunk_size]
        sentences.extend(chunk_data.apply(lambda seq: list(map(str, seq))).tolist())

    # Train the Word2Vec model
    model = Word2Vec(
        sentences=sentences,
        vector_size=embedding_dim,
        window=window_size,
        min_count=min_word_count,
        sg=sg,
        workers=os.cpu_count()  # Utilize all available CPU cores
    )

    # Save the model
    model_file_path = os.path.join(output_model_dir, f"word2vec_{sequence_column.lower()}_model.model")
    model.save(model_file_path)
    print(f"Word2Vec model for {sequence_column} saved to '{model_file_path}'.")
