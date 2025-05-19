# CBSeq
A Channel-Level Behaviour Sequence for Encrypted Malware Traffic Detection

This project implements a machine learning pipeline for detecting malware traffic based on behavioral patterns across network channels, using Word2Vec and a transformer-based model (MSFormer)
## Project Overview

Traditional flow-level or packet-based detection methods struggle to handle encrypted and evolving malware.  
**CBSeq** addresses this by analyzing **channel-level behavior** and capturing **attacking intent** through behavior sequences.

### Pipeline Steps

1. **Channel Aggregation**  
2. **Abstract Feature Extraction** (duration, flow count, data size, etc.)  
3. **Channel Clustering** (DBSCAN on abstract features)  
4. **Behavior Sequence Construction**  
   - PN (Packet Number), IAT (Inter Arrival Time), SP (Source Port), DP (Destination Port)  
5. **Word2Vec Embedding**  
6. **MSFormer** (Multi-sequence Transformer Classifier)

## Dataset

Benign Traffic: BENIGN-ALL
Malicious Traffic: CTU-13 / Malware Traffic Analysis datasets
