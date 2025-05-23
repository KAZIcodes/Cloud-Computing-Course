import sys

# Mapper Function
# Reads input CSV, extracts client_id and feature_value, and outputs key-value pairs
# Format: client_id \t feature_value,1

def mapper():
    for line in sys.stdin:
        line = line.strip()
        parts = line.split(",")
        
        # Ensure proper format (skip header)
        if len(parts) == 3 and parts[0] != "client_id":
            client_id, feature_value, _ = parts
            try:
                print(f"{client_id}\t{feature_value},1")
            except ValueError:
                continue  # Ignore invalid data

# Reducer Function
# Aggregates sum of feature_value and counts occurrences per client_id

def reducer():
    current_client = None
    total_sum = 0.0
    total_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        client_id, values = line.split("\t")
        feature_value, count = values.split(",")
        
        try:
            feature_value = float(feature_value)
            count = int(count)
        except ValueError:
            continue  # Ignore invalid data
        
        if client_id == current_client:
            total_sum += feature_value
            total_count += count
        else:
            if current_client is not None:
                print(f"{current_client}\t{total_sum},{total_count}")
            
            current_client = client_id
            total_sum = feature_value
            total_count = count
    
    if current_client is not None:
        print(f"{current_client}\t{total_sum},{total_count}")

# Run the appropriate function based on the script name
if __name__ == "__main__":
    if "mapper.py" in sys.argv[0]:
        mapper()
    else:
        reducer()
