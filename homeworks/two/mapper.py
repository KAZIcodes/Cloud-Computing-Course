#!/usr/bin/env python3
import sys

# Mapper function to process input
# It reads input CSV, extracts client_id and feature_value, and outputs key-value pairs

def mapper():
    for line in sys.stdin:
        line = line.strip()
        parts = line.split(",")
        
        # Skip header and ensure proper format
        if len(parts) == 3 and parts[0] != "client_id":
            client_id, feature_value, _ = parts
            try:
                print(f"{client_id}\t{feature_value},1")
            except ValueError:
                continue  # Ignore invalid data

if __name__ == "__main__":
    mapper()
