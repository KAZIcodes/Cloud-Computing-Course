#!/usr/bin/env python3
import sys

def reducer():
    current_client = None
    total_sum = 0
    total_count = 0

    for line in sys.stdin:
        line = line.strip()

        # Skip empty lines
        if not line:
            continue

        # Debugging: Print the problematic line before processing
        if "\t" not in line:
            print(f"Skipping malformed line: {line}", file=sys.stderr)
            continue

        try:
            client_id, values = line.split("\t")  # Ensure there's a tab separator
            feature_value, count = map(float, values.split(","))  # Convert values
        except ValueError as e:
            print(f"Error parsing line: {line} - {e}", file=sys.stderr)
            continue  # Skip the problematic line

        # Aggregate results
        if current_client == client_id:
            total_sum += feature_value
            total_count += count
        else:
            if current_client is not None:
                print(f"{current_client}\t{total_sum},{total_count}")
            current_client = client_id
            total_sum = feature_value
            total_count = count

    # Print the last client
    if current_client is not None:
        print(f"{current_client}\t{total_sum},{total_count}")

if __name__ == "__main__":
    reducer()
