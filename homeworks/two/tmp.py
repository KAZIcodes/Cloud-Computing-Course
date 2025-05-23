import csv
from collections import defaultdict

# Read data.csv as input
def load_data(file_path):
    data = []
    with open(file_path, mode='r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header
        for row in reader:
            data.append(row)
    return data

# Mapper function
def mapper(data):
    # Map: (client_id, feature_value) -> (key, (value, 1))
    mapped_data = []
    for row in data:
        client_id, feature_value, _ = row
        feature_value = float(feature_value)
        mapped_data.append((client_id, feature_value, 1))
    return mapped_data

# Reducer function
def reducer(mapped_data):
    # Reduce: (key, [(value, count), ...]) -> (key, (sum(value), sum(count)))
    reduced_data = defaultdict(lambda: [0, 0])  # Default to [sum, count]
    
    for client_id, feature_value, count in mapped_data:
        reduced_data[client_id][0] += feature_value  # Sum of feature_value
        reduced_data[client_id][1] += count  # Count of occurrences
    
    # Prepare reduced output as (client_id, sum_value, count)
    result = []
    for client_id, (total_value, total_count) in reduced_data.items():
        result.append([client_id, total_value, total_count])
    
    return result

# Save the result to out-temp.csv
def save_output(output_data, output_path):
    with open(output_path, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['client_id', 'sum_value', 'count'])  # Header
        writer.writerows(output_data)

# Main function to execute MapReduce
def main():
    input_path = 'data.csv'  # Input file
    output_path = 'out-temp.csv'  # Output file

    # Step 1: Load data from CSV
    data = load_data(input_path)

    # Step 2: Apply Map function
    mapped_data = mapper(data)

    # Step 3: Apply Reduce function
    reduced_data = reducer(mapped_data)

    # Step 4: Save the output
    save_output(reduced_data, output_path)
    print(f"Output saved to {output_path}")

if __name__ == "__main__":
    main()
