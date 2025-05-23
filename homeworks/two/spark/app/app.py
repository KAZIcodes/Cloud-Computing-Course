from pyspark import SparkContext
import random

# Initialize Spark
sc = SparkContext("local", "FederatedAveraging")

# Read data from file
file_path = "aggregated_results.txt"
rdd = sc.textFile(file_path)

# Parse the data into (client_id, (sum_of_feature_values, count))
def parse_line(line):
    parts = line.split("\t")
    client_id = parts[0]
    sum_value, count = map(float, parts[1].split(","))
    return (client_id, (sum_value, count))

parsed_rdd = rdd.map(parse_line)

# Compute initial global average
total_sum = parsed_rdd.map(lambda x: x[1][0]).sum()
total_count = parsed_rdd.map(lambda x: x[1][1]).sum()
global_average = total_sum / total_count

print(f"Initial Global Average: {global_average}")

# Simulate federated averaging rounds
num_rounds = 3
for round_num in range(num_rounds):
    local_averages = parsed_rdd.map(lambda x: (x[0], x[1][0] / x[1][1])).collect()
    print(f"Round {round_num + 1} Local Averages: {local_averages}")

    # Simulate local updates
    updated_averages = [(client, avg + random.uniform(-1, 1)) for client, avg in local_averages]
    print(f"Round {round_num + 1} Updated Averages: {updated_averages}")

    # Recalculate global average
    global_average = sum([avg for client, avg in updated_averages]) / len(updated_averages)
    print(f"Round {round_num + 1} Global Average: {global_average}")

sc.stop()
