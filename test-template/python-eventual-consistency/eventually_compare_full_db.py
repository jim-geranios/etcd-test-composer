from antithesis.assertions import reachable, unreachable, always
import etcd3
import time
import sys

# Built to match entrypoint.py
# It is a static list of the three etcd nodes in the cluster
NODES = ["etcd0", "etcd1", "etcd2"]
PORT = 2379

# Connects to one specific etcd node
def connect_to_node(host):
    try:
        client = etcd3.client(host=host, port=PORT)
        print(f"Connected to {host}")
        return client
    except Exception as e:
        print(f"Connection failed for {host}: {e}")
        unreachable("node_unavailable", {"host": host, "error": str(e)}) # Added unreachable as if this part is reached, we cannot connect to the node, and the eventual consistency is invalidated
        sys.exit(1)

# Reads every key/value combination of one etcd node into an array
def read_full_database(client):
    try:
        data = {}

        for value, metadata in client.get_all():
            key = metadata.key.decode("utf-8")
            val = value.decode("utf-8")
            data[key] = val

        print(f"Read {len(data)} entries from node")
        return data

    # Once again, hard stop if the reading fails
    except Exception as e:
        print(f"Failed to read database: {e}")
        unreachable("node_unreadable", {"error": str(e)})
        sys.exit(1)

# Convert a node's database contents into an easy-to-compare format
def normalise_dataset(data):
    keys = sorted(data.keys())
    values = [data[k] for k in keys]
    return keys, values

# Compares one node's dataset against the baseline node
def compare_datasets(base_keys, base_values, keys, values):

    # 1. Count check - The number of key/value pairs in both nodes
    if len(base_keys) != len(keys):
        return False, "entry_count_mismatch"

    # 2. Key check - Key X of node Y compared to Key X of node Z
    if base_keys != keys:
        return False, "key_set_mismatch"

    # 3. Value check - Value X of node Y compared to Value X of node Z
    if base_values != values:
        return False, "dataset_mismatch"

    return True, None


def main():

    print("Eventual comparing starts")

    # Antithesis assertion recording this eventually-validation command was reached
    reachable("Starting eventual cluster consistency validation", None)

    # Sleep to wait for recovery - this does not match the health check for faster results on execution
    print("Sleeping for recovery...")
    time.sleep(5)
    print("Recovery wait finished")

    # Node 0 is used as the reference dataset for the rest of the cluster
    base_client = connect_to_node(NODES[0])
    base_data = read_full_database(base_client)

    print(f"Baseline data: {base_data}")

    # Convert baseline data into a sorted comparison structure
    base_keys, base_values = normalise_dataset(base_data)

    all_match = True
    failure_type = None

    # Compare every remaining node against the baseline node
    for node in NODES[1:]:

        print(f"Comparing node: {node}")

        client = connect_to_node(node)
        data = read_full_database(client)

        print(f"{node} data: {data}")

        keys, values = normalise_dataset(data)

        match, failure = compare_datasets(base_keys, base_values, keys, values)

        print(f"Match result for {node}: {match}, failure: {failure}")

        # Fail on the first mismatch found
        if not match:
            all_match = False
            failure_type = failure
            break

    # Final assertion
    details = {"failure_type": failure_type}

    print(f"Final result: all_match={all_match}, failure_type={failure_type}")

    always(
        all_match,
        "All nodes expose identical database state",
        details
    )

    print("Script finished")

if __name__ == "__main__":
    main()