import hazelcast
import threading
import time

def create_client():
    client = hazelcast.HazelcastClient(cluster_name="dev")
    return client

def distributed_map_example(client):
    my_map = client.get_map("distributed-map").blocking()
    for i in range(1000):
        my_map.put(i, f"Value-{i}")
    print("Distributed map initialized with 1000 values.")

def distributed_map_without_locks(client):
    my_map = client.get_map("counter-map").blocking()
    my_map.put_if_absent("key", 0)
    for _ in range(10_000):
        value = my_map.get("key")
        my_map.put("key", value + 1)
    print("Final value without locks:", my_map.get("key"))

def distributed_map_with_pessimistic_locking(client):
    my_map = client.get_map("counter-map").blocking()
    my_map.put_if_absent("key", 0)
    start = time.time()
    for _ in range(10_000):
        my_map.lock("key")
        try:
            value = my_map.get("key")
            my_map.put("key", value + 1)
        finally:
            my_map.unlock("key")
    end = time.time()
    time_taken = end - start
    print(f"Time taken {time_taken:2f}")
    print("Final value with pessimistic locking:", my_map.get("key"))

def distributed_map_with_optimistic_locking(client):
    my_map = client.get_map("counter-map").blocking()
    my_map.put_if_absent("key", 0)
    start = time.time()
    for _ in range(10_000):
        while True:
            old_value = my_map.get("key")
            if my_map.replace_if_same("key", old_value, old_value + 1):
                break
    end = time.time()
    time_taken = end - start
    print(f"Time taken {time_taken:2f}")
    print("Final value with optimistic locking:", my_map.get("key"))

def bounded_queue_example(client):
    queue = client.get_queue("bounded-queue").blocking()
    def producer():
        for i in range(1, 101):
            queue.put(i)
            print("Produced:", i)
            time.sleep(0.1)
    
    def consumer(name):
        while True:
            item = queue.take()
            print(f"{name} consumed:", item)
    
    producer_thread = threading.Thread(target=producer)
    consumer1_thread = threading.Thread(target=consumer, args=("Consumer 1",))
    consumer2_thread = threading.Thread(target=consumer, args=("Consumer 2",))
    
    producer_thread.start()
    consumer1_thread.start()
    consumer2_thread.start()
    
    producer_thread.join()

def main():
    client = create_client()
    distributed_map_example(client)
    distributed_map_without_locks(client)
    distributed_map_with_pessimistic_locking(client)
    distributed_map_with_optimistic_locking(client)
    bounded_queue_example(client)
    client.shutdown()

if __name__ == "__main__":
    main()
