from get_ids import get_ids
from get_id_results import get_id_results
from get_rankings_once import send_rankings_query
import time

if __name__ == "__main__":
    start_time = time.time()
    get_ids()
    current_time = time.time()
    print(current_time - start_time)
    start_time = current_time

    get_id_results()
    current_time = time.time()
    print(current_time - start_time)
    start_time = current_time

    send_rankings_query()
    current_time = time.time()
    print(current_time - start_time)
    start_time = current_time