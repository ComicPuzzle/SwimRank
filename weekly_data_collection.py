from get_ncaa_rankings import get_ncaa_rankings
from get_meet_results import get_meet_results
from update_rankings import send_season_ranking_query
from datetime import datetime
import time

if __name__ == "__main__":

    start_time = time.time()
    get_meet_results()
    current_time = time.time()
    print(current_time - start_time)
    start_time = current_time

    month = datetime.now().month
    year = datetime.now().year
    if month  >= 9:
        season_start = str(year) + "-09-01"
        season_end = str(year + 1) + "-09-01"
        season = f"{year}-{year + 1}"
    else:
        season_start = str(year - 1) + "-09-01"
        season_end = str(year) + "-09-01"
        season = f"{year - 1}-{year}"

    print('season ranking')
    send_season_ranking_query(season_start, season_end, season)
    current_time = time.time()
    print(current_time - start_time)
    start_time = current_time

    get_ncaa_rankings()
    current_time = time.time()
    print(current_time - start_time)
    start_time = current_time