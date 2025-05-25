import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urljoin
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

base_url = "https://www.ncaa.com"
topic = "ncaa-player-stats"

max_retries = 10
retry_delay = 7  # seconds

# Set up Kafka producer
producer = None
for attempt in range(max_retries):
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:29092",
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
        )
        print("Connected to Kafka broker.")
        break
    except NoBrokersAvailable:
        print(f"Kafka broker not available, retrying {attempt + 1}/{max_retries} in {retry_delay} seconds...")
        time.sleep(retry_delay)

if producer is None:
    print("Failed to connect to Kafka broker after retries. Exiting.")
    exit(1)

# Stat category sources
stat_dropdown_items = [
    ('Stolen Bases Per Game', '/stats/baseball/d1/current/individual/206'),
    ('Batting Average', '/stats/baseball/d1/current/individual/200'),
    ('Earned Run Average', '/stats/baseball/d1/current/individual/205'),
    ('Slugging Percentage', '/stats/baseball/d1/current/individual/321'),
    ('WHIP', '/stats/baseball/d1/current/individual/596'),
    ('Home Runs Per Game', '/stats/baseball/d1/current/individual/201'),
    ('On Base Percentage', '/stats/baseball/d1/current/individual/504'),
    ('Complete Games', '/stats/baseball/d1/current/individual/587'),
    ('Hits Allowed Per Nine Innings', '/stats/baseball/d1/current/individual/505'),
    ('Strikeouts Per Nine Innings', '/stats/baseball/d1/current/individual/207'),
    ('Walks Allowed Per Nine Innings', '/stats/baseball/d1/current/individual/508')

]

team_stat_dropdown_items = [
    ('WL Pct', '/stats/baseball/d1/current/team/319'),
    ('Batting Average', '/stats/baseball/d1/current/team/210'),
    ('Earned Run Average', '/stats/baseball/d1/current/team/211'),
    ('Slugging Percentage', '/stats/baseball/d1/current/team/327'),
    ('WHIP', '/stats/baseball/d1/current/team/597'),
    ('Home Runs Per Game', '/stats/baseball/d1/current/team/323'),
    ('Fielding Percentage', '/stats/baseball/d1/current/team/212'),
    ('On Base Percentage', '/stats/baseball/d1/current/team/589'),
    ('Strikeouts Per Nine Innings', '/stats/baseball/d1/current/team/425'),
    ('Hits Allowed Per Nine Innings', '/stats/baseball/d1/current/team/506'),
    ('Walks Allowed Per Nine Innings', '/stats/baseball/d1/current/team/509')


]

def fetch_table(stat_name, url_suffix, is_team=False, max_pages=10, sleep_sec=1):
    visited_pages = set()
    page_number = 1
    stat_type = "team" if is_team else "player"

    while page_number <= max_pages:
        page_url = f"{url_suffix}/p{page_number}" if page_number > 1 else url_suffix
        full_url = urljoin(base_url, page_url)

        if full_url in visited_pages:
            print(f"Already visited {full_url}, stopping pagination.")
            break
        visited_pages.add(full_url)

        print(f"Fetching {stat_name} - Page {page_number}: {full_url}")
        try:
            response = requests.get(full_url, timeout=10)
            if response.status_code != 200:
                print(f"Failed to fetch {full_url}, status code: {response.status_code}")
                break
        except Exception as e:
            print(f"Exception fetching {full_url}: {e}")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')
        if not table:
            print(f"No table found on {full_url}")
            break

        headers = [th.text.strip() for th in table.find_all('th')]
        rows = table.find_all('tr')[1:]
        if not rows:
            print(f"No data rows on {full_url}, stopping.")
            break

        for tr in rows:
            cells = [td.text.strip() for td in tr.find_all('td')]
            if cells and len(cells) == len(headers):
                record = dict(zip(headers, cells))
                payload = {
                    "stat_category": stat_name,
                    "stat_type": stat_type,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                    f"{stat_type}_stats": record
                }
                try:
                    producer.send(topic, value=payload)
                    entity_key = "Team" if is_team else "Name"
                    print(f"Sent: {record.get(entity_key, 'Unknown')} - {stat_name}")
                except Exception as e:
                    print(f"Failed to send message for {record.get(entity_key, 'Unknown')}: {e}")

        try:
            producer.flush()
        except Exception as e:
            print(f"Failed to flush producer: {e}")

        page_number += 1
        time.sleep(sleep_sec)

    print(f"Finished fetching {stat_name} after {page_number - 1} pages.")

if __name__ == "__main__":
    try:
        while True:
            for stat_name, url_suffix in stat_dropdown_items:
                fetch_table(stat_name, url_suffix, is_team=False)

            for stat_name, url_suffix in team_stat_dropdown_items:
                fetch_table(stat_name, url_suffix, is_team=True)

            print("Sleeping 10 minutes before next scrape cycle...")
            time.sleep(600)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
        producer.close()
