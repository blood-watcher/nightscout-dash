#!/usr/bin/env python3
"""
Script to periodically fetch historical Nightscout data and calculate the minute-by-minute
running averages for each day, storing them in the 'running_averages' table within 
'running_averages.sqlite'. Designed to be run chronically by Circus.
"""
import requests
import json
import argparse
import os
import datetime
import sqlite3
from pathlib import Path
from urllib.parse import urlparse
from collections import defaultdict

# --- Configuration Defaults ---
DEFAULT_DB_FILE = 'running_averages.sqlite'
# Default to checking 14 days back if the database is empty
DEFAULT_INITIAL_BACKFILL_DAYS = 14

# --- Helper Functions ---

def parse_nightscout_url(url_or_host):
    """Parse nightscout server URL in various formats. Returns: (scheme, host, port)"""
    if url_or_host.startswith('http://') or url_or_host.startswith('https://'):
        parsed = urlparse(url_or_host)
        scheme = parsed.scheme
        host = parsed.hostname
        port = parsed.port or (443 if scheme == 'https' else 80)
    elif ':' in url_or_host:
        scheme = 'http' # default
        host, port = url_or_host.split(':', 1)
        port = int(port)
    else:
        scheme = 'http' # default
        host = url_or_host
        port = 80
    return scheme, host, port

def load_credentials(credential_file):
    """Loads a Nightscout user token from a JSON file."""
    if not Path(credential_file).exists():
        raise ValueError(f"Credential file not found: {credential_file}")
    with open(credential_file, 'r') as f:
        return json.load(f)

# --- SQLite Implementation ---

class SQLiteArchiver:
    def __init__(self, db_file=DEFAULT_DB_FILE):
        self.db_file = db_file
        self._initialize_db()

    def _initialize_db(self):
        """Creates the running_averages table with the minute_of_day index."""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        # Table: running_averages (Key is the tuple: date_str, minute_of_day)
        c.execute("""
            CREATE TABLE IF NOT EXISTS running_averages (
                date_str TEXT,
                minute_of_day INTEGER,
                avg_sgv INTEGER,
                PRIMARY KEY (date_str, minute_of_day)
            )
        """)
        
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_minute_of_day ON running_averages (minute_of_day)
        """)
        
        conn.commit()
        conn.close()

    def get_latest_date(self):
        """Returns the most recent date string ('YYYY-MM-DD') in the database."""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT MAX(date_str) FROM running_averages")
        result = c.fetchone()
        conn.close()
        return result[0] if result and result[0] else None

    def insert_minute_averages(self, date_str, entries):
        """Calculates minute averages for the day and inserts them."""
        
        minute_data = defaultdict(lambda: {'sum': 0, 'count': 0})
        
        for entry in entries:
            timestamp_ms = entry.get('date')
            sgv = entry.get('sgv')
            
            if sgv is None or timestamp_ms is None:
                continue
            
            entry_dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000)
            minute_of_day = entry_dt.hour * 60 + entry_dt.minute # 0 to 1439
            
            minute_data[minute_of_day]['sum'] += sgv
            minute_data[minute_of_day]['count'] += 1

        if not minute_data:
            print(f"Archiver: No valid SGV data to average for {date_str}.")
            return 0

        data_to_insert = []
        for minute, data in minute_data.items():
            avg_sgv = round(data['sum'] / data['count'])
            data_to_insert.append((date_str, minute, avg_sgv))

        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        try:
            # INSERT OR REPLACE ensures idempotency (no duplicates, handles re-runs)
            c.executemany("""
                INSERT OR REPLACE INTO running_averages (date_str, minute_of_day, avg_sgv)
                VALUES (?, ?, ?)
            """, data_to_insert)
            
            conn.commit()
            print(f"Archiver: Successfully stored {len(data_to_insert)} minute averages for {date_str}.")
            return len(data_to_insert)

        except sqlite3.Error as e:
            print(f"SQLite error during insert: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

# --- Main Script Logic ---

def main():
    parser = argparse.ArgumentParser(description='Nightscout Dash History Updater (SQLite)')
    parser.add_argument('nightscout_server', help='Nightscout URL (e.g., https://myserver.herokuapp.com)')
    parser.add_argument('--credential-file', help='Path to a JSON file containing the Nightscout user_token')
    parser.add_argument('--db-file', default=DEFAULT_DB_FILE, help='Name of the SQLite database file')
    # NEW ARGUMENT
    parser.add_argument('--initial-days-back', type=int, default=DEFAULT_INITIAL_BACKFILL_DAYS, 
                        help='Number of days back to start checking if history database is empty.')
    
    args = parser.parse_args()
    
    # 1. Configuration Setup
    scheme, nightscout_host, nightscout_port = parse_nightscout_url(args.nightscout_server)
    user_token = os.environ.get("NIGHTSCOUT_USER_TOKEN", "")
    
    if args.credential_file:
        try:
            creds = load_credentials(args.credential_file)
            user_token = creds.get('user_token', user_token)
        except ValueError as e:
            print(f"Error loading credentials: {e}")

    if not user_token:
        parser.error("Nightscout user token is required.")

    headers = {'api-secret': user_token}
    url = f"{scheme}://{nightscout_host}:{nightscout_port}/api/v1/entries.json"
    archiver = SQLiteArchiver(args.db_file)
    
    # 2. Sequential Catch-up Logic
    now_dt = datetime.datetime.now()
    # The last day guaranteed to be complete is yesterday.
    date_of_last_full_day = (now_dt - datetime.timedelta(days=1)).date()
    
    # Check if we already have data
    latest_date_str = archiver.get_latest_date()
    
    if latest_date_str:
        # If data exists, start checking the day after the latest recorded date
        latest_dt = datetime.datetime.strptime(latest_date_str, '%Y-%m-%d').date()
        next_date_to_fetch = latest_dt + datetime.timedelta(days=1)
    else:
        # --- INITIAL START DATE LOGIC ---
        # Database is empty. Start checking from the specified number of days back.
        print(f"Archiver: History DB is empty. Starting backfill check {args.initial_days_back} days ago.")
        start_dt = now_dt - datetime.timedelta(days=args.initial_days_back)
        next_date_to_fetch = start_dt.date()
        
    
    if next_date_to_fetch <= date_of_last_full_day:
        
        date_str = next_date_to_fetch.strftime('%Y-%m-%d')
        
        # Calculate timestamps for the full day to fetch
        midnight_fetch_day = datetime.datetime.combine(next_date_to_fetch, datetime.time())
        midnight_next_day = midnight_fetch_day + datetime.timedelta(days=1)
        
        params = {
            "find[date][$lt]": int(midnight_next_day.timestamp() * 1000), 
            "find[date][$gte]": int(midnight_fetch_day.timestamp() * 1000),
            "count": 300 
        }
        
        print(f"Archiver: Attempting to fetch and archive missing minute averages for: {date_str}...")
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            day_entries = response.json()
            
            # This is the single day insertion point.
            if day_entries:
                archiver.insert_minute_averages(date_str, day_entries)
            else:
                print(f"Archiver: No entries found for {date_str}.")
                
        except requests.RequestException as e:
            print(f"Archiver: ERROR fetching data for {date_str}: {e}. Will retry on next run.")
            
        except Exception as e:
            print(f"Archiver: An unexpected error occurred: {e}")

    else:
        print(f"Archiver: History is current up to {date_of_last_full_day}. Nothing to do.")

if __name__ == '__main__':
    main()
