#!/usr/bin/env python3
"""
Script to fetch historical Nightscout data, calculate minute-by-minute running averages
for each day, and store them in the 'running_averages' table within 'running_averages.sqlite'.

This script runs a full historical backfill in a single execution until it is caught up 
to yesterday's complete data.
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

    def mark_day_as_checked(self, date_str):
        """Inserts a single placeholder entry (minute 0, avg_sgv 0) to mark an empty day as processed."""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        try:
            # INSERT OR IGNORE avoids errors if the script is manually run on the same empty day
            c.execute("""
                INSERT OR IGNORE INTO running_averages (date_str, minute_of_day, avg_sgv)
                VALUES (?, 0, 0)
            """, (date_str,))
            conn.commit()
            print(f"Archiver: Successfully marked empty day {date_str} as checked.")
        except sqlite3.Error as e:
            print(f"SQLite error during placeholder insert: {e}")
            conn.rollback()
        finally:
            conn.close()

    def insert_minute_averages(self, date_str, entries):
        """Calculates minute averages for the day and inserts them."""
        
        minute_data = defaultdict(lambda: {'sum': 0, 'count': 0})
        
        for entry in entries:
            timestamp_ms = entry.get('date')
            sgv = entry.get('sgv')
            
            if sgv is None or timestamp_ms is None:
                continue
            
            entry_dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000)
            minute_of_day = entry_dt.hour * 60 + entry_dt.minute
            
            minute_data[minute_of_day]['sum'] += sgv
            minute_data[minute_of_day]['count'] += 1

        if not minute_data:
            self.mark_day_as_checked(date_str)
            return 0

        data_to_insert = []
        for minute, data in minute_data.items():
            avg_sgv = round(data['sum'] / data['count'])
            data_to_insert.append((date_str, minute, avg_sgv))

        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        try:
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
    
    # 2. Sequential Catch-up Logic in a loop
    while True:
        now_dt = datetime.datetime.now()
        # The last day guaranteed to be complete is yesterday.
        date_of_last_full_day = (now_dt - datetime.timedelta(days=1)).date()
        
        latest_date_str = archiver.get_latest_date()
        
        if latest_date_str:
            latest_dt = datetime.datetime.strptime(latest_date_str, '%Y-%m-%d').date()
            next_date_to_fetch = latest_dt + datetime.timedelta(days=1)
        else:
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
                
                # Insert data or mark the day as checked if no entries are returned
                if day_entries:
                    archiver.insert_minute_averages(date_str, day_entries)
                else:
                    archiver.mark_day_as_checked(date_str)
            
            except requests.RequestException as e:
                # If there is a network error, stop the loop and exit the script
                print(f"Archiver: FATAL ERROR fetching data for {date_str}: {e}. Stopping backfill.")
                break
                
            except Exception as e:
                print(f"Archiver: An unexpected error occurred: {e}. Stopping backfill.")
                break
                
        else:
            # Exit loop when next_date_to_fetch is not yet a full past day (i.e., today or tomorrow)
            print(f"Archiver: History is current up to {date_of_last_full_day}. Finished backfill.")
            break 

if __name__ == '__main__':
    main()