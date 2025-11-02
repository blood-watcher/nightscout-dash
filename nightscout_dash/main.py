#!/usr/bin/env python3
"""
Flask app for Nightscout Dashboard
"""

from flask import Flask, jsonify, render_template_string
import requests
import os
import argparse
import json
from pathlib import Path
from urllib.parse import urlparse

# Configuration defaults
DEFAULT_NIGHTSCOUT_PORT = "80"
DEFAULT_USER_TOKEN = os.environ.get("NIGHTSCOUT_USER_TOKEN", "")
DEFAULT_BIND_PORT = 5000

def parse_bind_address(bind_address):
    """Parse bind address in host or host:port format
    
    Returns: (host, port)
    """
    if ':' in bind_address:
        host, port = bind_address.split(':', 1)
        return host, int(port)
    else:
        return bind_address, DEFAULT_BIND_PORT

def parse_nightscout_url(url_or_host):
    """Parse nightscout server URL in various formats
    
    Supports:
    - http://host:port/
    - https://host:port/
    - host:port
    - host
    
    Returns: (scheme, host, port)
    """
    if url_or_host.startswith('http://') or url_or_host.startswith('https://'):
        parsed = urlparse(url_or_host)
        scheme = parsed.scheme
        host = parsed.hostname
        port = parsed.port or (443 if scheme == 'https' else 80)
    elif ':' in url_or_host:
        host, port = url_or_host.split(':', 1)
        scheme = 'http'
        port = int(port)
    else:
        host = url_or_host
        scheme = 'http'
        port = 80
    return scheme, host, port

def load_credentials(credential_file):
    """Load credentials from a JSON file
    
    Expected format:
    {
        "user_token": "your-api-secret-or-token-here"
    }
    """
    try:
        with open(credential_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise ValueError(f"Credential file not found: {credential_file}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in credential file: {e}")

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nightscout Dashboard</title>
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body {
            font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;
            background:#000; color:#fff;
            display:flex; flex-direction:column; justify-content:center; align-items:center;
            height:100vh; overflow:hidden;
        }
        #glucose-value { font-size:15rem; font-weight:bold; line-height:1; margin-bottom:20px; }
        #units { font-size:3rem; color:#888; margin-bottom:40px; }
        #timestamp { font-size:2rem; color:#666; }
        #error { font-size:2rem; color:#ff4444; text-align:center; padding:20px; display:none; }
        .loading { font-size:3rem; color:#666; }
    </style>
</head>
<body>
    <div id="glucose-value" class="loading">--</div>
    <div id="units">mg/dL</div>
    <div id="timestamp">Loading...</div>
    <div id="error"></div>
    <script>
        var REFRESH_INTERVAL = 30000;

        function formatMinutesAgo(timestamp) {
            var now = new Date();
            var then = new Date(timestamp);
            var diffMs = now - then;
            var diffMins = Math.floor(diffMs / 60000);

            if (diffMins === 0) return 'just now';
            else if (diffMins === 1) return '1 minute ago';
            else if (diffMins < 60) return diffMins + ' minutes ago';
            else {
                var hours = Math.floor(diffMins / 60);
                var mins = diffMins % 60;
                if (hours === 1) return mins === 0 ? '1 hour ago' : '1 hour ' + mins + ' minutes ago';
                else return mins === 0 ? hours + ' hours ago' : hours + ' hours ' + mins + ' minutes ago';
            }
        }

        function fetchGlucose() {
            var xhr = new XMLHttpRequest();
            xhr.open('GET', '/api/glucose', true);
            xhr.onreadystatechange = function() {
                if (xhr.readyState === 4) {
                    var errorElem = document.getElementById('error');
                    var glucoseElem = document.getElementById('glucose-value');
                    var timestampElem = document.getElementById('timestamp');

                    if (xhr.status === 200) {
                        try {
                            var data = JSON.parse(xhr.responseText);
                            if (data.error) throw new Error(data.error);

                            glucoseElem.textContent = data.value;
                            glucoseElem.classList.remove('loading');
                            timestampElem.textContent = formatMinutesAgo(data.timestamp);
                            errorElem.style.display = 'none';
                        } catch (e) {
                            errorElem.textContent = 'Error: ' + e.message;
                            errorElem.style.display = 'block';
                            timestampElem.textContent = 'Failed to load';
                        }
                    } else {
                        errorElem.textContent = 'Error fetching data (status ' + xhr.status + ')';
                        errorElem.style.display = 'block';
                        timestampElem.textContent = 'Failed to load';
                    }
                }
            };
            xhr.send();
        }

        fetchGlucose();
        setInterval(fetchGlucose, REFRESH_INTERVAL);
    </script>
</body>
</html>
"""

def create_app(nightscout_scheme, nightscout_host, nightscout_port, user_token, production=False):
    """Create and configure the Flask app"""
    app = Flask(__name__)

    # Store config in app
    app.config['NIGHTSCOUT_SCHEME'] = nightscout_scheme
    app.config['NIGHTSCOUT_HOST'] = nightscout_host
    app.config['NIGHTSCOUT_PORT'] = nightscout_port
    app.config['USER_TOKEN'] = user_token
    app.config['PRODUCTION'] = production

    @app.route('/')
    def index():
        return render_template_string(HTML_TEMPLATE)

    @app.route('/api/glucose')
    def get_glucose():
        try:
            url = "{}://{}:{}/api/v1/entries.json".format(
                app.config['NIGHTSCOUT_SCHEME'],
                app.config['NIGHTSCOUT_HOST'],
                app.config['NIGHTSCOUT_PORT']
            )
            headers = {"API-SECRET": app.config['USER_TOKEN']}
            params = {"count": 1}

            if not production:
                print("Fetching from:", url)
                print("Using user token:", app.config['USER_TOKEN'][:10] + "..." if app.config['USER_TOKEN'] else "No user token set!")

            response = requests.get(url, headers=headers, params=params, timeout=10)

            if not production:
                print("Response status:", response.status_code)
                print("Response headers:", response.headers)
                print("Response text:", response.text[:200])

            response.raise_for_status()
            data = response.json()
            if not data:
                return jsonify({"error": "No data available"}), 404

            entry = data[0]
            return jsonify({
                "value": entry.get('sgv', '--'),
                "timestamp": entry.get('dateString'),
                "units": entry.get('units', 'mg/dL'),
                "direction": entry.get('direction', '')
            })

        except requests.RequestException as e:
            print("Request error:", e)
            return jsonify({"error": str(e)}), 500
        except Exception as e:
            print("Unexpected error:", e)
            import traceback
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    return app

def main():
    parser = argparse.ArgumentParser(
        description="Nightscout Dashboard - Web display for Nightscout glucose data"
    )

    parser.add_argument('bind_address', help='Bind address (e.g., 0.0.0.0 or 0.0.0.0:8080)')
    parser.add_argument('nightscout_server', help='Nightscout server (e.g., http://host:port/, host:port, or host)')
    parser.add_argument('--credential-file', type=str, help='Path to JSON file containing user_token')
    parser.add_argument('--production', action='store_true', help='Run with production WSGI server (waitress)')

    args = parser.parse_args()

    bind_host, bind_port = parse_bind_address(args.bind_address)
    scheme, nightscout_host, nightscout_port = parse_nightscout_url(args.nightscout_server)

    if args.credential_file:
        try:
            creds = load_credentials(args.credential_file)
            user_token = creds.get('user_token', '')
            if not user_token:
                parser.error("credential file must contain 'user_token' field")
        except ValueError as e:
            parser.error(str(e))
    else:
        user_token = DEFAULT_USER_TOKEN

    app = create_app(scheme, nightscout_host, nightscout_port, user_token, production=args.production)

    print("Starting Nightscout Dashboard on http://{}:{}".format(bind_host, bind_port))
    print("Connecting to Nightscout at {}://{}:{}".format(scheme, nightscout_host, nightscout_port))

    if args.production:
        try:
            from waitress import serve
            print("Running in PRODUCTION mode with Waitress WSGI server")
            serve(app, host=bind_host, port=bind_port)
        except ImportError:
            print("ERROR: waitress is not installed. Install with: pip install waitress")
            import sys
            sys.exit(1)
    else:
        print("Running in DEVELOPMENT mode (use --production for production)")
        print("Debug mode: ENABLED")
        app.run(host=bind_host, port=bind_port, debug=True)

if __name__ == '__main__':
    main()
