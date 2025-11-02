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

# Configuration defaults
DEFAULT_NIGHTSCOUT_PORT = "80"
DEFAULT_USER_TOKEN = os.environ.get("NIGHTSCOUT_USER_TOKEN", "")

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
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            background: #000;
            color: #fff;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            height: 100vh;
            overflow: hidden;
        }
        
        #glucose-value {
            font-size: 15rem;
            font-weight: bold;
            line-height: 1;
            margin-bottom: 20px;
        }
        
        #units {
            font-size: 3rem;
            color: #888;
            margin-bottom: 40px;
        }
        
        #timestamp {
            font-size: 2rem;
            color: #666;
        }
        
        #error {
            font-size: 2rem;
            color: #ff4444;
            text-align: center;
            padding: 20px;
        }
        
        .loading {
            font-size: 3rem;
            color: #666;
        }
    </style>
</head>
<body>
    <div id="glucose-value" class="loading">--</div>
    <div id="units">mg/dL</div>
    <div id="timestamp">Loading...</div>
    <div id="error" style="display: none;"></div>

    <script>
        const REFRESH_INTERVAL = 30000; // 30 seconds

        function formatMinutesAgo(timestamp) {
            const now = new Date();
            const then = new Date(timestamp);
            const diffMs = now - then;
            const diffMins = Math.floor(diffMs / 60000);
            
            if (diffMins === 0) {
                return 'just now';
            } else if (diffMins === 1) {
                return '1 minute ago';
            } else if (diffMins < 60) {
                return `${diffMins} minutes ago`;
            } else {
                const hours = Math.floor(diffMins / 60);
                const mins = diffMins % 60;
                if (hours === 1) {
                    return mins === 0 ? '1 hour ago' : `1 hour ${mins} minutes ago`;
                } else {
                    return mins === 0 ? `${hours} hours ago` : `${hours} hours ${mins} minutes ago`;
                }
            }
        }

        async function fetchGlucose() {
            try {
                const response = await fetch('/api/glucose');
                
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                
                const data = await response.json();
                
                if (data.error) {
                    throw new Error(data.error);
                }
                
                document.getElementById('glucose-value').textContent = data.value;
                document.getElementById('glucose-value').classList.remove('loading');
                document.getElementById('timestamp').textContent = formatMinutesAgo(data.timestamp);
                document.getElementById('error').style.display = 'none';
            } catch (error) {
                console.error('Error fetching glucose data:', error);
                document.getElementById('error').textContent = `Error: ${error.message}`;
                document.getElementById('error').style.display = 'block';
                document.getElementById('timestamp').textContent = 'Failed to load';
            }
        }

        // Initial fetch
        fetchGlucose();
        
        // Refresh every 30 seconds
        setInterval(fetchGlucose, REFRESH_INTERVAL);
    </script>
</body>
</html>
"""

def create_app(nightscout_host, nightscout_port, user_token):
    """Create and configure the Flask app"""
    app = Flask(__name__)
    
    # Store config in app
    app.config['NIGHTSCOUT_HOST'] = nightscout_host
    app.config['NIGHTSCOUT_PORT'] = nightscout_port
    app.config['USER_TOKEN'] = user_token
    
    @app.route('/')
    def index():
        """Serve the main display page"""
        return render_template_string(HTML_TEMPLATE)
    
    @app.route('/api/glucose')
    def get_glucose():
        """API endpoint to fetch latest glucose value"""
        try:
            url = f"http://{app.config['NIGHTSCOUT_HOST']}:{app.config['NIGHTSCOUT_PORT']}/api/v1/entries.json"
            headers = {"API-SECRET": app.config['USER_TOKEN']}
            params = {"count": 1}
            
            print(f"Fetching from: {url}")
            print(f"Using user token: {app.config['USER_TOKEN'][:10]}..." if app.config['USER_TOKEN'] else "No user token set!")
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            print(f"Response status: {response.status_code}")
            print(f"Response headers: {response.headers}")
            print(f"Response text: {response.text[:200]}")
            
            response.raise_for_status()
            
            data = response.json()
            
            if not data or len(data) == 0:
                return jsonify({"error": "No data available"}), 404
            
            entry = data[0]
            
            return jsonify({
                "value": entry.get('sgv', '--'),
                "timestamp": entry.get('dateString'),
                "units": entry.get('units', 'mg/dL'),
                "direction": entry.get('direction', '')
            })
        
        except requests.RequestException as e:
            print(f"Request error: {e}")
            return jsonify({"error": str(e)}), 500
        except Exception as e:
            print(f"Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500
    
    return app

def main():
    """Main entry point for the nightscout-dash command"""
    parser = argparse.ArgumentParser(
        description="Nightscout Dashboard - Web display for Nightscout glucose data"
    )
    
    # Mandatory arguments
    parser.add_argument('bind_ip', 
                       help='IP address to bind to (e.g., 0.0.0.0 or 127.0.0.1)')
    parser.add_argument('nightscout_server',
                       help='Nightscout server hostname (e.g., glucose.example.com)')
    
    # Optional arguments
    parser.add_argument('--port', type=int, default=5000,
                       help='Port to bind to (default: 5000)')
    parser.add_argument('--nightscout-port', default=None,
                       help=f'Nightscout port (default: {DEFAULT_NIGHTSCOUT_PORT})')
    parser.add_argument('--credential-file', type=str,
                       help='Path to JSON file containing user_token')
    parser.add_argument('--production', action='store_true',
                       help='Run with production WSGI server (waitress)')
    
    args = parser.parse_args()
    
    nightscout_server = args.nightscout_server
    nightscout_port = args.nightscout_port or DEFAULT_NIGHTSCOUT_PORT
    
    # Load user_token from credential file if provided, otherwise use env var
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
    
    app = create_app(nightscout_server, nightscout_port, user_token)
    
    print(f"Starting Nightscout Dashboard on http://{args.bind_ip}:{args.port}")
    print(f"Connecting to Nightscout at {nightscout_server}:{nightscout_port}")
    
    if args.production:
        # Use waitress for production
        try:
            from waitress import serve
            print("Running in PRODUCTION mode with Waitress WSGI server")
            serve(app, host=args.bind_ip, port=args.port)
        except ImportError:
            print("ERROR: waitress is not installed. Install with: pip install waitress")
            print("Or install nightscout-dash with: pip install --upgrade nightscout-dash")
            import sys
            sys.exit(1)
    else:
        # Use Flask development server with debug always enabled
        print("Running in DEVELOPMENT mode (use --production for production)")
        print("Debug mode: ENABLED")
        app.run(host=args.bind_ip, port=args.port, debug=True)

if __name__ == '__main__':
    main()