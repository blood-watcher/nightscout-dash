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
        #timestamp { font-size:2rem; color:#666; }
        #timestamp.stale { color:#ff4444; }
        #deltas { font-size:1.5rem; color:#888; margin-top:30px; display:flex; gap:30px; justify-content:center; }
        #deltas .delta { display:flex; flex-direction:column; align-items:center; }
        #deltas .delta-label { font-size:1rem; color:#666; margin-bottom:5px; }
        #deltas .delta-value { font-weight:bold; }
        #deltas .delta-value.positive { color:#ff6666; }
        #deltas .delta-value.negative { color:#66ff66; }
        .sparkline { display:inline-block; margin-left:10px; vertical-align:middle; }
        #stats { margin-top:50px; font-size:1.2rem; width:600px; }
        .stat-row { display:flex; align-items:center; margin-bottom:15px; gap:15px; }
        .stat-label { width:80px; color:#888; text-align:right; }
        .progress-bar-container { flex:1; height:25px; background:#222; border-radius:5px; overflow:hidden; position:relative; }
        .progress-bar-fill { height:100%; transition:width 0.3s ease; }
        .progress-bar-fill.green { background:#66ff66; }
        .progress-bar-fill.orange { background:#ff9933; }
        .progress-bar-text { position:absolute; right:10px; top:50%; transform:translateY(-50%); color:#fff; font-weight:bold; font-size:1rem; }
        #error { font-size:2rem; color:#ff4444; text-align:center; padding:20px; display:none; }
        .loading { font-size:3rem; color:#666; }
        #clock { position:absolute; top:20px; left:20px; font-size:2rem; color:#666; display:flex; gap:20px; align-items:center; }
        #day-chart-container { width:90%; max-width:1200px; margin:20px auto 40px; }
        #day-chart { width:100%; height:150px; border:1px solid #333; }
    </style>
</head>
<body>
    <div id="clock">
        <span id="clock-time"></span>
        <span id="timestamp">Loading...</span>
    </div>
    <div id="day-chart-container">
        <svg id="day-chart" height="150"></svg>
    </div>
    <div id="glucose-value" class="loading">--</div>
    <div id="deltas">
        <div class="delta">
            <div class="delta-label">1 min</div>
            <div>
                <span class="delta-value" id="delta-1min">--</span>
                <svg class="sparkline" id="sparkline-1min" width="60" height="20"></svg>
            </div>
        </div>
        <div class="delta">
            <div class="delta-label">10 min</div>
            <div>
                <span class="delta-value" id="delta-10min">--</span>
                <svg class="sparkline" id="sparkline-10min" width="40" height="20"></svg>
            </div>
        </div>
        <div class="delta">
            <div class="delta-label">30 min</div>
            <div class="delta-value" id="delta-30min">--</div>
        </div>
        <div class="delta">
            <div class="delta-label">1 hour</div>
            <div>
                <span class="delta-value" id="delta-1hr">--</span>
                <svg class="sparkline" id="sparkline-1hr" width="60" height="20"></svg>
            </div>
        </div>
        <div class="delta">
            <div class="delta-label">3 hours</div>
            <div class="delta-value" id="delta-3hr">--</div>
        </div>
    </div>
    <div id="stats">
        <div class="stat-row">
            <div class="stat-label">&lt; 100</div>
            <div class="progress-bar-container">
                <div class="progress-bar-fill green" id="bar-100" style="width:0%"></div>
                <div class="progress-bar-text" id="text-100">0%</div>
            </div>
        </div>
        <div class="stat-row">
            <div class="stat-label">&lt; 180</div>
            <div class="progress-bar-container">
                <div class="progress-bar-fill orange" id="bar-180" style="width:0%"></div>
                <div class="progress-bar-text" id="text-180">0%</div>
            </div>
        </div>
    </div>
    <div id="error"></div>
    <script>
        var REFRESH_INTERVAL = 30000;
        
        function drawDayChart(data) {
            var svg = document.getElementById('day-chart');
            if (!svg) {
                console.log('Day chart: SVG element not found');
                return;
            }
            if (!data || data.length === 0) {
                console.log('Day chart: No data', {dataLength: data ? data.length : 0});
                return;
            }
            
            console.log('Drawing day chart with', data.length, 'points');
            
            // Clear previous content
            svg.innerHTML = '';
            
            var width = svg.clientWidth;
            if (width === 0) width = 1000;  // Fallback
            var height = parseInt(svg.getAttribute('height'));
            var padding = 30;
            
            try {
                // Filter to valid glucose values first
                var validData = [];
                for (var i = 0; i < data.length; i++) {
                    var val = data[i].value;
                    if (val >= 20 && val <= 500) {
                        validData.push({
                            time: data[i].time,
                            value: val
                        });
                    }
                }
                
                if (validData.length === 0) {
                    console.log('Day chart: No valid data points');
                    return;
                }
                
                console.log('Day chart: filtered to', validData.length, 'valid points from', data.length, 'total');
                
                // Calculate scales from VALID data only
                var values = validData.map(function(d) { return d.value; });
                var minVal = Math.min.apply(null, values);
                var maxVal = Math.max.apply(null, values);
                var range = maxVal - minVal;
                if (range === 0) range = 1; // Avoid division by zero
                
                console.log('Day chart range:', minVal, '-', maxVal);
                
                // X-axis: 0-1440 minutes (24 hours)
                var xScale = (width - 2 * padding) / 1440;
                
                // Build path from VALID data
                var points = [];
                for (var i = 0; i < validData.length; i++) {
                    var x = padding + validData[i].time * xScale;
                    var normalizedY = (validData[i].value - minVal) / range;
                    var y = height - padding - normalizedY * (height - 2 * padding);
                    points.push(x + ',' + y);
                }
                
                console.log('Day chart: rendering', points.length, 'points');
                
                // Draw path
                if (points.length > 0) {
                    var polyline = document.createElementNS('http://www.w3.org/2000/svg', 'polyline');
                    polyline.setAttribute('points', points.join(' '));
                    polyline.setAttribute('fill', 'none');
                    polyline.setAttribute('stroke', '#66aaff');
                    polyline.setAttribute('stroke-width', '2');
                    svg.appendChild(polyline);
                }
                
                // Draw grid
                var g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
                g.setAttribute('stroke', '#333');
                g.setAttribute('stroke-width', '0.5');
                
                // Horizontal grid (glucose values)
                var step = Math.ceil(range / 4);
                for (var v = Math.ceil(minVal / step) * step; v <= maxVal; v += step) {
                    var y = height - padding - ((v - minVal) / range) * (height - 2 * padding);
                    var line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                    line.setAttribute('x1', padding);
                    line.setAttribute('y1', y);
                    line.setAttribute('x2', width - padding);
                    line.setAttribute('y2', y);
                    g.appendChild(line);
                    
                    // Label
                    var text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                    text.setAttribute('x', padding - 5);
                    text.setAttribute('y', y + 4);
                    text.setAttribute('text-anchor', 'end');
                    text.setAttribute('fill', '#666');
                    text.setAttribute('font-size', '10');
                    text.textContent = v;
                    g.appendChild(text);
                }
                
                // Vertical grid (hours)
                for (var h = 0; h <= 24; h += 3) {
                    var x = padding + (h * 60) * xScale;
                    var line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                    line.setAttribute('x1', x);
                    line.setAttribute('y1', padding);
                    line.setAttribute('x2', x);
                    line.setAttribute('y2', height - padding);
                    g.appendChild(line);
                    
                    // Label
                    var text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                    text.setAttribute('x', x);
                    text.setAttribute('y', height - padding + 15);
                    text.setAttribute('text-anchor', 'middle');
                    text.setAttribute('fill', '#666');
                    text.setAttribute('font-size', '10');
                    text.textContent = h + ':00';
                    g.appendChild(text);
                }
                
                svg.appendChild(g);
            } catch (e) {
                console.error('Error drawing day chart:', e);
            }
        }
        
        function drawSparkline(svgId, dataPoints) {
            var svg = document.getElementById(svgId);
            if (!svg) return;
            
            // Clear previous content
            svg.innerHTML = '';
            
            // Filter out null values
            var validPoints = dataPoints.filter(function(p) { return p !== null; });
            if (validPoints.length === 0) return;
            
            var width = parseInt(svg.getAttribute('width'));
            var height = parseInt(svg.getAttribute('height'));
            var padding = 2;
            
            // Calculate scales
            var minVal = Math.min.apply(null, validPoints);
            var maxVal = Math.max.apply(null, validPoints);
            var range = maxVal - minVal || 1; // Avoid division by zero
            
            // Build path
            var points = [];
            var xStep = (width - 2 * padding) / (dataPoints.length - 1 || 1);
            
            for (var i = 0; i < dataPoints.length; i++) {
                if (dataPoints[i] !== null) {
                    var x = padding + i * xStep;
                    var normalizedY = (dataPoints[i] - minVal) / range;
                    var y = height - padding - normalizedY * (height - 2 * padding);
                    points.push(x + ',' + y);
                }
            }
            
            if (points.length > 0) {
                var polyline = document.createElementNS('http://www.w3.org/2000/svg', 'polyline');
                polyline.setAttribute('points', points.join(' '));
                polyline.setAttribute('fill', 'none');
                polyline.setAttribute('stroke', '#888');
                polyline.setAttribute('stroke-width', '1.5');
                svg.appendChild(polyline);
            }
        }
        
        function updateStats(stats) {
            if (!stats) return;
            
            // Update < 100
            var bar100 = document.getElementById('bar-100');
            var text100 = document.getElementById('text-100');
            if (bar100 && text100) {
                bar100.style.width = stats.percent_below_100 + '%';
                text100.textContent = stats.percent_below_100 + '%';
            }
            
            // Update < 180
            var bar180 = document.getElementById('bar-180');
            var text180 = document.getElementById('text-180');
            if (bar180 && text180) {
                bar180.style.width = stats.percent_below_180 + '%';
                text180.textContent = stats.percent_below_180 + '%';
            }
        }
        
        function formatMinutesAgo(timestamp) {
            var now = new Date();
            var then = new Date(timestamp);
            var diffMs = now - then;
            var diffMins = Math.floor(diffMs / 60000);
            
            // Update timestamp element with stale class if >5 minutes
            var timestampElem = document.getElementById('timestamp');
            if (diffMins > 5) {
                timestampElem.classList.add('stale');
            } else {
                timestampElem.classList.remove('stale');
            }
            
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
                            
                            // Update deltas
                            var deltaMapping = {
                                '1min': 'delta-1min',
                                '10min': 'delta-10min',
                                '30min': 'delta-30min',
                                '1hr': 'delta-1hr',
                                '3hr': 'delta-3hr'
                            };
                            
                            for (var key in deltaMapping) {
                                var elemId = deltaMapping[key];
                                var elem = document.getElementById(elemId);
                                var deltaValue = data.deltas[key];
                                
                                if (deltaValue !== null && deltaValue !== undefined) {
                                    var sign = deltaValue > 0 ? '+' : '';
                                    elem.textContent = sign + deltaValue;
                                    
                                    // Color code: red for increase, green for decrease
                                    elem.classList.remove('positive', 'negative');
                                    if (deltaValue > 0) {
                                        elem.classList.add('positive');
                                    } else if (deltaValue < 0) {
                                        elem.classList.add('negative');
                                    }
                                } else {
                                    elem.textContent = '--';
                                    elem.classList.remove('positive', 'negative');
                                }
                            }
                            
                            // Draw sparklines
                            if (data.sparklines) {
                                if (data.sparklines['1min']) {
                                    drawSparkline('sparkline-1min', data.sparklines['1min']);
                                }
                                if (data.sparklines['10min']) {
                                    drawSparkline('sparkline-10min', data.sparklines['10min']);
                                }
                                if (data.sparklines['1hr']) {
                                    drawSparkline('sparkline-1hr', data.sparklines['1hr']);
                                }
                            }
                            
                            // Draw day chart
                            if (data.day_chart) {
                                drawDayChart(data.day_chart);
                            }
                            
                            // Update stats
                            if (data.stats) {
                                updateStats(data.stats);
                            }
                            
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
        
        // Update clock every 30 seconds
        function updateClock() {
            var now = new Date();
            var hours = now.getHours().toString().padStart(2, '0');
            var minutes = now.getMinutes().toString().padStart(2, '0');
            document.getElementById('clock-time').textContent = hours + ':' + minutes;
        }
        updateClock();
        setInterval(updateClock, 30000);
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
    
    # Cache for glucose entries (stored as list sorted by timestamp descending)
    app.config['GLUCOSE_CACHE'] = []
    app.config['CACHE_INITIALIZED'] = False
    
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
            
            THREE_HOURS_MS = 3 * 60 * 60 * 1000
            SIX_HOURS_MS = 6 * 60 * 60 * 1000
            
            # First load: fetch until midnight (00:00 today)
            if not app.config['CACHE_INITIALIZED']:
                if not production:
                    print("Initial load: fetching entries back to midnight")
                
                all_entries = []
                batch_size = 100
                
                # Get the current time first
                params = {"count": 1}
                response = requests.get(url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                latest = response.json()
                
                if not latest:
                    return jsonify({"error": "No data available"}), 404
                
                current_time = latest[0].get('date')
                
                # Calculate midnight today (00:00 local time)
                # Note: current_time is in milliseconds since epoch
                import datetime
                current_dt = datetime.datetime.fromtimestamp(current_time / 1000)
                midnight_today = current_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                midnight_ms = int(midnight_today.timestamp() * 1000)
                all_entries.extend(latest)
                
                oldest_fetched = current_time
                
                # Keep fetching until we've gone back to midnight
                while oldest_fetched > midnight_ms:
                    params = {
                        "count": batch_size,
                        "find[date][$lt]": oldest_fetched
                    }
                    
                    response = requests.get(url, headers=headers, params=params, timeout=10)
                    response.raise_for_status()
                    batch = response.json()
                    
                    if not batch:
                        # No more data available, but keep trying to go back
                        # Simulate going back in time
                        oldest_fetched = oldest_fetched - (10 * 60 * 1000)  # Jump back 10 minutes
                        if oldest_fetched < midnight_ms:
                            break
                        continue
                    
                    all_entries.extend(batch)
                    oldest_fetched = batch[-1].get('date')
                    
                    if not production:
                        print(f"Fetched {len(all_entries)} entries so far, oldest: {oldest_fetched}")
                    
                    # Safety: don't fetch more than 500 entries total
                    if len(all_entries) >= 500:
                        if not production:
                            print(f"Reached 500 entry safety limit")
                        break
                
                if not production:
                    print(f"Initial fetch complete: {len(all_entries)} entries")
                
                app.config['GLUCOSE_CACHE'] = all_entries
                app.config['CACHE_INITIALIZED'] = True
                
            else:
                # Subsequent loads: fetch only 1 entry
                if not production:
                    print("Incremental update: fetching 1 entry")
                params = {"count": 1}
                response = requests.get(url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    return jsonify({"error": "No data available"}), 404
                
                # Merge new data into cache
                cache = app.config['GLUCOSE_CACHE']
                for entry in data:
                    entry_date = entry.get('date')
                    # Only add if not already in cache
                    if not any(e.get('date') == entry_date for e in cache):
                        cache.insert(0, entry)
                
                # Trim cache: keep data since yesterday (to handle midnight rollover)
                now = data[0].get('date')
                yesterday_ms = now - (25 * 60 * 60 * 1000)  # 25 hours ago
                app.config['GLUCOSE_CACHE'] = [
                    e for e in cache if e.get('date') > yesterday_ms
                ]
            
            if not production:
                print("Response status:", response.status_code)
            
            # Use cache for calculations
            cache = app.config['GLUCOSE_CACHE']
            
            if not cache:
                return jsonify({"error": "No data available"}), 404
            
            current_entry = cache[0]
            current_value = current_entry.get('sgv')
            current_time = current_entry.get('date')
            
            # Calculate deltas for different time periods (in milliseconds)
            time_periods = {
                '1min': 1 * 60 * 1000,
                '10min': 10 * 60 * 1000,
                '30min': 30 * 60 * 1000,
                '1hr': 60 * 60 * 1000,
                '3hr': 3 * 60 * 60 * 1000
            }
            
            deltas = {}
            for period_name, period_ms in time_periods.items():
                target_time = current_time - period_ms
                # Find the closest entry to the target time
                closest_entry = None
                min_diff = float('inf')
                
                for entry in cache:
                    entry_time = entry.get('date')
                    time_diff = abs(entry_time - target_time)
                    if time_diff < min_diff:
                        min_diff = time_diff
                        closest_entry = entry
                
                if closest_entry and closest_entry.get('sgv'):
                    delta = current_value - closest_entry.get('sgv')
                    deltas[period_name] = delta
                else:
                    deltas[period_name] = None
            
            # Generate sparkline data
            sparklines = {}
            
            # 1-min sparkline: 10 points, last 10 minutes
            sparkline_points = []
            for i in range(10):
                target_time = current_time - (i * 60 * 1000)  # i minutes ago
                closest = None
                min_diff = float('inf')
                for entry in cache:
                    diff = abs(entry.get('date') - target_time)
                    if diff < min_diff:
                        min_diff = diff
                        closest = entry
                if closest and closest.get('sgv'):
                    sparkline_points.append(closest.get('sgv'))
                else:
                    sparkline_points.append(None)
            sparklines['1min'] = list(reversed(sparkline_points))  # Oldest to newest
            
            # 10-min sparkline: 6 points, last hour (10-min intervals)
            sparkline_points = []
            for i in range(6):
                target_time = current_time - (i * 10 * 60 * 1000)  # i*10 minutes ago
                closest = None
                min_diff = float('inf')
                for entry in cache:
                    diff = abs(entry.get('date') - target_time)
                    if diff < min_diff:
                        min_diff = diff
                        closest = entry
                if closest and closest.get('sgv'):
                    sparkline_points.append(closest.get('sgv'))
                else:
                    sparkline_points.append(None)
            sparklines['10min'] = list(reversed(sparkline_points))
            
            # 1-hour sparkline: 6 points, last 6 hours (1-hour intervals)
            sparkline_points = []
            for i in range(6):
                target_time = current_time - (i * 60 * 60 * 1000)  # i hours ago
                closest = None
                min_diff = float('inf')
                for entry in cache:
                    diff = abs(entry.get('date') - target_time)
                    if diff < min_diff:
                        min_diff = diff
                        closest = entry
                if closest and closest.get('sgv'):
                    sparkline_points.append(closest.get('sgv'))
                else:
                    sparkline_points.append(None)
            sparklines['1hr'] = list(reversed(sparkline_points))
            
            # Full day chart: all data since midnight
            import datetime
            current_dt = datetime.datetime.fromtimestamp(current_time / 1000)
            midnight_today = current_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            midnight_ms = int(midnight_today.timestamp() * 1000)
            
            day_chart_data = []
            for entry in reversed(cache):  # Oldest to newest
                entry_time = entry.get('date')
                if entry_time >= midnight_ms and entry.get('sgv'):
                    # Calculate minutes since midnight
                    minutes_since_midnight = (entry_time - midnight_ms) / (60 * 1000)
                    day_chart_data.append({
                        'time': minutes_since_midnight,
                        'value': entry.get('sgv')
                    })
            
            # Calculate percentages for < 100 and < 180 from midnight
            entries_since_midnight = [e for e in cache if e.get('date') >= midnight_ms and e.get('sgv')]
            total_entries = len(entries_since_midnight)
            
            if total_entries > 0:
                below_100_count = sum(1 for e in entries_since_midnight if e.get('sgv') < 100)
                below_180_count = sum(1 for e in entries_since_midnight if e.get('sgv') < 180)
                
                percent_below_100 = round((below_100_count / total_entries) * 100)
                percent_below_180 = round((below_180_count / total_entries) * 100)
            else:
                percent_below_100 = 0
                percent_below_180 = 0
            
            if not production:
                print(f"Cache size: {len(cache)} entries")
                print(f"< 100: {percent_below_100}%, < 180: {percent_below_180}%")
            
            return jsonify({
                "value": current_value or '--',
                "timestamp": current_entry.get('dateString'),
                "units": current_entry.get('units', 'mg/dL'),
                "direction": current_entry.get('direction', ''),
                "deltas": deltas,
                "sparklines": sparklines,
                "day_chart": day_chart_data,
                "stats": {
                    "percent_below_100": percent_below_100,
                    "percent_below_180": percent_below_180
                }
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