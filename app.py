from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime, timedelta
import json
import re
from typing import List, Dict, Any
import uuid

app = Flask(__name__)
CORS(app)

# In-memory storage
rules_storage = {}
processed_data = []
statistics_cache = {}

class RuleEngine:
    def __init__(self):
        self.operators = {
            '=': lambda x, y: x == y,
            '!=': lambda x, y: x != y,
            '<': lambda x, y: float(x) < float(y),
            '>': lambda x, y: float(x) > float(y),
            '<=': lambda x, y: float(x) <= float(y),
            '>=': lambda x, y: float(x) >= float(y)
        }
    
    def parse_condition(self, condition: str) -> Dict:
        """Parse a single condition like 'Price > 5'"""
        condition = condition.strip()
        
        for op in ['>=', '<=', '!=', '=', '>', '<']:
            if op in condition:
                key, value = condition.split(op, 1)
                key = key.strip()
                value = value.strip().strip('"\'')
                
                # Try to convert to number if possible
                try:
                    value = float(value)
                    if value.is_integer():
                        value = int(value)
                except ValueError:
                    pass
                
                return {
                    'key': key,
                    'operator': op,
                    'value': value
                }
        
        raise ValueError(f"Invalid condition format: {condition}")
    
    def evaluate_condition(self, condition: Dict, data: Dict) -> bool:
        """Evaluate a single condition against data"""
        key = condition['key']
        operator = condition['operator']
        expected_value = condition['value']
        
        if key not in data:
            return False
        
        actual_value = data[key]
        
        try:
            return self.operators[operator](actual_value, expected_value)
        except (ValueError, TypeError):
            # For string comparisons
            return self.operators[operator](str(actual_value), str(expected_value))
    
    def parse_rule(self, rule_text: str) -> List[List[Dict]]:
        """Parse rule text into conditions
        Returns list of OR groups, each containing list of AND conditions
        """
        # Split by OR first
        or_groups = rule_text.split(' OR ')
        
        parsed_groups = []
        for or_group in or_groups:
            # Split by AND
            and_conditions = or_group.split(' AND ')
            parsed_conditions = []
            
            for condition in and_conditions:
                parsed_conditions.append(self.parse_condition(condition))
            
            parsed_groups.append(parsed_conditions)
        
        return parsed_groups
    
    def evaluate_rule(self, rule_conditions: List[List[Dict]], data: Dict) -> bool:
        """Evaluate rule against data
        Rule is satisfied if ANY OR group is satisfied
        OR group is satisfied if ALL AND conditions are satisfied
        """
        for or_group in rule_conditions:
            # Check if all AND conditions in this OR group are satisfied
            if all(self.evaluate_condition(condition, data) for condition in or_group):
                return True
        
        return False

rule_engine = RuleEngine()

# API Endpoints

@app.route('/api/rules', methods=['POST'])
def create_rule():
    try:
        data = request.get_json()
        
        # Validate required fields
        if not all(key in data for key in ['condition', 'label']):
            return jsonify({'error': 'Missing required fields: condition, label'}), 400
        
        # Validate rule syntax
        try:
            rule_engine.parse_rule(data['condition'])
        except Exception as e:
            return jsonify({'error': f'Invalid rule syntax: {str(e)}'}), 400
        
        rule_id = str(uuid.uuid4())
        rules_storage[rule_id] = {
            'id': rule_id,
            'condition': data['condition'],
            'label': data['label'],
            'enabled': data.get('enabled', True),
            'priority': data.get('priority', 1),
            'created_at': datetime.now().isoformat(),
            'usage_count': 0,  # Track how many times this rule has been applied
            'last_used': None
        }
        
        return jsonify(rules_storage[rule_id]), 201
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/rules', methods=['GET'])
def get_rules():
    # Sort rules by priority (descending) then by creation date
    sorted_rules = sorted(
        rules_storage.values(), 
        key=lambda x: (-x['priority'], x['created_at']),
        reverse=False
    )
    return jsonify(sorted_rules)

@app.route('/api/rules/<rule_id>', methods=['PUT'])
def update_rule(rule_id):
    if rule_id not in rules_storage:
        return jsonify({'error': 'Rule not found'}), 404
    
    try:
        data = request.get_json()
        
        # Validate rule syntax if condition is being updated
        if 'condition' in data:
            rule_engine.parse_rule(data['condition'])
        
        # Update rule
        rule = rules_storage[rule_id]
        rule.update(data)
        rule['updated_at'] = datetime.now().isoformat()
        
        return jsonify(rule)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/rules/<rule_id>', methods=['DELETE'])
def delete_rule(rule_id):
    if rule_id not in rules_storage:
        return jsonify({'error': 'Rule not found'}), 404
    
    del rules_storage[rule_id]
    update_statistics_cache()
    return jsonify({'message': 'Rule deleted successfully'})

@app.route('/api/rules/<rule_id>/toggle', methods=['POST'])
def toggle_rule(rule_id):
    if rule_id not in rules_storage:
        return jsonify({'error': 'Rule not found'}), 404
    
    rule = rules_storage[rule_id]
    rule['enabled'] = not rule['enabled']
    rule['updated_at'] = datetime.now().isoformat()
    
    update_statistics_cache()
    return jsonify(rule)

@app.route('/api/process', methods=['POST'])
def process_payload():
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({'error': 'Invalid JSON payload'}), 400
        
        # Get active rules sorted by priority
        active_rules = [rule for rule in rules_storage.values() if rule['enabled']]
        active_rules.sort(key=lambda x: x['priority'], reverse=True)
        
        applied_labels = []
        matched_rules = []
        
        # Apply rules
        for rule in active_rules:
            try:
                rule_conditions = rule_engine.parse_rule(rule['condition'])
                if rule_engine.evaluate_rule(rule_conditions, payload):
                    applied_labels.append(rule['label'])
                    matched_rules.append(rule['id'])
                    
                    # Update rule usage statistics
                    rule['usage_count'] = rule.get('usage_count', 0) + 1
                    rule['last_used'] = datetime.now().isoformat()
                    
            except Exception as e:
                print(f"Error evaluating rule {rule['id']}: {e}")
                continue
        
        # Store processed data with more details
        processed_entry = {
            'id': str(uuid.uuid4()),
            'payload': payload,
            'labels': applied_labels,
            'matched_rules': matched_rules,
            'timestamp': datetime.now().isoformat(),
            'processing_time_ms': 0  # Could be calculated if needed
        }
        
        processed_data.append(processed_entry)
        
        # Keep only last 1000 entries to prevent memory overflow
        if len(processed_data) > 1000:
            processed_data.pop(0)
        
        # Update statistics cache
        update_statistics_cache()
        
        return jsonify({
            'id': processed_entry['id'],
            'labels': applied_labels,
            'matched_rules_count': len(matched_rules),
            'timestamp': processed_entry['timestamp']
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/processed-data', methods=['GET'])
def get_processed_data():
    """Get processed data with optional filtering"""
    limit = request.args.get('limit', type=int, default=100)
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    label_filter = request.args.get('label')
    
    filtered_data = processed_data.copy()
    
    # Apply date filters
    if from_date:
        from_dt = datetime.fromisoformat(from_date)
        filtered_data = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) >= from_dt]
    
    if to_date:
        to_dt = datetime.fromisoformat(to_date)
        filtered_data = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) <= to_dt]
    
    # Apply label filter
    if label_filter:
        filtered_data = [d for d in filtered_data if label_filter in d['labels']]
    
    # Apply limit and return most recent first
    return jsonify(filtered_data[-limit:])

@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    """Enhanced statistics with more detailed breakdown"""
    label_filter = request.args.get('label')
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    
    # Filter data based on parameters
    filtered_data = processed_data
    
    if from_date:
        from_dt = datetime.fromisoformat(from_date)
        filtered_data = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) >= from_dt]
    
    if to_date:
        to_dt = datetime.fromisoformat(to_date)
        filtered_data = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) <= to_dt]
    
    if label_filter:
        filtered_data = [d for d in filtered_data if label_filter in d['labels']]
    
    # Calculate statistics
    total_processed = len(filtered_data)
    
    # Label statistics
    label_counts = {}
    for entry in filtered_data:
        for label in entry['labels']:
            label_counts[label] = label_counts.get(label, 0) + 1
    
    # Calculate percentages
    label_stats = []
    for label, count in label_counts.items():
        percentage = (count / total_processed * 100) if total_processed > 0 else 0
        label_stats.append({
            'label': label,
            'count': count,
            'percentage': round(percentage, 2)
        })
    
    # Sort by count descending
    label_stats.sort(key=lambda x: x['count'], reverse=True)
    
    # Processing rate calculations
    now = datetime.now()
    last_hour = now - timedelta(hours=1)
    last_24h = now - timedelta(hours=24)
    last_week = now - timedelta(days=7)
    
    recent_hour = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) > last_hour]
    recent_24h = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) > last_24h]
    recent_week = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) > last_week]
    
    # Rule effectiveness
    rule_effectiveness = {}
    for rule_id, rule in rules_storage.items():
        if rule['enabled']:
            rule_effectiveness[rule['label']] = {
                'usage_count': rule.get('usage_count', 0),
                'last_used': rule.get('last_used'),
                'condition': rule['condition']
            }

    return jsonify({
        'total_processed': total_processed,
        'label_breakdown': label_stats,
        'processing_rates': {
            'last_hour': len(recent_hour),
            'last_24h': len(recent_24h),
            'last_week': len(recent_week)
        },
        'success_rate': {
            'labeled_records': len([d for d in filtered_data if d['labels']]),
            'unlabeled_records': len([d for d in filtered_data if not d['labels']]),
            'percentage': round((len([d for d in filtered_data if d['labels']]) / total_processed * 100), 2) if total_processed > 0 else 0
        },
        'rule_effectiveness': rule_effectiveness,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/analytics/timeline', methods=['GET'])
def get_timeline_analytics():
    """Get timeline data for charting"""
    hours = request.args.get('hours', type=int, default=24)
    
    now = datetime.now()
    start_time = now - timedelta(hours=hours)
    
    # Filter data to requested time range
    timeline_data = [d for d in processed_data if datetime.fromisoformat(d['timestamp']) > start_time]
    
    # Group by hour
    hourly_counts = {}
    for i in range(hours):
        hour_start = now - timedelta(hours=i)
        hour_key = hour_start.strftime('%Y-%m-%d %H:00')
        hourly_counts[hour_key] = {
            'processed': 0,
            'labeled': 0,
            'labels': {}
        }
    
    for entry in timeline_data:
        entry_time = datetime.fromisoformat(entry['timestamp'])
        hour_key = entry_time.strftime('%Y-%m-%d %H:00')
        
        if hour_key in hourly_counts:
            hourly_counts[hour_key]['processed'] += 1
            if entry['labels']:
                hourly_counts[hour_key]['labeled'] += 1
                for label in entry['labels']:
                    hourly_counts[hour_key]['labels'][label] = hourly_counts[hour_key]['labels'].get(label, 0) + 1
    
    # Convert to list format for frontend
    timeline_list = []
    for hour_key in sorted(hourly_counts.keys()):
        data = hourly_counts[hour_key]
        timeline_list.append({
            'hour': hour_key,
            'processed': data['processed'],
            'labeled': data['labeled'],
            'labels': data['labels']
        })
    
    return jsonify(timeline_list)

@app.route('/api/rules/analytics', methods=['GET'])
def get_rule_analytics():
    """Get detailed rule performance analytics"""
    analytics = []
    
    for rule_id, rule in rules_storage.items():
        # Count how many times this rule matched in processed data
        matches = 0
        recent_matches = 0
        last_24h = datetime.now() - timedelta(hours=24)
        
        for entry in processed_data:
            if rule_id in entry.get('matched_rules', []):
                matches += 1
                if datetime.fromisoformat(entry['timestamp']) > last_24h:
                    recent_matches += 1
        
        analytics.append({
            'rule_id': rule_id,
            'condition': rule['condition'],
            'label': rule['label'],
            'priority': rule['priority'],
            'enabled': rule['enabled'],
            'total_matches': matches,
            'recent_matches_24h': recent_matches,
            'usage_count': rule.get('usage_count', 0),
            'last_used': rule.get('last_used'),
            'created_at': rule['created_at']
        })
    
    # Sort by total matches descending
    analytics.sort(key=lambda x: x['total_matches'], reverse=True)
    
    return jsonify(analytics)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Enhanced health check with system metrics"""
    # Calculate some basic metrics
    active_rules = len([r for r in rules_storage.values() if r['enabled']])
    total_rules = len(rules_storage)
    
    # Recent processing activity
    last_hour = datetime.now() - timedelta(hours=1)
    recent_activity = len([d for d in processed_data if datetime.fromisoformat(d['timestamp']) > last_hour])
    
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'metrics': {
            'rules_total': total_rules,
            'rules_active': active_rules,
            'rules_inactive': total_rules - active_rules,
            'processed_total': len(processed_data),
            'processed_last_hour': recent_activity,
            'memory_usage': {
                'rules_count': len(rules_storage),
                'processed_data_count': len(processed_data),
                'estimated_memory_mb': (len(str(rules_storage)) + len(str(processed_data))) / 1024 / 1024
            }
        }
    })

@app.route('/api/export/rules', methods=['GET'])
def export_rules():
    """Export rules configuration as JSON"""
    export_data = {
        'export_timestamp': datetime.now().isoformat(),
        'rules': list(rules_storage.values()),
        'statistics': {
            'total_rules': len(rules_storage),
            'active_rules': len([r for r in rules_storage.values() if r['enabled']])
        }
    }
    return jsonify(export_data)

@app.route('/api/import/rules', methods=['POST'])
def import_rules():
    """Import rules configuration from JSON"""
    try:
        data = request.get_json()
        imported_count = 0
        
        if 'rules' not in data:
            return jsonify({'error': 'No rules found in import data'}), 400
        
        for rule_data in data['rules']:
            # Validate rule structure
            if not all(key in rule_data for key in ['condition', 'label']):
                continue
            
            # Test rule syntax
            try:
                rule_engine.parse_rule(rule_data['condition'])
            except Exception:
                continue  # Skip invalid rules
            
            # Generate new ID for imported rule
            rule_id = str(uuid.uuid4())
            rules_storage[rule_id] = {
                'id': rule_id,
                'condition': rule_data['condition'],
                'label': rule_data['label'],
                'enabled': rule_data.get('enabled', True),
                'priority': rule_data.get('priority', 1),
                'created_at': datetime.now().isoformat(),
                'usage_count': 0,
                'last_used': None,
                'imported': True
            }
            imported_count += 1
        
        update_statistics_cache()
        
        return jsonify({
            'message': f'Successfully imported {imported_count} rules',
            'imported_count': imported_count,
            'total_rules': len(rules_storage)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def update_statistics_cache():
    """Update the statistics cache for efficient dashboard queries"""
    global statistics_cache
    
    total = len(processed_data)
    label_counts = {}
    
    for entry in processed_data:
        for label in entry['labels']:
            label_counts[label] = label_counts.get(label, 0) + 1
    
    # Calculate processing rates
    now = datetime.now()
    last_24h = now - timedelta(hours=24)
    recent_data = [d for d in processed_data if datetime.fromisoformat(d['timestamp']) > last_24h]
    
    statistics_cache = {
        'total_processed': total,
        'labels': label_counts,
        'processing_rate_24h': len(recent_data),
        'success_rate': (len([d for d in processed_data if d['labels']]) / total * 100) if total > 0 else 0,
        'last_updated': datetime.now().isoformat()
    }

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    # Add some sample rules for testing
    sample_rules = [
        {
            'condition': 'Product = "Chocolate" AND Price < 2',
            'label': 'Green',
            'priority': 1
        },
        {
            'condition': 'Product = "Chocolate" AND Price >= 2 AND Price < 5',
            'label': 'Yellow',
            'priority': 1
        },
        {
            'condition': 'MOQ < 100',
            'label': 'HighPriority',
            'priority': 2
        },
        {
            'condition': 'Price > 10',
            'label': 'Premium',
            'priority': 3
        }
    ]
    
    for rule_data in sample_rules:
        rule_id = str(uuid.uuid4())
        rules_storage[rule_id] = {
            'id': rule_id,
            'condition': rule_data['condition'],
            'label': rule_data['label'],
            'enabled': True,
            'priority': rule_data['priority'],
            'created_at': datetime.now().isoformat(),
            'usage_count': 0,
            'last_used': None
        }
    
    print(f"Started with {len(rules_storage)} sample rules")
    print("API Endpoints:")
    print("- POST /api/process - Process data payload")
    print("- GET /api/statistics - Get processing statistics") 
    print("- GET /api/processed-data - Get processed data history")
    print("- GET /api/rules - Get all rules")
    print("- GET /api/health - Health check")
    
    app.run(debug=True, port=5000, host='0.0.0.0')