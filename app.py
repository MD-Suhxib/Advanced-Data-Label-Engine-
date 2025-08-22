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
            'created_at': datetime.now().isoformat()
        }
        
        return jsonify(rules_storage[rule_id]), 201
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/rules', methods=['GET'])
def get_rules():
    return jsonify(list(rules_storage.values()))

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
    return jsonify({'message': 'Rule deleted successfully'})

@app.route('/api/rules/<rule_id>/toggle', methods=['POST'])
def toggle_rule(rule_id):
    if rule_id not in rules_storage:
        return jsonify({'error': 'Rule not found'}), 404
    
    rule = rules_storage[rule_id]
    rule['enabled'] = not rule['enabled']
    rule['updated_at'] = datetime.now().isoformat()
    
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
        
        # Apply rules
        for rule in active_rules:
            try:
                rule_conditions = rule_engine.parse_rule(rule['condition'])
                if rule_engine.evaluate_rule(rule_conditions, payload):
                    applied_labels.append(rule['label'])
            except Exception as e:
                print(f"Error evaluating rule {rule['id']}: {e}")
                continue
        
        # Store processed data
        processed_entry = {
            'id': str(uuid.uuid4()),
            'payload': payload,
            'labels': applied_labels,
            'timestamp': datetime.now().isoformat()
        }
        
        processed_data.append(processed_entry)
        
        # Update statistics cache
        update_statistics_cache()
        
        return jsonify({
            'id': processed_entry['id'],
            'labels': applied_labels,
            'timestamp': processed_entry['timestamp']
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/statistics', methods=['GET'])
def get_statistics():
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
    
    return jsonify({
        'total_processed': total_processed,
        'label_breakdown': label_stats,
        'timestamp': datetime.now().isoformat()
    })

def update_statistics_cache():
    """Update the statistics cache for efficient dashboard queries"""
    global statistics_cache
    
    total = len(processed_data)
    label_counts = {}
    
    for entry in processed_data:
        for label in entry['labels']:
            label_counts[label] = label_counts.get(label, 0) + 1
    
    statistics_cache = {
        'total': total,
        'labels': label_counts,
        'last_updated': datetime.now().isoformat()
    }

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'rules_count': len(rules_storage),
        'processed_count': len(processed_data)
    })

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
            'created_at': datetime.now().isoformat()
        }
    
    app.run(debug=True, port=5000)