class RateLimitRule:
    def __init__(self, requests_per_minute: int, requests_per_hour: int, burst_limit: int = None):
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.burst_limit = burst_limit or requests_per_minute * 3

class RuleEngine:
    def __init__(self):
        self.rules = {
            'telemetry': RateLimitRule(300, 7200, 600),
            'batch': RateLimitRule(10, 100, 20),
            'api_read': RateLimitRule(300, 3000, 500),
            'default': RateLimitRule(60, 600, 100)
        }
    
    def get_endpoint_category(self, path: str, method: str) -> str:
        if '/api/telemetry' in path and method == 'POST':
            return 'telemetry'
        elif '/api/batch' in path and method == 'POST':
            return 'batch'
        elif path.startswith('/api/') and method == 'GET':
            return 'api_read'
        return 'default'
    
    def get_rule(self, category: str) -> RateLimitRule:
        return self.rules.get(category, self.rules['default'])
    
    def get_rules_info(self):
        return {name: {
            "requests_per_minute": rule.requests_per_minute,
            "requests_per_hour": rule.requests_per_hour,
            "burst_limit": rule.burst_limit
        } for name, rule in self.rules.items()}
