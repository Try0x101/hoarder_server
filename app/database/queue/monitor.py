class ConnectionQueueMonitor:
    def __init__(self):
        self.pending_requests = 0
        self.total_requests = 0
        self.timeouts = 0
        self.queue_full_rejections = 0
        self.max_pending_seen = 0
        
    def request_started(self):
        self.pending_requests += 1
        self.total_requests += 1
        self.max_pending_seen = max(self.max_pending_seen, self.pending_requests)
        
    def request_completed(self):
        self.pending_requests = max(0, self.pending_requests - 1)
        
    def request_timeout(self):
        self.timeouts += 1
        self.request_completed()
        
    def queue_full(self):
        self.queue_full_rejections += 1
        
    def get_stats(self):
        return {
            'pending_requests': self.pending_requests,
            'total_requests': self.total_requests,
            'timeouts': self.timeouts,
            'queue_full_rejections': self.queue_full_rejections,
            'max_pending_seen': self.max_pending_seen,
            'queue_pressure': min(1.0, self.pending_requests / 10.0)
        }

queue_monitor = ConnectionQueueMonitor()