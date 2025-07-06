import hashlib
from fastapi import Request

class ClientIdentifier:
    def get_client_identifier(self, request: Request) -> str:
        forwarded = request.headers.get('x-forwarded-for', '').split(',')[0].strip()
        real_ip = request.headers.get('x-real-ip', '')
        client_ip = forwarded or real_ip or request.client.host or 'unknown'
        
        user_agent = request.headers.get('user-agent', '')[:50]
        identifier_parts = [client_ip, hashlib.md5(user_agent.encode()).hexdigest()[:8]]
        
        return '_'.join(identifier_parts)
    
    def get_client_ip(self, request: Request) -> str:
        forwarded = request.headers.get('x-forwarded-for', '').split(',')[0].strip()
        real_ip = request.headers.get('x-real-ip', '')
        return forwarded or real_ip or request.client.host or 'unknown'
