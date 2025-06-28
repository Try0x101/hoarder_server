#!/usr/bin/env python3
import asyncio
import aiohttp
import time
from datetime import datetime

async def test_rate_limit():
    """Test the rate limiting functionality"""
    print(f"[{datetime.now()}] Testing rate limiting functionality")
    
    # Test endpoint
    base_url = "http://localhost:5000"
    endpoints = [
        "/data/history?device_id=bbb5&limit=1000",  # Large query
        "/data/latest",                            # Regular endpoint
    ]
    
    async with aiohttp.ClientSession() as session:
        for endpoint in endpoints:
            print(f"\nTesting endpoint: {endpoint}")
            
            # Make multiple requests in quick succession
            for i in range(5):
                start_time = time.time()
                try:
                    async with session.get(f"{base_url}{endpoint}") as response:
                        status = response.status
                        if status == 429:
                            rate_limit_data = await response.json()
                            print(f"Request {i+1}: Rate limited (429) - {rate_limit_data.get('detail')}")
                        else:
                            print(f"Request {i+1}: Status {status}")
                except Exception as e:
                    print(f"Request {i+1}: Error - {str(e)}")
                
                # Small delay to see the rate limiting in action
                await asyncio.sleep(0.2)
            
            # Wait before testing next endpoint
            print("Waiting for rate limit window to reset...")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(test_rate_limit())
