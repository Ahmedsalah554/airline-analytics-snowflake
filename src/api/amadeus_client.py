import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# تحميل المتغيرات من ملف .env
load_dotenv()

class AmadeusAPIClient:
    """Amadeus API Client for Flight Search"""
    
    BASE_URL = "https://test.api.amadeus.com/v1"  # لاحظ استخدام رابط الـ Test
    AUTH_URL = "https://test.api.amadeus.com/v1/security/oauth2/token"
    
    def __init__(self):
        # التأكد من وجود البيانات في الـ Environment Variables
        self.client_id = os.getenv('AMADEUS_CLIENT_ID')
        self.client_secret = os.getenv('AMADEUS_CLIENT_SECRET')
        self.access_token = None
        
        if not self.client_id or not self.client_secret:
            raise ValueError("❌ Missing AMADEUS credentials. Check your .env file or Airflow Variables.")
        
        self._authenticate()
    
    def _authenticate(self):
        """الحصول على Access Token جديد"""
        try:
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            
            response = requests.post(self.AUTH_URL, data=payload, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            self.access_token = data["access_token"]
            print("✅ API Token obtained successfully")
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            raise
    
    def get_flight_offers(self, origin, destination, departure_date, adults=1):
        """جلب عروض الطيران بناءً على الوجهة والتاريخ"""
        if not self.access_token:
            self._authenticate()
        
        # رابط البحث عن الرحلات (نسخة v2)
        url = "https://test.api.amadeus.com/v2/shopping/flight-offers"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params = {
            "originLocationCode": origin,
            "destinationLocationCode": destination,
            "departureDate": departure_date,
            "adults": adults,
            "max": 10
        }
        
        try:
            print(f"📡 Fetching data for {origin} -> {destination} on {departure_date}...")
            response = requests.get(url, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            flights = data.get("data", [])
            print(f"🎯 Successfully found {len(flights)} flight offers")
            return flights
        except Exception as e:
            print(f"❌ API Request failed: {e}")
            return []

# تجربة الكود محلياً (لن يعمل داخل Airflow إلا عند مناداته)
if __name__ == "__main__":
    client = AmadeusAPIClient()
    res = client.get_flight_offers("DXB", "CAI", "2024-06-01")
    print(res)