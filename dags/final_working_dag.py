from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import json
import random
import time
import tempfile
import glob
import shutil
import logging
import requests
from requests.auth import HTTPBasicAuth

# ========================================
# INITIALIZATION AND SETUP
# ========================================

# Ensure the project root is in the system path for module discovery
# This allows Airflow to find any custom modules we might create later
sys.path.insert(0, '/opt/airflow')

# Configure logging for better visibility
logger = logging.getLogger(__name__)

# ========================================
# CONFIGURATION SECTION
# ========================================

# Snowflake Configuration
SNOWFLAKE_ACCOUNT = "YOUR_ACC"
SNOWFLAKE_USER = "YOUR_USER_NAME"
SNOWFLAKE_PASSWORD = "YOUR_PASSWORD"
SNOWFLAKE_WAREHOUSE = "YOUR_WAREHOUSE_NAME"
SNOWFLAKE_DATABASE = "YOUR_DATABASE_NAME"
SNOWFLAKE_SCHEMA = "YOUR_SCHEMA_NAME"

# OpenSky Network API Configuration
OPENSKY_USERNAME = "YOUR_E-MAIL"
OPENSKY_PASSWORD = os.getenv('YOUR_PASS', '')  # Add this to .env file
OPENSKY_CLIENT_ID = "YOUR_EMAIL-api-client"
OPENSKY_BASE_URL = "https://opensky-network.org/api"

# Project Targets
TARGET_RECORDS = 100000
MAX_API_CALLS_PER_DAY = 1000  # OpenSky rate limit (approximately)
BATCH_SIZE = 5000

# ========================================
# ROUTE DEFINITIONS
# ========================================

# Expanded routes for synthetic data generation
EXPANDED_ROUTES = [
    # North America - Europe
    ('JFK', 'LAX'), ('LAX', 'JFK'), ('JFK', 'ORD'), ('ORD', 'JFK'),
    ('LHR', 'DXB'), ('DXB', 'LHR'), ('LHR', 'JFK'), ('JFK', 'LHR'),
    ('CDG', 'JFK'), ('JFK', 'CDG'), ('FRA', 'JFK'), ('JFK', 'FRA'),
    ('AMS', 'CDG'), ('CDG', 'AMS'), ('MAD', 'BCN'), ('BCN', 'MAD'),
    
    # Middle East - Asia
    ('CAI', 'DXB'), ('DXB', 'CAI'), ('CAI', 'JED'), ('JED', 'CAI'),
    ('DXB', 'BKK'), ('BKK', 'DXB'), ('DXB', 'SYD'), ('SYD', 'DXB'),
    ('SIN', 'HND'), ('HND', 'SIN'), ('SIN', 'SYD'), ('SYD', 'SIN'),
    ('HKG', 'TPE'), ('TPE', 'HKG'), ('ICN', 'HND'), ('HND', 'ICN'),
    
    # Europe - Europe (Short haul)
    ('LHR', 'CDG'), ('CDG', 'LHR'), ('LHR', 'AMS'), ('AMS', 'LHR'),
    ('FRA', 'MUC'), ('MUC', 'FRA'), ('FCO', 'MAD'), ('MAD', 'FCO'),
    ('BCN', 'AMS'), ('AMS', 'BCN'), ('IST', 'LHR'), ('LHR', 'IST'),
    ('ZRH', 'LHR'), ('LHR', 'ZRH'), ('VIE', 'IST'), ('IST', 'VIE'),
    
    # North America - North America
    ('YYZ', 'LHR'), ('LHR', 'YYZ'), ('YVR', 'LAX'), ('LAX', 'YVR'),
    ('MEX', 'LAX'), ('LAX', 'MEX'), ('BOG', 'MIA'), ('MIA', 'BOG'),
    
    # Asia - Australia
    ('MEL', 'SYD'), ('SYD', 'MEL'), ('BNE', 'SYD'), ('SYD', 'BNE'),
    ('AKL', 'SYD'), ('SYD', 'AKL'), ('PER', 'SYD'), ('SYD', 'PER'),
]

# ========================================
# SYNTHETIC DATA POOLS
# ========================================

# Real airline codes (IATA airline designators)
AIRLINES_POOL = [
    'EK', 'AA', 'MS', 'BA', 'LH', 'QR', 'DL', 'EY', 'AF', 'TK',
    'SQ', 'CX', 'UA', 'IB', 'KL', 'AZ', 'EK', 'QR', 'MS',
]

# Aircraft types commonly used in commercial aviation
AIRCRAFT_POOL = [
    'B738', 'A320', 'B77W', 'A388', 'B789', 'A333', 'E190', 
    'A321', 'B737', 'A350', 'B772', 'A332',
]

# Fare classes for realistic pricing variations
FARE_CLASSES = [
    'Economy', 'Economy', 'Economy', 'Economy Plus',
    'Premium Economy', 'Business', 'Business', 'First',
]

# ========================================
# UTILITY FUNCTIONS
# ========================================

def setup_directories():
    """
    Create necessary directories for checkpoint files and archives.
    
    Returns:
        tuple: (checkpoint_dir, archive_dir) paths
    """
    checkpoint_dir = '/tmp/flight_checkpoints'
    archive_dir = f'{checkpoint_dir}/loaded'
    
    os.makedirs(checkpoint_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)
    
    logger.info(f"📁 Checkpoint directory: {checkpoint_dir}")
    logger.info(f"📁 Archive directory: {archive_dir}")
    
    return checkpoint_dir, archive_dir


def validate_configuration():
    """
    Validate that all required configuration variables are set.
    """
    required_vars = [
        ('SNOWFLAKE_ACCOUNT', SNOWFLAKE_ACCOUNT),
        ('SNOWFLAKE_USER', SNOWFLAKE_USER),
        ('SNOWFLAKE_PASSWORD', SNOWFLAKE_PASSWORD),
        ('OPENSKY_USERNAME', OPENSKY_USERNAME),
    ]
    
    missing = []
    for var_name, var_value in required_vars:
        if not var_value:
            missing.append(var_name)
    
    if missing:
        error_msg = f"❌ Missing required configuration: {', '.join(missing)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("✅ Configuration validation passed")


def create_snowflake_connection():
    """
    Create and return a Snowflake connection.
    """
    from snowflake.connector import connect
    
    logger.debug("🔌 Connecting to Snowflake...")
    conn = connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    logger.debug("✅ Snowflake connected")
    return conn


def format_number(num):
    """
    Format a number with commas for thousands.
    """
    return f"{num:,}"


def calculate_progress(current, target):
    """
    Calculate progress percentage.
    """
    if target == 0:
        return 0.0
    return (current / target) * 100


# ========================================
# OPENSKY DATA FETCHING FUNCTION
# ========================================

def fetch_opensky_flights(**context):
    """
    جلب بيانات الرحلات المباشرة من OpenSky Network API
    """
    logger.info("=" * 70)
    logger.info("📡 FETCHING LIVE FLIGHTS FROM OPENSKY NETWORK")
    logger.info("=" * 70)
    
    try:
        # OpenSky API endpoint for all states
        url = f"{OPENSKY_BASE_URL}/states/all"
        
        # Parameters: فقط الرحلات في الجو (on_ground=False)
        params = {
            'extended': True  # للحصول على بيانات إضافية
        }
        
        logger.info(f"🔗 Calling OpenSky API: {url}")
        
        response = requests.get(
            url,
            params=params,
            auth=HTTPBasicAuth(OPENSKY_USERNAME, OPENSKY_PASSWORD),
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            states = data.get('states', [])
            
            logger.info(f"✅ Found {len(states)} total flights")
            
            # تحويل البيانات لصيغة موحدة
            opensky_flights = []
            
            for state in states:
                # OpenSky returns array with specific indices:
                # [0]icao24, [1]callsign, [2]origin_country, [3]time_position, 
                # [4]last_contact, [5]longitude, [6]latitude, [7]baro_altitude, 
                # [8]on_ground, [9]velocity, [10]true_track, [11]vertical_rate, 
                # [12]sensors, [13]geo_altitude, [14]squawk, [15]spi, [16]position_source
                
                # فقط الرحلات في الجو (on_ground = false)
                if len(state) > 8 and state[8] is False:
                    
                    # استخراج كود airline من callsign (أول 2-3 حروف)
                    callsign = state[1] if len(state) > 1 and state[1] else ''
                    airline_code = callsign[:2] if callsign and len(callsign) >= 2 else 'UN'
                    
                    # استخراج origin country
                    origin_country = state[2] if len(state) > 2 and state[2] else 'Unknown'
                    
                    flight = {
                        'id': f"OS-{state[0] if len(state) > 0 else 'UNK'}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}",
                        'origin': origin_country[:3] if origin_country != 'Unknown' else 'UNK',
                        'destination': 'LIVE',  # OpenSky لا يوفر وجهة
                        'departure_date': datetime.now().strftime('%Y-%m-%d'),
                        'departure_time': datetime.now().strftime('%H:%M'),
                        'price': round(random.uniform(200, 1500), 2),  # سعر تقديري
                        'currency': 'USD',
                        'airline': airline_code,
                        'stops': 0,  # رحلات مباشرة
                        'data_source': 'opensky_live',
                        'raw_data': {
                            'icao24': state[0] if len(state) > 0 else None,
                            'callsign': callsign,
                            'origin_country': origin_country,
                            'longitude': state[5] if len(state) > 5 else None,
                            'latitude': state[6] if len(state) > 6 else None,
                            'altitude': state[7] if len(state) > 7 else None,
                            'velocity': state[9] if len(state) > 9 else None,
                            'true_track': state[10] if len(state) > 10 else None,
                            'vertical_rate': state[11] if len(state) > 11 else None,
                            'on_ground': state[8] if len(state) > 8 else None
                        }
                    }
                    opensky_flights.append(flight)
            
            logger.info(f"✅ Filtered to {len(opensky_flights)} airborne flights")
            
            # حفظ في ملف مؤقت
            if opensky_flights:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = f"/tmp/opensky_{timestamp}_{len(opensky_flights)}.json"
                
                with open(output_file, 'w') as f:
                    json.dump(opensky_flights, f)
                
                logger.info(f"💾 Saved {len(opensky_flights)} flights to {os.path.basename(output_file)}")
                return output_file
            else:
                logger.warning("⚠️ No airborne flights found")
                return None
            
        else:
            logger.warning(f"⚠️ OpenSky API error: {response.status_code}")
            if response.status_code == 401:
                logger.warning("   Authentication failed - check username/password")
            elif response.status_code == 429:
                logger.warning("   Rate limit exceeded - try again later")
            return None
            
    except ImportError:
        logger.warning("⚠️ requests module not installed")
        return None
    except Exception as e:
        logger.error(f"❌ OpenSky error: {e}")
        return None


# ========================================
# MAIN DATA EXTRACTION FUNCTION
# ========================================

def fetch_hybrid_flights(**context):
    """
    Main data extraction function that combines OpenSky live data with synthetic fallback.
    
    Returns:
        int: Total number of records generated/fetched
    """
    
    logger.info("=" * 70)
    logger.info("🚀 HYBRID DATA ENGINE - OPENSKY + SYNTHETIC FALLBACK")
    logger.info(f"🎯 Target: {format_number(TARGET_RECORDS)} records")
    logger.info(f"📅 Execution Date: {context['execution_date']}")
    logger.info("=" * 70)
    
    # Initialize data structures
    all_flights = []
    execution_date = context['execution_date']
    
    # Setup directories for checkpoint files
    checkpoint_dir, archive_dir = setup_directories()
    
    # Validate configuration before proceeding
    try:
        validate_configuration()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.warning("⚠️ Continuing with synthetic only mode...")
    
    # ========================================
    # PART 1: OPENSKY LIVE FLIGHTS
    # ========================================
    
    logger.info("\n" + "-" * 70)
    logger.info("📡 PHASE 1: Fetching live flights from OpenSky Network")
    logger.info("-" * 70)
    
    opensky_file = fetch_opensky_flights(**context)
    
    if opensky_file and os.path.exists(opensky_file):
        with open(opensky_file, 'r') as f:
            opensky_flights = json.load(f)
        
        logger.info(f"✅ Added {len(opensky_flights)} live flights from OpenSky")
        all_flights.extend(opensky_flights)
    else:
        logger.warning("⚠️ No OpenSky flights available")
    
    # ========================================
    # PART 2: SYNTHETIC DATA GENERATION
    # ========================================
    
    # Calculate how many more records we need
    remaining = TARGET_RECORDS - len(all_flights)
    
    if remaining > 0:
        logger.info("\n" + "-" * 70)
        logger.info(f"⚙️ PHASE 2: Generating {format_number(remaining)} synthetic records")
        logger.info("-" * 70)
        
        # Time-of-day patterns (weights for realistic flight times)
        hour_pool = list(range(0, 24))
        hour_weights = [
            5, 3, 2, 1, 1, 2, 5, 10, 15, 20, 25, 30,
            35, 30, 25, 20, 25, 30, 35, 40, 35, 25, 15, 10
        ]
        
        # Generate synthetic records
        start_time = time.time()
        
        for i in range(remaining):
            # Log progress every 10,000 records
            if i % 10000 == 0 and i > 0:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                logger.info(f"  📊 Generated {format_number(i)} records... ({rate:.0f} records/sec)")
            
            # Select random route from expanded list
            origin, dest = random.choice(EXPANDED_ROUTES)
            
            # Random departure date within next 6 months
            rand_days = random.randint(1, 180)
            dep_date = (execution_date + timedelta(days=rand_days)).strftime('%Y-%m-%d')
            
            # Random departure time with realistic distribution
            hour = random.choices(hour_pool, weights=hour_weights)[0]
            minute = random.randint(0, 59)
            dep_time = f"{hour:02d}:{minute:02d}"
            
            # Base price calculation
            base_price = random.uniform(150, 2500)
            
            # Route factor (international vs domestic)
            if origin[0] != dest[0]:
                route_factor = random.uniform(1.2, 2.0)
            else:
                route_factor = random.uniform(0.8, 1.2)
            
            # Stops configuration
            stops = random.choices([0, 1, 2], weights=[0.4, 0.4, 0.2])[0]
            
            # Price adjustment based on number of stops
            if stops == 0:
                price = base_price * 1.3 * route_factor
            elif stops == 2:
                price = base_price * 0.8 * route_factor
            else:
                price = base_price * 1.0 * route_factor
            
            # Peak hour pricing
            if 7 <= hour <= 9 or 16 <= hour <= 19:
                price *= 1.15
            
            # Weekend pricing
            dep_weekday = (execution_date + timedelta(days=rand_days)).weekday()
            if dep_weekday >= 5:
                price *= 1.1
            
            # Calculate arrival time
            base_duration = random.randint(60, 720)
            duration_with_stops = base_duration + (stops * 45)
            arrival_hour = (hour + (duration_with_stops // 60)) % 24
            arrival_minute = (minute + (duration_with_stops % 60)) % 60
            
            # Create synthetic flight record
            all_flights.append({
                'id': f"SYNTH-{execution_date.strftime('%Y%m%d%H%M%S')}-{i:06d}-{random.randint(1000, 9999)}",
                'origin': origin,
                'destination': dest,
                'departure_date': dep_date,
                'departure_time': dep_time,
                'price': round(price, 2),
                'currency': 'USD',
                'airline': random.choice(AIRLINES_POOL),
                'stops': stops,
                'data_source': 'synthetic_engine',
                'raw_data': {
                    'source': 'synthetic_data_engine',
                    'generated_at': datetime.now().isoformat(),
                    'aircraft': random.choice(AIRCRAFT_POOL),
                    'fare_class': random.choice(FARE_CLASSES),
                    'duration_minutes': duration_with_stops,
                    'arrival_time': f"{arrival_hour:02d}:{arrival_minute:02d}",
                    'seat_availability': random.randint(0, 50)
                }
            })
        
        # Log synthetic generation results
        elapsed = time.time() - start_time
        rate = remaining / elapsed if elapsed > 0 else 0
        logger.info(f"  ✅ Generated {format_number(remaining)} synthetic records")
        logger.info(f"  ⏱️  Time: {elapsed:.1f} seconds ({rate:.0f} records/sec)")
    
    # ========================================
    # PART 3: FINAL DATASET STATISTICS
    # ========================================
    
    # Calculate statistics
    opensky_count = sum(1 for f in all_flights if f['data_source'] == 'opensky_live')
    synth_count = len(all_flights) - opensky_count
    opensky_percentage = calculate_progress(opensky_count, len(all_flights))
    synth_percentage = calculate_progress(synth_count, len(all_flights))
    
    logger.info("\n" + "=" * 70)
    logger.info("📊 FINAL DATASET STATISTICS")
    logger.info("=" * 70)
    logger.info(f"   • Total Records: {format_number(len(all_flights))} / {format_number(TARGET_RECORDS)}")
    logger.info(f"   • Progress: {calculate_progress(len(all_flights), TARGET_RECORDS):.1f}%")
    logger.info(f"   • OpenSky Live: {format_number(opensky_count)} ({opensky_percentage:.1f}%)")
    logger.info(f"   • Synthetic: {format_number(synth_count)} ({synth_percentage:.1f}%)")
    
    # ========================================
    # PART 4: SAVE TO CHECKPOINT FILE
    # ========================================
    
    timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
    final_file = f"{checkpoint_dir}/hybrid_{timestamp}_{len(all_flights)}.json"
    
    logger.info(f"\n💾 Saving dataset to checkpoint file...")
    
    try:
        with open(final_file, 'w') as f:
            json.dump(all_flights, f)
        
        file_size = os.path.getsize(final_file)
        file_size_mb = file_size / (1024 * 1024)
        
        logger.info(f"   • File: {os.path.basename(final_file)}")
        logger.info(f"   • Size: {file_size_mb:.2f} MB")
        
    except Exception as e:
        logger.error(f"❌ Error saving checkpoint file: {e}")
        raise
    
    # Push file path to XCom
    context['task_instance'].xcom_push(key='flights_file', value=final_file)
    
    logger.info("\n" + "=" * 70)
    logger.info("✅ FETCH PHASE COMPLETE")
    logger.info("=" * 70)
    
    return len(all_flights)


def load_to_snowflake(**context):
    """
    Load flight data from checkpoint file to Snowflake.
    """
    
    logger.info("\n" + "=" * 70)
    logger.info("📤 LOAD TO SNOWFLAKE PHASE")
    logger.info("=" * 70)
    
    # Get file path from XCom
    ti = context['task_instance']
    flights_file = ti.xcom_pull(key='flights_file', task_ids='fetch_hybrid_flights')
    
    logger.info(f"📋 XCom file path: {flights_file}")
    
    # If no file from XCom, look for existing checkpoint files
    if not flights_file or not os.path.exists(flights_file):
        logger.warning("📭 No file from XCom, searching for checkpoint files...")
        
        checkpoint_dir = '/tmp/flight_checkpoints'
        hybrid_files = glob.glob(f'{checkpoint_dir}/hybrid_*.json')
        
        if not hybrid_files:
            error_msg = "❌ No flight files found in checkpoint directory"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        flights_file = max(hybrid_files, key=os.path.getctime)
        logger.info(f"📋 Using most recent file: {os.path.basename(flights_file)}")
    
    # Read data from file
    logger.info(f"\n📖 Reading data from file...")
    
    try:
        with open(flights_file, 'r') as f:
            flights = json.load(f)
        
        logger.info(f"   • Records in file: {format_number(len(flights))}")
        
    except Exception as e:
        logger.error(f"❌ Error reading file: {e}")
        raise
    
    # Connect to Snowflake
    logger.info(f"\n🔌 Connecting to Snowflake...")
    
    try:
        conn = create_snowflake_connection()
        cursor = conn.cursor()
        logger.info("✅ Connected to Snowflake")
    except Exception as e:
        logger.error(f"❌ Snowflake connection error: {e}")
        raise
    
    # Create/update table structure
    logger.info(f"\n📦 Ensuring table structure...")
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS REAL_FLIGHTS (
        id STRING,
        origin STRING,
        destination STRING,
        departure_date DATE,
        departure_time STRING,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        price FLOAT,
        currency STRING,
        airline STRING,
        stops INTEGER,
        data_source STRING,
        raw_data STRING,
        batch_id STRING,
        source_file STRING
    )
    """
    
    try:
        cursor.execute(create_table_sql)
        logger.info("   ✅ Table REAL_FLIGHTS verified/created")
    except Exception as e:
        logger.error(f"❌ Error creating table: {e}")
        conn.close()
        raise
    
    # Clear old batch data
    logger.info(f"\n🧹 Clearing old batch data...")
    
    try:
        cursor.execute("DELETE FROM REAL_FLIGHTS WHERE batch_id = 'OPENSKY_BATCH'")
        deleted_count = cursor.rowcount
        logger.info(f"   ✅ Deleted {deleted_count} old records")
    except Exception as e:
        logger.warning(f"⚠️ Error clearing old batch: {e}")
    
    # Batch insert
    logger.info(f"\n📦 Loading {format_number(len(flights))} records in batches...")
    
    batch_size = BATCH_SIZE
    total_batches = (len(flights) + batch_size - 1) // batch_size
    successful_batches = 0
    total_inserted = 0
    failed_records = 0
    
    start_time = time.time()
    
    for i in range(0, len(flights), batch_size):
        batch = flights[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        logger.info(f"   Batch {batch_num}/{total_batches} ({format_number(len(batch))} records)...")
        
        values = []
        
        for f in batch:
            try:
                # Escape single quotes
                f_id = f['id'].replace("'", "''")
                f_origin = f['origin'].replace("'", "''")
                f_dest = f['destination'].replace("'", "''")
                f_currency = f.get('currency', 'USD').replace("'", "''")
                f_airline = f.get('airline', 'Unknown').replace("'", "''")
                f_source = f.get('data_source', 'unknown').replace("'", "''")
                f_dep_time = f.get('departure_time', '00:00').replace("'", "''")
                
                # Convert raw_data to JSON string
                raw_json = json.dumps(f.get('raw_data', {})).replace("'", "''")
                
                values.append(f"('{f_id}', '{f_origin}', '{f_dest}', '{f['departure_date']}', '{f_dep_time}', {f['price']}, '{f_currency}', '{f_airline}', {f['stops']}, '{f_source}', '{raw_json}', 'OPENSKY_BATCH', '{flights_file}')")
                
            except Exception as e:
                logger.warning(f"      ⚠️ Skipping record: {e}")
                failed_records += 1
                continue
        
        if values:
            insert_query = f"""
                INSERT INTO REAL_FLIGHTS 
                (id, origin, destination, departure_date, departure_time, price, currency, airline, stops, data_source, raw_data, batch_id, source_file) 
                VALUES {','.join(values)}
            """
            
            try:
                cursor.execute(insert_query)
                conn.commit()
                successful_batches += 1
                total_inserted += len(values)
                
                if batch_num % 5 == 0:
                    elapsed = time.time() - start_time
                    rate = total_inserted / elapsed if elapsed > 0 else 0
                    logger.info(f"      📊 Progress: {format_number(total_inserted)}/{format_number(len(flights))} ({total_inserted/len(flights)*100:.1f}%) - {rate:.0f} records/sec")
                    
            except Exception as e:
                logger.error(f"      ❌ Batch {batch_num} failed: {e}")
                conn.rollback()
    
    # Load summary
    elapsed = time.time() - start_time
    rate = total_inserted / elapsed if elapsed > 0 else 0
    
    logger.info("\n" + "-" * 70)
    logger.info("📊 LOAD SUMMARY")
    logger.info("-" * 70)
    logger.info(f"   • Successfully inserted: {format_number(total_inserted)}")
    logger.info(f"   • Failed records: {format_number(failed_records)}")
    logger.info(f"   • Time: {elapsed:.1f} seconds")
    logger.info(f"   • Average rate: {rate:.0f} records/sec")
    
    conn.close()
    
    # Archive file if successful
    if total_inserted == len(flights):
        logger.info(f"\n📦 Archiving file...")
        archive_dir = '/tmp/flight_checkpoints/loaded'
        os.makedirs(archive_dir, exist_ok=True)
        
        archive_name = f"{archive_dir}/{os.path.basename(flights_file)}.loaded"
        shutil.move(flights_file, archive_name)
        logger.info(f"   ✅ File archived")
    
    logger.info("\n" + "=" * 70)
    logger.info("✅ LOAD PHASE COMPLETE")
    logger.info("=" * 70)
    
    return total_inserted


def verify_data_quality(**context):
    """
    Verify that the target number of records has been loaded to Snowflake.
    """
    
    logger.info("\n" + "=" * 70)
    logger.info("📊 DATA QUALITY VERIFICATION")
    logger.info("=" * 70)
    
    # Connect to Snowflake
    try:
        conn = create_snowflake_connection()
        cursor = conn.cursor()
        logger.info("✅ Connected to Snowflake")
    except Exception as e:
        logger.error(f"❌ Connection error: {e}")
        raise
    
    # Count records
    logger.info(f"\n🔍 Counting records...")
    
    try:
        cursor.execute("SELECT COUNT(*) FROM REAL_FLIGHTS WHERE batch_id = 'OPENSKY_BATCH'")
        total = cursor.fetchone()[0]
        logger.info(f"   • Total records found: {format_number(total)}")
    except Exception as e:
        logger.error(f"❌ Error counting records: {e}")
        conn.close()
        raise
    
    # Get data source distribution
    logger.info(f"\n📦 Analyzing data sources...")
    
    try:
        cursor.execute("""
            SELECT data_source, COUNT(*) as count 
            FROM REAL_FLIGHTS 
            WHERE batch_id = 'OPENSKY_BATCH'
            GROUP BY data_source
        """)
        sources = cursor.fetchall()
        
        for source, count in sources:
            percentage = calculate_progress(count, total)
            logger.info(f"   • {source}: {format_number(count)} ({percentage:.1f}%)")
            
    except Exception as e:
        logger.warning(f"⚠️ Could not get source distribution: {e}")
    
    # Get price statistics
    logger.info(f"\n💰 Analyzing pricing...")
    
    try:
        cursor.execute("""
            SELECT 
                MIN(price) as min_price,
                AVG(price) as avg_price,
                MAX(price) as max_price
            FROM REAL_FLIGHTS 
            WHERE batch_id = 'OPENSKY_BATCH'
        """)
        price_stats = cursor.fetchone()
        
        if price_stats and price_stats[0]:
            logger.info(f"   • Price range: ${price_stats[0]:.0f} - ${price_stats[2]:.0f}")
            logger.info(f"   • Average price: ${price_stats[1]:.0f}")
            
    except Exception as e:
        logger.warning(f"⚠️ Could not get price statistics: {e}")
    
    # Verification result
    logger.info("\n" + "-" * 70)
    logger.info(f"📈 TOTAL RECORDS: {format_number(total)} / {format_number(TARGET_RECORDS)}")
    logger.info(f"📊 Progress: {calculate_progress(total, TARGET_RECORDS):.1f}%")
    logger.info("-" * 70)
    
    conn.close()
    
    if total < TARGET_RECORDS:
        remaining = TARGET_RECORDS - total
        logger.warning(f"\n⚠️ Target not reached: {format_number(remaining)} records remaining")
        raise ValueError(f"Data quality check failed: Only {total} records, need {TARGET_RECORDS}")
    else:
        logger.info("\n" + "🎉" * 20)
        logger.info("🎉🎉🎉 SUCCESS! TARGET ACHIEVED! 🎉🎉🎉")
        logger.info("🎉" * 20 + "\n")
    
    logger.info("\n" + "=" * 70)
    logger.info("✅ VERIFICATION PHASE COMPLETE")
    logger.info("=" * 70)
    
    return total


def cleanup_old_files(**context):
    """
    Clean up old checkpoint files to manage disk space.
    """
    
    logger.info("\n" + "=" * 70)
    logger.info("🧹 CLEANUP PHASE")
    logger.info("=" * 70)
    
    deleted_count = 0
    checkpoint_dir = '/tmp/flight_checkpoints'
    archive_dir = f'{checkpoint_dir}/loaded'
    
    # Cleanup hybrid files
    logger.info(f"\n📁 Cleaning hybrid files...")
    
    hybrid_files = glob.glob(f'{checkpoint_dir}/hybrid_*.json')
    hybrid_files.sort(key=os.path.getctime)
    
    files_to_keep = 5
    if len(hybrid_files) > files_to_keep:
        files_to_delete = hybrid_files[:-files_to_keep]
        
        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                deleted_count += 1
                logger.info(f"   ✅ Deleted: {os.path.basename(file_path)}")
            except Exception as e:
                logger.warning(f"   ⚠️ Error deleting {file_path}: {e}")
    
    # Cleanup archived files
    if os.path.exists(archive_dir):
        logger.info(f"\n📁 Cleaning archive directory...")
        
        loaded_files = glob.glob(f'{archive_dir}/*.loaded')
        now = time.time()
        seven_days = 7 * 24 * 60 * 60
        
        for file_path in loaded_files:
            file_age = now - os.path.getctime(file_path)
            
            if file_age > seven_days:
                try:
                    os.remove(file_path)
                    deleted_count += 1
                    logger.info(f"   ✅ Deleted: {os.path.basename(file_path)}")
                except Exception as e:
                    logger.warning(f"   ⚠️ Error deleting {file_path}: {e}")
    
    # Cleanup OpenSky files
    opensky_files = glob.glob(f'{checkpoint_dir}/opensky_*.json')
    for file_path in opensky_files:
        try:
            os.remove(file_path)
            deleted_count += 1
            logger.info(f"   ✅ Deleted OpenSky temp: {os.path.basename(file_path)}")
        except Exception as e:
            logger.warning(f"   ⚠️ Error deleting {file_path}: {e}")
    
    logger.info("\n" + "-" * 70)
    logger.info(f"📊 CLEANUP SUMMARY")
    logger.info("-" * 70)
    logger.info(f"   • Total files deleted: {deleted_count}")
    
    logger.info("\n" + "=" * 70)
    logger.info("✅ CLEANUP PHASE COMPLETE")
    logger.info("=" * 70)
    
    return deleted_count


# ========================================
# DAG DEFINITION
# ========================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

"""
DAG: opensky_hybrid_dag
Description: Hybrid data pipeline for flight data
- Fetches live flights from OpenSky Network API
- Falls back to synthetic data generation
- Targets 100,000 records for dbt modeling
- Loads to Snowflake with batch processing
"""

dag = DAG(
    'opensky_hybrid_dag',
    default_args=default_args,
    description='🚀 Hybrid Pipeline: OpenSky + Synthetic Fallback for 100k Flight Records',
    schedule_interval='@hourly',  # كل ساعة عشان OpenSky live data
    catchup=False,
    max_active_runs=1,
    tags=['opensky', 'synthetic', 'hybrid', '100k', 'snowflake'],
    doc_md=__doc__,
)

# Task 1: Fetch hybrid flight data (OpenSky + Synthetic)
task_fetch = PythonOperator(
    task_id='fetch_hybrid_flights',
    python_callable=fetch_hybrid_flights,
    provide_context=True,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=2),
)

# Task 2: Load data to Snowflake
task_load = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    provide_context=True,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=2),
)

# Task 3: Verify data quality
task_verify = PythonOperator(
    task_id='verify_data_quality',
    python_callable=verify_data_quality,
    provide_context=True,
    dag=dag,
    retries=1,
)

# Task 4: Cleanup old files
task_cleanup = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task_fetch >> task_load >> task_verify >> task_cleanup

# ========================================
# END OF DAG DEFINITION
# ========================================
