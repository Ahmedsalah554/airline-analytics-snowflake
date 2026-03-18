"""
Snowflake Data Connector for Amadeus API data
Handles writing API responses to Snowflake with proper schema management
"""

import os
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from snowflake.connector import connect
from snowflake.connector.errors import ProgrammingError
import sqlalchemy
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """
    Manages Snowflake connections and operations for the airline analytics pipeline.
    
    Supports:
    - Username/Password authentication
    - Private Key authentication
    - Direct SQL execution
    - Pandas DataFrame operations
    - Automatic schema/table creation
    """
    
    def __init__(
        self,
        account: str = None,
        user: str = None,
        password: str = None,
        warehouse: str = None,
        database: str = None,
        schema: str = None,
        private_key_path: str = None,
    ):
        """
        Initialize Snowflake connector.
        
        Args:
            account: Snowflake account (e.g., 'FR28068')
            user: Snowflake username (e.g., 'Karen')
            password: Snowflake password
            warehouse: Warehouse name (e.g., 'COMPUTE_WH')
            database: Database name (e.g., 'ANALYTICS_DB')
            schema: Schema name (e.g., 'PUBLIC')
            private_key_path: Path to private key for key-pair auth (optional)
        """
        self.account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = user or os.getenv("SNOWFLAKE_USER")
        self.password = password or os.getenv("SNOWFLAKE_PASSWORD")
        self.warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        self.database = database or os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DB")
        self.schema = schema or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
        self.private_key_path = private_key_path or os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        
        # Validate required credentials
        if not self.account or not self.user:
            raise ValueError(
                "Missing Snowflake credentials. "
                "Set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER environment variables."
            )
        
        if not self.password and not self.private_key_path:
            raise ValueError(
                "Need either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH"
            )
        
        self.connection = None
        self.engine = None
        logger.info(f"📍 Snowflake Config: {self.account}/{self.database}/{self.schema}")
    
    def connect(self):
        """Establish connection to Snowflake."""
        try:
            # Prepare connection parameters
            conn_params = {
                "account": self.account,
                "user": self.user,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
            }
            
            # Add authentication method
            if self.password:
                conn_params["password"] = self.password
                logger.info("🔐 Using username/password authentication")
            elif self.private_key_path:
                with open(self.private_key_path, 'rb') as f:
                    conn_params["private_key_content"] = f.read()
                logger.info("🔐 Using private key authentication")
            
            self.connection = connect(**conn_params)
            logger.info(f"✅ Connected to Snowflake: {self.account}/{self.database}/{self.schema}")
            return self.connection
        
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def get_sqlalchemy_engine(self):
        """Get SQLAlchemy engine for pandas operations."""
        if not self.engine:
            try:
                connection_string = (
                    f"snowflake://{self.user}:{self.password}@{self.account}/"
                    f"{self.database}/{self.schema}?warehouse={self.warehouse}"
                )
                self.engine = create_engine(connection_string)
                logger.info("✅ SQLAlchemy engine created")
            except Exception as e:
                logger.error(f"❌ Failed to create SQLAlchemy engine: {e}")
                raise
        return self.engine
    
    def close(self):
        """Close Snowflake connection."""
        if self.connection:
            self.connection.close()
            logger.info("✅ Snowflake connection closed")
    
    def execute_query(self, query: str, params: Dict = None) -> List[Dict]:
        """
        Execute SQL query and return results.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            List of dictionaries with results
        """
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or {})
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch all results
            results = cursor.fetchall()
            cursor.close()
            
            return [dict(zip(columns, row)) for row in results]
        
        except ProgrammingError as e:
            logger.error(f"❌ SQL Error: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Error executing query: {e}")
            raise
    
    def create_database_if_not_exists(self):
        """Create database if it doesn't exist."""
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.close()
            logger.info(f"✅ Database {self.database} ready")
        except Exception as e:
            logger.error(f"❌ Error creating database: {e}")
            raise
    
    def create_schema_if_not_exists(self):
        """Create schema if it doesn't exist."""
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            cursor.close()
            logger.info(f"✅ Schema {self.schema} ready")
        except Exception as e:
            logger.error(f"❌ Error creating schema: {e}")
            raise
    
    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
    ):
        """
        Write pandas DataFrame to Snowflake.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            if_exists: 'fail', 'replace', or 'append'
            index: Whether to include index
        """
        try:
            engine = self.get_sqlalchemy_engine()
            df.to_sql(
                table_name.lower(),
                engine,
                if_exists=if_exists,
                index=index,
                method='multi',
                chunksize=10000,
            )
            logger.info(f"✅ Wrote {len(df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"❌ Error writing DataFrame to {table_name}: {e}")
            raise
    
    def write_json_raw(
        self,
        data: Dict | List,
        table_name: str,
        source_name: str = None,
        timestamp: str = None,
    ):
        """
        Write raw JSON data to Snowflake table.
        
        Args:
            data: JSON data (dict or list)
            table_name: Target table name
            source_name: Source identifier (e.g., 'amadeus_flights')
            timestamp: Load timestamp
        """
        if not self.connection:
            self.connect()
        
        try:
            # Ensure data is list
            if isinstance(data, dict):
                data = [data]
            
            # Create DataFrame with JSON and metadata
            df = pd.DataFrame({
                'RAW_DATA': [json.dumps(item) for item in data],
                'SOURCE': source_name or 'unknown',
                'LOADED_AT': timestamp or datetime.now().isoformat(),
            })
            
            # Write to Snowflake
            self.write_dataframe(df, table_name, if_exists='append')
            logger.info(f"✅ Loaded {len(data)} raw records to {table_name}")
        
        except Exception as e:
            logger.error(f"❌ Error writing raw JSON: {e}")
            raise
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table."""
        try:
            result = self.execute_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
            return result[0]['CNT'] if result else 0
        except Exception as e:
            logger.error(f"❌ Error getting table count: {e}")
            return -1
    
    def get_schema_info(self) -> Dict:
        """Get information about current schema and its tables."""
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"""
                SELECT TABLE_NAME, ROW_COUNT, BYTES
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{self.schema}'
                ORDER BY TABLE_NAME
            """)
            
            tables = cursor.fetchall()
            cursor.close()
            
            return {
                "schema": self.schema,
                "tables": [
                    {
                        "name": t[0],
                        "row_count": t[1],
                        "bytes": t[2],
                    }
                    for t in tables
                ]
            }
        except Exception as e:
            logger.error(f"❌ Error getting schema info: {e}")
            raise


class SnowflakeAmadeusWriter:
    """
    Specialized writer for Amadeus API data to Snowflake.
    Handles flight offers, airports, airlines data.
    """
    
    def __init__(self, connector: SnowflakeConnector = None):
        """
        Initialize with Snowflake connector.
        
        Args:
            connector: SnowflakeConnector instance (creates new if None)
        """
        self.connector = connector or SnowflakeConnector()
        if not self.connector.connection:
            self.connector.connect()
    
    def write_flight_offers(self, offers: List[Dict], source_route: str = None):
        """Write Amadeus flight offers to Snowflake."""
        try:
            df = pd.DataFrame(offers)
            df['LOADED_AT'] = datetime.now()
            df['SOURCE_ROUTE'] = source_route or 'unknown'
            
            self.connector.write_dataframe(
                df,
                'FLIGHT_OFFERS_RAW',
                if_exists='append'
            )
            logger.info(f"✅ Wrote {len(offers)} flight offers")
        except Exception as e:
            logger.error(f"❌ Error writing flight offers: {e}")
            raise
    
    def write_airports(self, airports: List[Dict]):
        """Write airport data to Snowflake."""
        try:
            df = pd.DataFrame(airports)
            df['LOADED_AT'] = datetime.now()
            
            self.connector.write_dataframe(
                df,
                'AIRPORTS_RAW',
                if_exists='replace'  # Replace since reference data
            )
            logger.info(f"✅ Wrote {len(airports)} airports")
        except Exception as e:
            logger.error(f"❌ Error writing airports: {e}")
            raise
    
    def write_airlines(self, airlines: List[Dict]):
        """Write airline data to Snowflake."""
        try:
            df = pd.DataFrame(airlines)
            df['LOADED_AT'] = datetime.now()
            
            self.connector.write_dataframe(
                df,
                'AIRLINES_RAW',
                if_exists='replace'  # Replace since reference data
            )
            logger.info(f"✅ Wrote {len(airlines)} airlines")
        except Exception as e:
            logger.error(f"❌ Error writing airlines: {e}")
            raise
