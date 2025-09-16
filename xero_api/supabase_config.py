
import os
import pandas as pd
import psycopg2
from supabase import create_client, Client
from dotenv import load_dotenv
import numpy as np
import json
from decimal import Decimal

# Custom JSON encoder to handle Decimal types
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(CustomJSONEncoder, self).default(obj)

# Load environment variables from .env file
load_dotenv()

class SupabaseConfig:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
        self.client: Client = None

        # Database connection details for direct connection
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT")
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")

    def initialize(self) -> bool:
        """Initializes the Supabase client and ensures helper functions exist."""
        if not self.url or not self.key:
            print("Error: SUPABASE_URL and SUPABASE_KEY must be set in .env file.")
            return False
        try:
            self.client = create_client(self.url, self.key)
            print("Supabase client initialized successfully.")
            # Ensure the required 'exec' function exists for running raw SQL
            self._create_exec_function_if_not_exist()
            return True
        except Exception as e:
            print(f"Failed to initialize Supabase client: {e}")
            return False

    def _create_exec_function_if_not_exist(self):
        """Creates the 'exec' function via a direct DB connection if it doesn't exist."""
        if not all([self.db_host, self.db_port, self.db_name, self.db_user, self.db_password]):
            print("Warning: DB connection details not found in .env. Cannot verify 'exec' function.")
            print("Please ensure DB_HOST, DB_PORT, DB_NAME, DB_USER, and DB_PASSWORD are set.")
            return

        conn_string = f"dbname='{self.db_name}' user='{self.db_user}' host='{self.db_host}' port='{self.db_port}' password='{self.db_password}' options='-c search_path=public'"
        
        function_sql = """
        CREATE OR REPLACE FUNCTION public.exec(sql text)
        RETURNS void AS $
        BEGIN
            EXECUTE sql;
        END;
        $ LANGUAGE plpgsql VOLATILE;
        """

        try:
            with psycopg2.connect(conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(function_sql)
            print("Successfully ensured 'exec' function exists in Supabase.")
        except psycopg2.OperationalError as e:
            print(f"Could not connect to the database to create 'exec' function: {e}")
            print("Please check your DB connection details in the .env file.")
            raise
        except Exception as e:
            print(f"An error occurred while creating 'exec' function: {e}")
            raise

    def _map_dtype_to_sql(self, dtype) -> str:
        """Maps pandas dtype to PostgreSQL type."""
        if pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'
        if pd.api.types.is_float_dtype(dtype):
            return 'FLOAT8'
        if pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMPTZ'
        # Default to TEXT for objects, strings, or any other type
        return 'TEXT'

    def create_table_if_not_exist(self, table_name: str, df: pd.DataFrame, primary_key: str):
        """Creates a table in Supabase if it doesn't already exist, based on DataFrame schema."""
        if self.client is None:
            raise ConnectionError("Supabase client not initialized.")

        if primary_key not in df.columns:
            # Case-insensitive check for primary key
            pk_in_cols = [c for c in df.columns if c.lower() == primary_key.lower()]
            if not pk_in_cols:
                raise ValueError(f"Primary key '{primary_key}' not found in DataFrame for table '{table_name}'.")
            primary_key = pk_in_cols[0]

        # Check if table exists using a direct DB connection to bypass schema cache issues
        conn_string = f"dbname='{self.db_name}' user='{self.db_user}' host='{self.db_host}' port='{self.db_port}' password='{self.db_password}' options='-c search_path=public'"
        try:
            with psycopg2.connect(conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);", (table_name,))
                    table_exists = cur.fetchone()[0]
        except Exception as e:
            print(f"Database connection error: {e}")
            raise

        if table_exists:
            print(f"Table '{table_name}' already exists.")
            return

        print(f"Table '{table_name}' does not exist. Creating it...")
        columns_sql = []
        for col, dtype in df.dtypes.items():
            sql_type = self._map_dtype_to_sql(dtype)
            if col.lower() == primary_key.lower():
                columns_sql.append(f'"{col}" {sql_type} PRIMARY KEY')
            else:
                columns_sql.append(f'"{col}" {sql_type}')
        
        create_sql = f'CREATE TABLE public."{table_name}" ({', '.join(columns_sql)})'
        
        try:
            # Use rpc to execute raw SQL.
            self.client.rpc('exec', {'sql': create_sql}).execute()
            print(f"Table '{table_name}' created successfully.")

            # Disable Row Level Security (RLS) for the new table
            disable_rls_sql = f'ALTER TABLE public."{table_name}" DISABLE ROW LEVEL SECURITY'
            self.client.rpc('exec', {'sql': disable_rls_sql}).execute()
            print(f"Row Level Security disabled for table '{table_name}'.")

            # After creating the table, the Supabase client's schema cache might be stale.
            # Re-initializing the client is a workaround to refresh the schema.
            print("Re-initializing Supabase client to refresh schema cache.")
            self.initialize()

        except Exception as e:
            print(f"Failed to create table '{table_name}': {e}")
            raise

    def expand_json_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expands columns containing JSON strings, dicts, or lists of dicts into separate columns."""
        df = df.copy()
        
        # Identify columns that need expansion
        cols_to_expand = []
        for col in df.columns:
            if df[col].dtype == 'object':
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample, str) and ('[' in sample or '{' in sample):
                    # Attempt to deserialize string representations of JSON
                    try:
                        df[col] = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                        sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    except (json.JSONDecodeError, TypeError):
                        pass # Not a valid JSON string, leave as is

                if isinstance(sample, (dict, list)):
                    cols_to_expand.append(col)

        for col in cols_to_expand:
            # Handle lists of dictionaries
            if not df[col].dropna().empty and isinstance(df[col].dropna().iloc[0], list):
                # Create a temporary DataFrame with the original index and the list column
                temp_df = df[[df.index.name if df.index.name else 'index_col', col]].copy()
                if not df.index.name:
                    temp_df['index_col'] = df.index # Add a temporary index column if not named

                # Explode the list column
                temp_df = temp_df.explode(col)
                
                # Normalize the exploded dictionaries
                normalized_list_df = pd.json_normalize(temp_df[col])
                
                # Prefix columns
                normalized_list_df.columns = [f"{col}.{sub_col}" for sub_col in normalized_list_df.columns]
                
                # Join back to the original DataFrame's index
                # First, drop the original list column from df
                df = df.drop(columns=[col])
                
                # Merge the normalized data back. Use left_index=True, right_index=True
                # to join on the index, which was preserved by explode.
                df = df.merge(normalized_list_df, left_index=True, right_index=True, how='left')

            # Handle single dictionaries
            elif not df[col].dropna().empty and isinstance(df[col].dropna().iloc[0], dict):
                # Normalize the dictionary column
                expanded_col = pd.json_normalize(df[col])
                expanded_col.columns = [f"{col}.{sub_col}" for sub_col in expanded_col.columns]
                
                # Drop original column and join expanded columns
                df = df.drop(columns=[col])
                df = df.join(expanded_col)
        return df

    def prepare_data_for_supabase(self, records: list) -> list:
        """Cleans data for Supabase insertion (e.g., converts NaN to None)."""
        clean_records = []
        for record in records:
            clean_record = {}
            for key, value in record.items():
                if pd.isna(value) or value is np.nan:
                    clean_record[key] = None
                elif isinstance(value, (dict, list)):
                    clean_record[key] = json.dumps(value, cls=CustomJSONEncoder) # Serialize complex types to JSON string
                else:
                    clean_record[key] = value
            clean_records.append(clean_record)
        return clean_records

    def upsert_data(self, table_name: str, records: list, primary_key: str):
        """Upserts data into a Supabase table."""
        if not records:
            print(f"No records to upsert for table '{table_name}'.")
            return

        if self.client is None:
            raise ConnectionError("Supabase client not initialized.")

        try:
            # The `upsert` method handles the insert-or-update logic automatically.
            response = self.client.table(table_name).upsert(records, on_conflict=primary_key).execute()

            # Check for errors in the response from Supabase
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Supabase returned an error: {response.error}")

            print(f"Successfully upserted {len(records)} records to '{table_name}'.")
            if response.data:
                print(f"Upsert response sample: {response.data[:1]}")
        except Exception as e:
            print(f"Error upserting data to '{table_name}': {e}")
            # Log the first record that might be causing the issue
            print(f"Sample failing record: {records[0] if records else 'N/A'}")
            raise

# Singleton instance
supabase_config = SupabaseConfig()
