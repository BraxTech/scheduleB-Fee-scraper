import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from psycopg2.pool import SimpleConnectionPool
import logging

# Load environment variables
load_dotenv()

# Global pool variable
pool: Optional[SimpleConnectionPool] = None
logger = logging.getLogger('fee_schedule_scraper')

def init_db():
    """Initialize database connection pool"""
    global pool
    try:
        # Railway provides DATABASE_URL, but we can fall back to individual vars
        database_url = os.getenv('DATABASE_URL')
        
        if database_url:
            pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=database_url
            )
        else:
            pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=os.getenv('PGHOST'),
                database=os.getenv('PGDATABASE'),
                user=os.getenv('PGUSER'),
                password=os.getenv('PGPASSWORD'),
                port=os.getenv('PGPORT', 5432)
            )
        logging.info("Database connection pool established.")
        return pool
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

@contextmanager
def get_db_connection():
    """Get a database connection from the pool"""
    conn = None
    try:
        if pool is None:
            raise Exception("Database pool not initialized")
        conn = pool.getconn()
        yield conn
    except Exception:
        if conn is not None:
            try:
                pool.putconn(conn)
            except Exception:
                pass
        raise
    else:
        if conn is not None:
            try:
                pool.putconn(conn)
            except Exception as e:
                logger.error(f"Error returning connection to pool: {str(e)}")

def close_db():
    """Close all database connections"""
    global pool
    if pool is not None:
        try:
            pool.closeall()
        except Exception as e:
            logger.error(f"Error closing database pool: {str(e)}")
        finally:
            pool = None

def batch_insert_records(records: List[Dict[str, Any]]) -> int:
    """Batch insert records into database."""
    global cur, conn
    
    if not records:
        return 0
    
    query = """
        INSERT INTO pa_wc_scheduleb_fees (
            "cpt/hcpc_code",
            modifier,
            medicare_location,
            global_surgery_indicator,
            multiple_surgery_indicator,
            prevailing_charge_amount,
            fee_schedule_amount,
            site_of_service_amount
        ) VALUES (
            %(cpt/hcpc_code)s,
            %(modifier)s,
            %(medicare_location)s,
            %(global_surgery_indicator)s,
            %(multiple_surgery_indicator)s,
            %(prevailing_charge_amount)s,
            %(fee_schedule_amount)s,
            %(site_of_service_amount)s
        )
    """
    
    cur.executemany(query, records)
    conn.commit()
    return cur.rowcount

def commit_transaction():
    """Commit the current transaction"""
    global conn
    if conn:
        conn.commit()

def rollback_transaction():
    """Rollback the current transaction"""
    global conn
    if conn:
        conn.rollback()

def get_db_cursor(connection):
    """
    Creates a cursor that returns results as dictionaries
    """
    return connection.cursor(cursor_factory=RealDictCursor)

def insert_fee_schedule(data):
    """
    Insert single fee schedule record with duplicate checking.
    """
    with get_db_connection() as conn:
        try:
            cur = get_db_cursor(conn)
            
            # First check if exact duplicate exists
            check_query = """
                SELECT id FROM pa_wc_scheduleb_fees 
                WHERE "cpt/hcpc_code" = %(cpt/hcpc_code)s
                AND (modifier IS NOT DISTINCT FROM %(modifier)s)
                AND medicare_location = %(medicare_location)s
                AND global_surgery_indicator = %(global_surgery_indicator)s
                AND multiple_surgery_indicator = %(multiple_surgery_indicator)s
                AND prevailing_charge_amount = %(prevailing_charge_amount)s
                AND (fee_schedule_amount IS NOT DISTINCT FROM %(fee_schedule_amount)s)
                AND (site_of_service_amount IS NOT DISTINCT FROM %(site_of_service_amount)s)
            """
            
            cur.execute(check_query, data)
            exists = cur.fetchone()
            
            if exists:
                logging.info(f"Skipping duplicate code: {data['cpt/hcpc_code']}")
                return
                
            # If no duplicate, insert the new record
            insert_query = """
                INSERT INTO pa_wc_scheduleb_fees (
                    "cpt/hcpc_code",
                    modifier,
                    medicare_location,
                    global_surgery_indicator,
                    multiple_surgery_indicator,
                    prevailing_charge_amount,
                    fee_schedule_amount,
                    site_of_service_amount
                ) VALUES (
                    %(cpt/hcpc_code)s,
                    %(modifier)s,
                    %(medicare_location)s,
                    %(global_surgery_indicator)s,
                    %(multiple_surgery_indicator)s,
                    %(prevailing_charge_amount)s,
                    %(fee_schedule_amount)s,
                    %(site_of_service_amount)s
                )
            """
            
            cur.execute(insert_query, data)
            conn.commit()
            logging.info(f"Successfully inserted code: {data['cpt/hcpc_code']}")
            
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
            conn.rollback()
            raise
        finally:
            cur.close()

def insert_many_fee_schedules(data_list, batch_size=1000):
    """
    Insert multiple fee schedule records using batch processing.
    """
    if not data_list:
        return

    with get_db_connection() as conn:
        try:
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                records_inserted = batch_insert_records(batch)
                logging.info(f"Inserted {records_inserted} records in batch")
        except Exception as e:
            logging.error(f"Error in batch insert: {e}")
            conn.rollback()
            raise
        else:
            conn.commit()

if __name__ == "__main__":
    # Test the database connection
    try:
        # Initialize the connection pool
        init_db()
        
        with get_db_connection() as conn:
            # Get a cursor and try a simple query
            cur = get_db_cursor(conn)
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print("PostgreSQL version:", version["version"])
            
            # Clean up
            cur.close()
        
        print("Connection test successful and connection closed.")
        
    except Exception as e:
        print(f"Test failed: {e}")

    # Test data
    test_data = {
        'cpt/hcpc_code': '00842',
        'modifier': None,
        'medicare_location': '004',
        'global_surgery_indicator': 'XXX',
        'multiple_surgery_indicator': '9',
        'prevailing_charge_amount': '69.62',
        'fee_schedule_amount': None,
        'site_of_service_amount': None
    }
    
    insert_fee_schedule(test_data)

