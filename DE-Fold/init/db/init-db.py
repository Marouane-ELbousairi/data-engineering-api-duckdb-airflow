import duckdb
import logging
import os

# ========== Setup Logging ==========
log_file = "etl_creation.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file)
    ]
)

# ========== DuckDB File ==========
db_path = "ecommerce_data.duckdb"

def create_duckdb_structure():
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True) if "/" in db_path else None

        # Connect to DuckDB (file will be created if it doesn't exist)
        conn = duckdb.connect(database=db_path)
        logging.info("Connected to DuckDB database.")

        # Create Schemas
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw_layer;")
        logging.info("Created raw_layer schema.")

        conn.execute("CREATE SCHEMA IF NOT EXISTS refined_layer;")
        logging.info("Created refined_layer schema.")

        conn.execute("CREATE SCHEMA IF NOT EXISTS report_layer;")
        logging.info("Created report_layer schema.")

        # Create raw table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_layer.sales (
            order_id INTEGER,
            order_date DATE,
            customer_id INTEGER,
            customer_name VARCHAR,
            product_id INTEGER,
            product_name VARCHAR,
            quantity INTEGER,
            unit_price DECIMAL(10, 2),
            payment_method VARCHAR,
            order_status VARCHAR
        );
        """)
        logging.info("Created raw_layer.sales table.")

        # Close connection
        conn.close()
        logging.info("Closed the database connection.")

    except Exception as e:
        logging.error(f"❌ Error: {e}")

# ========== MAIN ==========
if __name__ == "__main__":
    create_duckdb_structure()