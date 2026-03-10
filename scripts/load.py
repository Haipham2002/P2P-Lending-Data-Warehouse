# LOAD: logic to connect to the database and upload your DataFrames.

# import pandas as pd
# from sqlalchemy import create_engine
# import sys
# import os

# # Add the project root to sys.path to allow importing from the config folder
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from config.db_config import DB_CONFIG

# def get_db_engine():
#     """
#     Creates a SQLAlchemy engine for MySQL connection.
#     """
#     try:
#         connection_url = (
#             f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
#             f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
#         )
#         engine = create_engine(connection_url)
#         return engine
#     except Exception as e:
#         print(f"Error creating engine: {e}")
#         return None

# def load_to_mysql(df, table_name, if_exists='append'):
#     """
#     Uploads a DataFrame to a specific MySQL table.
#     """
#     # print(f"--- Loading data into: {table_name} ---")
#     # engine = get_db_engine()

    
#     if engine is not None:
#         try:
#             # chunksize is used to handle large datasets more efficiently
#             df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False, chunksize=1000)
#             print(f"Success! Loaded {len(df)} rows into '{table_name}'.")
#         except Exception as e:
#             print(f"Error loading to {table_name}: {e}")
#     else:
#         print("Database engine could not be initialized.")

# if __name__ == "__main__":
#     print("Load module is ready for use in the main pipeline.")


# Add the project root to sys.path to allow importing from the config folder
# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# import pandas as pd
# from sqlalchemy import create_engine
# from sqlalchemy import text 
# import logging
# from config.db_config import DB_CONFIG

# # Get the logger we set up in main.py
# logger = logging.getLogger(__name__)

# from sqlalchemy import text # Add this import at the top

# def load_to_mysql(df, table_name, if_exists='append'):
#     try:
#         conn_str = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}"
#         engine = create_engine(conn_str)
        
#         # Connect and disable foreign key checks
#         with engine.connect() as conn:
#             conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            
#             # Perform the load
#             df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False, chunksize=5000)
            
#             # Re-enable the safety checks
#             conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
#             conn.commit() # Ensure changes are saved
            
#         logger.info(f"Success! Loaded {len(df)} rows into '{table_name}'.")
#     except Exception as e:
#         logger.error(f"Error loading to {table_name}: {str(e)}")
#         raise e


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from sqlalchemy import create_engine, text
import logging
from config.db_config import DB_CONFIG

logger = logging.getLogger(__name__)

def load_to_mysql(df, table_name, if_exists='append'):
    try:
        conn_str = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}"
        engine = create_engine(conn_str)
        
        with engine.begin() as conn:
            # Temporarily stop safety checks
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            
            # Clear the table manually so we don't get 'Duplicate Key' errors
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            
            # Load the new data
            df.to_sql(
                name=table_name, 
                con=conn, 
                if_exists='append', # Always append to an empty table
                index=False, 
                chunksize=5000
            )
            
            # Restart safety checks
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            
        logger.info(f"Success! Loaded {len(df)} rows into '{table_name}'.")
    except Exception as e:
        logger.error(f"Error loading to {table_name}: {str(e)}")
        raise e