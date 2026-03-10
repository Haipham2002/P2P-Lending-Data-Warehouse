# CONFIG

# DB_CONFIG = {
#     'host': 'localhost',
#     'user': 'root',          # Replace with your MySQL username
#     'password': 'Hai.seal2002', # Replace with your MySQL password
#     'database': 'prosper_dw',
#     'port': 3306
# }


# Python script needs to talk to db (the service name) instead of localhost.
import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'pass': os.getenv('DB_PASSWORD', 'Hai.seal2002'), 
    'db': os.getenv('DB_NAME', 'prosper_dw'),
    'port': os.getenv('DB_PORT', '3307')
}