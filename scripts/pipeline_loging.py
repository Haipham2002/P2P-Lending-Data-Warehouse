# Implemented comprehensive logging and error handling to monitor pipeline health and reduce debugging time.

import os
import logging
from datetime import datetime

def setup_logging():
    """Sets up a dual-logging system (Console + File)."""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    log_filename = f"{log_dir}/pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)