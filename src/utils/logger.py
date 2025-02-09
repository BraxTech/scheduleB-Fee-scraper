import logging
from datetime import datetime
import os

def setup_logger():
    """Configure logging to write only to file, not console"""
    # Create logs directory if it doesn't exist
    log_dir = os.path.join('src', 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Set up the logger
    logger = logging.getLogger('fee_schedule_scraper')
    logger.setLevel(logging.INFO)
    
    # Prevent propagation to root logger (stops console output)
    logger.propagate = False
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create file handlers with UTF-8 encoding
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'scraper_{timestamp}.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
    file_handler.setLevel(logging.INFO)
    
    error_file = os.path.join(log_dir, f'scraper_{timestamp}_errors.log')
    error_handler = logging.FileHandler(error_file, encoding='utf-8', mode='w')
    error_handler.setLevel(logging.ERROR)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    
    # Also disable root logger's handlers
    root = logging.getLogger()
    root.handlers = []
    
    return logger