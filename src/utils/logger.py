import logging
from datetime import datetime
import os

def setup_logger():
    """Configure logging to write to file and stdout in production"""
    # Create logs directory if it doesn't exist and we're not in production
    if not os.getenv('RAILWAY_ENVIRONMENT'):
        log_dir = os.path.join('src', 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
    
    # Set up the logger
    logger = logging.getLogger('fee_schedule_scraper')
    logger.setLevel(logging.INFO)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Always add console handler in production
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Add file handlers only if not in production
    if not os.getenv('RAILWAY_ENVIRONMENT'):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(log_dir, f'scraper_{timestamp}.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
        file_handler.setLevel(logging.INFO)
        
        error_file = os.path.join(log_dir, f'scraper_{timestamp}_errors.log')
        error_handler = logging.FileHandler(error_file, encoding='utf-8', mode='w')
        error_handler.setLevel(logging.ERROR)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        error_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(error_handler)
    
    return logger