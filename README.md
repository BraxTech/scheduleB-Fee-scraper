# Schedule B Fee Scraper

## Overview
This project scrapes and maintains an up-to-date database of Schedule B fee data for Pennsylvania Workers' Compensation from the official PA government website. It processes PDF documents containing fee schedules and stores the extracted data in a PostgreSQL database, ensuring that duplicate entries are detected and avoided.

## Features
- Automated scraping of PDF documents from the Pennsylvania government website.
- Table extraction and data normalization from PDFs.
- Data storage in PostgreSQL with duplicate detection.
- Comprehensive logging system to track operations and errors.
- Configurable through environment variables.
- Automated daily updates via cron job.



### Prerequisites
- Python 3.x
- PostgreSQL (locally or on a service like Railway)
- Dependencies listed in `requirements.txt`

