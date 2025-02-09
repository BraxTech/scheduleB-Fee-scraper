from setuptools import setup, find_packages

setup(
    name="fee_scheduleb_scraper",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        'psycopg2-binary==2.9.9',
        'python-dotenv==1.0.0',
        'requests==2.31.0',
        'pdfplumber==0.10.3',
        'beautifulsoup4==4.12.3',
        'lxml==5.1.0',
        'soupsieve==2.5',
    ],
    setup_requires=['setuptools'],
) 