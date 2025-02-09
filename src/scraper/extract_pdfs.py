import pdfplumber
import requests
from io import BytesIO
from typing import List, Dict, Any

def normalize_key(key: str) -> str:
    """Map PDF column names to database column names."""
    key = key.replace('\n', ' ').strip()
    
    # Map of PDF column names to database column names
    column_map = {
        'CPT/HCPC Code': 'cpt/hcpc_code',
        'Modifier': 'modifier',
        'Medicare Location': 'medicare_location',
        'Global Surgery Indicator': 'global_surgery_indicator',
        'Multiple Surgery Indicator': 'multiple_surgery_indicator',
        'Prevailing Charge Amount': 'prevailing_charge_amount',
        'Fee Schedule Amount': 'fee_schedule_amount',
        'Site of Service Amount': 'site_of_service_amount'
    }
    
    return column_map.get(key, key)

def normalize_value(value: str) -> Any:
    """Normalize cell values by converting special cases to None."""
    if value is None:
        return None
    value = str(value).strip()
    if value in ['X', '', 'N/A', '-']:
        return None
    return value

def extract_pdf_data(url: str) -> List[Dict[str, Any]]:
    """
    Download and extract tabular data from a remote PDF.
    
    Args:
        url (str): URL of the PDF to download and parse
        
    Returns:
        List[Dict[str, Any]]: List of dictionaries containing normalized table data
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        pdf_bytes = BytesIO(response.content)
        all_tables = []
        total_rows = 0
        
        with pdfplumber.open(pdf_bytes) as pdf:
            # Only look at first page for headers
            first_page = pdf.pages[0]
            tables = first_page.extract_tables()
            
            if tables and tables[0] and tables[0][0]:
                # Get headers from first table and map to database column names
                headers = [normalize_key(str(h)) for h in tables[0][0]]
                
                # Process all pages with these headers
                for page in pdf.pages:
                    page_tables = page.extract_tables()
                    if page_tables:
                        for table in page_tables:
                            if table and len(table) > 1:  # Skip header row
                                for row in table[1:]:
                                    total_rows += 1
                                    if len(row) == len(headers):
                                        # Keep raw values, just convert None to empty string
                                        row_data = [str(cell).strip() if cell is not None else '' for cell in row]
                                        row_dict = dict(zip(headers, row_data))
                                        all_tables.append(row_dict)
        
        if all_tables:
            print(f"Found {len(all_tables)}/{total_rows} valid records in PDF")
        else:
            print(f"No valid records found out of {total_rows} total rows in PDF")
            
        return all_tables
            
    except requests.RequestException as e:
        print(f"Error downloading PDF from {url}: {e}")
        return []
    except Exception as e:
        print(f"Error parsing PDF from {url}: {e}")
        return []

if __name__ == "__main__":
    test_url = "https://www.pa.gov/content/dam/copapwp-pagov/en/dli/documents/businesses/compensation/wc/hcsr/medfeereview/fee-schedule/documents/part-b/0001u-00842.pdf"
    tables = extract_pdf_data(test_url)
    
    if tables:
        print(f"Found {len(tables)} rows of data")
        for i, row_dict in enumerate(tables, 1):
            print(f"\nRow {i}:")
            print(row_dict)
    else:
        print("No tables found in the PDF")