import pdfplumber
import requests
from io import BytesIO
from typing import List, Dict, Any

def normalize_key(key: str) -> str:
    """Normalize dictionary keys by removing newlines and standardizing format."""
    return key.replace('\n', '_').replace(' ', '_').lower()

def normalize_value(value: str) -> Any:
    """Normalize cell values by converting special cases to None."""
    value = value.strip()
    if value in ['X', '']:  # Both 'X' and empty strings become None
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
        
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                if tables:
                    for table in tables:
                        if table and table[0]:
                            headers = [normalize_key(str(h).strip()) for h in table[0]]
                            
                            for row in table[1:]:
                                row_data = [normalize_value(str(cell)) if cell is not None else None for cell in row]
                                row_dict = dict(zip(headers, row_data))
                                all_tables.append(row_dict)
        
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