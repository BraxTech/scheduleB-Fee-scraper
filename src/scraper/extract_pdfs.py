import pdfplumber
import requests
from io import BytesIO
from typing import List, Dict, Any

def normalize_key(key: str) -> str:
    """Map PDF column names to database column names."""
    if not key or key == 'None' or key == '':
        return None
        
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
        'Site of Service Amount': 'site_of_service_amount',
        # Add common variations
        'Site of\nService\nAmount': 'site_of_service_amount',
        'Multiple Surgery\nIndicator': 'multiple_surgery_indicator'
    
    }
    
    return column_map.get(key, key)  # Return None for unmapped keys

def normalize_value(value: str) -> Any:
    """Normalize cell values by converting special cases to None."""
    if value is None:
        return None
    value = str(value).strip()
    if value in ['X', '', 'N/A', '-']:
        return None
    return value

def extract_pdf_data(url: str) -> List[Dict[str, Any]]:
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        pdf_bytes = BytesIO(response.content)
        all_tables = []
        
        with pdfplumber.open(pdf_bytes) as pdf:
            first_page = pdf.pages[0]
            tables = first_page.extract_tables()
            
            if tables and tables[0]:
                # Skip the title row if it contains "Workers' Compensation"
                start_row = 0
                for idx, row in enumerate(tables[0]):
                    if any('CPT/HCPC' in str(cell) for cell in row):
                        start_row = idx
                        break
                
                if start_row < len(tables[0]):
                    headers = [normalize_key(str(h)) for h in tables[0][start_row]]
                    valid_headers = [h for h in headers if h is not None]
                    
                    if not valid_headers:
                        print(f"No valid headers found in table")
                        return []
                        
                    for page in pdf.pages:
                        page_tables = page.extract_tables()
                        if page_tables:
                            for table in page_tables:
                                if table and len(table) > start_row + 1:  # Skip header and title rows
                                    for row in table[start_row + 1:]:
                                        row_data = {}
                                        for header, value in zip(headers, row):
                                            if header is not None:
                                                row_data[header] = normalize_value(value)
                                        
                                        if row_data:
                                            all_tables.append(row_data)
        
        return all_tables
            
    except Exception as e:
        print(f"Error processing PDF: {str(e)}")
        return []

if __name__ == "__main__":
    test_url = "https://www.pa.gov/content/dam/copapwp-pagov/en/dli/documents/businesses/compensation/wc/hcsr/medfeereview/fee-schedule/documents/part-b/e0665-e2310.pdf"
    tables = extract_pdf_data(test_url)
    
    if tables:
        print(f"Found {len(tables)} rows of data")
        for i, row_dict in enumerate(tables, 1):
            print(f"\nRow {i}:")
            print(row_dict)
    else:
        print("No tables found in the PDF")