from bs4 import BeautifulSoup
import requests

def fetch_part_b_pdf_urls():
    url = "https://www.pa.gov/agencies/dli/programs-services/workers-compensation/wc-health-care-services-review/wc-fee-schedule/part-b-fee-schedules.html"
    base_url = "https://www.pa.gov"
    part_b_urls = []
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        soup = BeautifulSoup(response.text, "html.parser")
        pdfbutton = soup.find_all('a', id=lambda x: x and x.startswith('button-'))
        
        for button in pdfbutton:
            if 'part-b' in button.get('href', '').lower():
                pdf_url = base_url + button.get('href')
                part_b_urls.append(pdf_url)
        
        return part_b_urls
    
    except requests.RequestException as e:
        print(f"Error fetching URLs: {e}")
        return []

if __name__ == "__main__":
    pdf_urls = fetch_part_b_pdf_urls()
    print(f"Found {len(pdf_urls)} Part B PDF URLs:")
    for url in pdf_urls:
        print(f"PDF URL: {url}")

