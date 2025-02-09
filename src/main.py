from scraper.fetch_pdfs import fetch_part_b_pdf_urls
from scraper.extract_pdfs import extract_pdf_data
from database.db_connector import init_db, close_db, get_db_connection
from utils.logger import setup_logger
import time
import signal
import sys
import json

def signal_handler(signum, frame):
    logger.info("\n\nGracefully shutting down...")
    close_db()
    sys.exit(0)

def is_valid_record(row):
    """Validate that required fields are not empty"""
    required_fields = ["cpt/hcpc_code", "medicare_location"]
    return all(row.get(field) and str(row.get(field)).strip() for field in required_fields)

def main():
    global logger
    logger = setup_logger()
    
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Fetching PDF URLs...")
    pdf_urls = fetch_part_b_pdf_urls()
    logger.info(f"Found {len(pdf_urls)} PDFs to process")

    # Initialize database connection
    logger.info("Connecting to database...")
    init_db()
    
    total_records = 0
    failed_pdfs = []
    
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            total_pdfs = len(pdf_urls)
            for pdf_num, url in enumerate(pdf_urls, 1):
                logger.info(f"\nProcessing PDF {pdf_num}/{total_pdfs}: {url}")
                start_time = time.time()
                
                try:
                    tables = extract_pdf_data(url)
                    if not tables:
                        logger.error("No data found in PDF: " + url)
                        failed_pdfs.append({"url": url, "error": "No data found"})
                        continue
                    
                    logger.info(f"\nFound {len(tables)} records in PDF")
                    pdf_start_time = time.time()
                    pdf_records = len(tables)
                    pdf_inserted = 0
                    pdf_updated = 0
                    
                    # Prepare all records for checking
                    check_query = """
                        WITH input_records AS (
                            SELECT * FROM json_to_recordset(%s) AS x(
                                "cpt/hcpc_code" text,
                                modifier text,
                                medicare_location text,
                                global_surgery_indicator text,
                                multiple_surgery_indicator text,
                                prevailing_charge_amount text,
                                fee_schedule_amount text,
                                site_of_service_amount text
                            )
                        )
                        SELECT 
                            i."cpt/hcpc_code",
                            i.modifier,
                            i.medicare_location,
                            CASE 
                                WHEN e."cpt/hcpc_code" IS NULL THEN 'new'
                                WHEN (
                                    COALESCE(e.global_surgery_indicator,'') != COALESCE(i.global_surgery_indicator,'') OR
                                    COALESCE(e.multiple_surgery_indicator,'') != COALESCE(i.multiple_surgery_indicator,'') OR
                                    COALESCE(e.prevailing_charge_amount,'') != COALESCE(i.prevailing_charge_amount,'') OR
                                    COALESCE(e.fee_schedule_amount,'') != COALESCE(i.fee_schedule_amount,'') OR
                                    COALESCE(e.site_of_service_amount,'') != COALESCE(i.site_of_service_amount,'')
                                ) THEN 'changed'
                                ELSE 'duplicate'
                            END as status,
                            e."cpt/hcpc_code" as existing_cpt,
                            e.modifier as existing_mod,
                            e.medicare_location as existing_loc,
                            e.global_surgery_indicator as existing_global,
                            e.multiple_surgery_indicator as existing_multi,
                            e.prevailing_charge_amount as existing_charge,
                            e.fee_schedule_amount as existing_fee,
                            e.site_of_service_amount as existing_site
                        FROM input_records i
                        LEFT JOIN pa_wc_scheduleb_fees e ON 
                            e."cpt/hcpc_code" = i."cpt/hcpc_code"
                            AND (e.modifier IS NOT DISTINCT FROM i.modifier)
                            AND e.medicare_location = i.medicare_location;
                    """
                    
                    # Check all records at once
                    cur.execute(check_query, (json.dumps(tables),))
                    results = cur.fetchall()
                    
                    # Separate records by status
                    new_records = []
                    update_records = []
                    
                    for record_num, result in enumerate(results, 1):
                        try:
                            (cpt_code, modifier, location, status, 
                             existing_cpt, existing_mod, existing_loc,
                             existing_global, existing_multi, existing_charge,
                             existing_fee, existing_site) = result

                            # Find matching row with better error handling
                            matching_rows = [r for r in tables 
                                          if r.get("cpt/hcpc_code") == cpt_code 
                                          and r.get("modifier") == modifier 
                                          and r.get("medicare_location") == location]
                            
                            if not matching_rows:
                                logger.error(f"Could not find matching row for CPT {cpt_code}, modifier {modifier}, location {location}")
                                continue
                            
                            row = matching_rows[0]
                            
                            # Add validation check
                            if not is_valid_record(row):
                                logger.error(f"Skipping invalid record: {row}")
                                continue
                            
                            logger.info(f"Processing ({record_num}/{pdf_records}): {row}")
                            
                            if status == 'duplicate':
                                logger.info(f"Skipping ({record_num}/{pdf_records}): Row already exists in database")
                            elif status == 'new':
                                new_records.append(row)
                                logger.info(f"INSERTED ({record_num}/{pdf_records}): New record added to database")
                            else:
                                update_records.append(row)
                                logger.info(f"UPDATE ({record_num}/{pdf_records}): Record will be updated")
                                
                        except Exception as e:
                            logger.error(f"Error processing record {record_num}: {str(e)}")
                            continue
                    
                    # Batch insert new records
                    if new_records:
                        insert_query = """
                            INSERT INTO pa_wc_scheduleb_fees (
                                "cpt/hcpc_code", modifier, medicare_location,
                                global_surgery_indicator, multiple_surgery_indicator,
                                prevailing_charge_amount, fee_schedule_amount,
                                site_of_service_amount
                            ) 
                            SELECT * FROM json_to_recordset(%s) AS x(
                                "cpt/hcpc_code" text,
                                modifier text,
                                medicare_location text,
                                global_surgery_indicator text,
                                multiple_surgery_indicator text,
                                prevailing_charge_amount text,
                                fee_schedule_amount text,
                                site_of_service_amount text
                            );
                        """
                        cur.execute(insert_query, (json.dumps(new_records),))
                        pdf_inserted = len(new_records)
                        total_records += pdf_inserted
                        logger.info(f"\nBatch inserted {pdf_inserted} new records")
                    
                    # Batch update changed records
                    if update_records:
                        update_query = """
                            UPDATE pa_wc_scheduleb_fees e
                            SET 
                                global_surgery_indicator = x.global_surgery_indicator,
                                multiple_surgery_indicator = x.multiple_surgery_indicator,
                                prevailing_charge_amount = x.prevailing_charge_amount,
                                fee_schedule_amount = x.fee_schedule_amount,
                                site_of_service_amount = x.site_of_service_amount
                            FROM json_to_recordset(%s) AS x(
                                "cpt/hcpc_code" text,
                                modifier text,
                                medicare_location text,
                                global_surgery_indicator text,
                                multiple_surgery_indicator text,
                                prevailing_charge_amount text,
                                fee_schedule_amount text,
                                site_of_service_amount text
                            )
                            WHERE e."cpt/hcpc_code" = x."cpt/hcpc_code"
                            AND (e.modifier IS NOT DISTINCT FROM x.modifier)
                            AND e.medicare_location = x.medicare_location;
                        """
                        cur.execute(update_query, (json.dumps(update_records),))
                        pdf_updated = len(update_records)
                        logger.info(f"Batch updated {pdf_updated} changed records")
                    
                    conn.commit()
                    
                    # PDF Summary
                    total_time = time.time() - pdf_start_time
                    logger.info(f"\n=== PDF {pdf_num}/{total_pdfs} Summary ===")
                    logger.info(f"File: {url}")
                    logger.info(f"Time: {total_time:.2f} seconds")
                    logger.info(f"Total Records: {pdf_records}")
                    logger.info(f"New Records: {pdf_inserted}")
                    logger.info(f"Updated Records: {pdf_updated}")
                    logger.info(f"Duplicates: {pdf_records - pdf_inserted - pdf_updated}")
                    logger.info("=" * 30 + "\n")
                    
                except Exception as e:
                    logger.error("Error processing PDF " + url + ": " + str(e))
                    failed_pdfs.append({"url": url, "error": str(e)})
                    conn.rollback()
                    continue
                
    finally:
        close_db()

    logger.info("\n=== Processing Complete ===")
    logger.info(f"Total PDFs processed: {len(pdf_urls)}")
    logger.info(f"Total records inserted: {total_records}")
    logger.info(f"Failed PDFs: {len(failed_pdfs)}")

    if failed_pdfs:
        logger.error("\nFailed PDFs:")
        for fail in failed_pdfs:
            logger.error("- URL: " + fail['url'])
            logger.error("  Error: " + fail['error'])

if __name__ == "__main__":
    main()