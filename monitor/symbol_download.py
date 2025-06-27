#!/usr/bin/env python3
"""
DTN IQFeed Symbol Downloader using the correct API
Downloads all symbols using the actual API endpoints found in the JavaScript
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DTNCorrectAPIDownloader:
    def __init__(self, output_dir="dtn_symbols"):
        self.base_url = "https://ws1.dtn.com"
        self.search_url = f"{self.base_url}/SymbolSearch/QuerySymbolsDD"
        self.categories_url = f"{self.base_url}/SymbolSearch/GetSymbolCategories"
        self.session = requests.Session()
        self.output_dir = output_dir
        self.default_limit = 4998  # From the JavaScript: DEFAULT_LIMIT = 4998
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Headers to mimic browser requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            'Referer': 'https://ws1.dtn.com/IQ/Search/'
        }
        self.session.headers.update(self.headers)
    
    def get_categories(self):
        """Get available exchanges and security types"""
        try:
            params = {'symbology': 'IQ'}
            response = self.session.get(self.categories_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    logger.info("Successfully retrieved categories")
                    return data['data']
            return None
        except Exception as e:
            logger.error(f"Error getting categories: {e}")
            return None
    
    def search_symbols(self, next_key=None, retry_count=3, retry_delay=5):
        """Search for symbols with pagination support and retry mechanism"""
        params = {
            'nextKey': next_key,
            'searchText': '',  # Empty to get all symbols
            'symbology': 'iq',
            'exchange': None,  # All exchanges
            'secType': None,   # All security types
            'sicCode': None,
            'naicsCode': None,
            'onlyFront': 'false',
            'onlyContinuous': 'false',
            'onlyMini': 'false',
            'noOptions': 'false',
            'noSpreads': 'false',
            'limit': self.default_limit,
            'clientVersion': 'IQsite 1.0'
        }
        
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}
        
        last_error = None
        for attempt in range(retry_count):
            try:
                response = self.session.get(self.search_url, params=params, timeout=60)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data:
                        return data['data']
                    elif 'errors' in data:
                        # Handle known errors
                        error_msg = data['errors'][0] if data['errors'] else 'Unknown error'
                        logger.warning(f"API returned error: {error_msg}")
                        
                        # If it's a backend connection error, wait longer before retry
                        if 'backend search database' in error_msg:
                            if attempt < retry_count - 1:
                                wait_time = retry_delay * (attempt + 2)  # Exponential backoff
                                logger.info(f"Backend database error. Waiting {wait_time} seconds before retry {attempt + 2}/{retry_count}...")
                                time.sleep(wait_time)
                                continue
                        last_error = error_msg
                    else:
                        logger.error(f"Unexpected response structure: {data}")
                        last_error = "Unexpected response structure"
                else:
                    last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                    logger.error(last_error)
                    
                    # Retry on server errors
                    if response.status_code >= 500 and attempt < retry_count - 1:
                        wait_time = retry_delay * (attempt + 1)
                        logger.info(f"Server error. Waiting {wait_time} seconds before retry {attempt + 2}/{retry_count}...")
                        time.sleep(wait_time)
                        continue
                        
            except requests.exceptions.Timeout:
                last_error = "Request timed out"
                logger.warning(f"Attempt {attempt + 1}/{retry_count}: {last_error}")
                if attempt < retry_count - 1:
                    logger.info(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                    
            except requests.exceptions.ConnectionError as e:
                last_error = f"Connection error: {e}"
                logger.warning(f"Attempt {attempt + 1}/{retry_count}: {last_error}")
                if attempt < retry_count - 1:
                    wait_time = retry_delay * (attempt + 1)
                    logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    
            except Exception as e:
                last_error = f"Unexpected error: {e}"
                logger.error(f"Attempt {attempt + 1}/{retry_count}: {last_error}")
                if attempt < retry_count - 1:
                    time.sleep(retry_delay)
        
        logger.error(f"All retry attempts failed. Last error: {last_error}")
        return None
    
    def save_symbols_batch(self, symbols, batch_number):
        """Save a batch of symbols to CSV"""
        df = pd.DataFrame(symbols)
        
        # Save batch file
        batch_file = os.path.join(self.output_dir, f"batch_{batch_number}.csv")
        df.to_csv(batch_file, index=False)
        logger.info(f"Saved batch {batch_number} with {len(df)} symbols to {batch_file}")
        
        return df
    
    def download_all_symbols(self, delay=2, resume_from_batch=None):
        """Download all symbols using the correct pagination with resume capability"""
        all_dataframes = []
        batch = resume_from_batch if resume_from_batch else 1
        next_key = None
        total_symbols = 0
        total_reported = 0
        start_time = time.time()
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        # If resuming, load existing data
        if resume_from_batch and resume_from_batch > 1:
            logger.info(f"Resuming from batch {resume_from_batch}")
            # Load previous batches
            for i in range(1, resume_from_batch):
                batch_file = os.path.join(self.output_dir, f"batch_{i}.csv")
                if os.path.exists(batch_file):
                    df = pd.read_csv(batch_file)
                    all_dataframes.append(df)
                    total_symbols += len(df)
                    logger.info(f"Loaded batch {i} with {len(df)} symbols")
            
            # Try to find the next_key from a state file
            state_file = os.path.join(self.output_dir, "download_state.json")
            if os.path.exists(state_file):
                try:
                    with open(state_file, 'r') as f:
                        state = json.load(f)
                        next_key = state.get('next_key')
                        total_reported = state.get('total_reported', 0)
                        logger.info(f"Restored state: next_key={next_key}, total_reported={total_reported}")
                except:
                    logger.warning("Could not restore state, starting fresh")
        
        logger.info("="*60)
        logger.info("Starting DTN IQFeed symbol download...")
        logger.info("="*60)
        
        # First, get categories to show available options
        if batch == 1:
            categories = self.get_categories()
            if categories:
                exchanges = categories.get('exchange', [])
                security_types = categories.get('securityType', [])
                logger.info(f"Available exchanges: {len(exchanges)}")
                logger.info(f"Available security types: {len(security_types)}")
                logger.info("-"*60)
        
        while True:
            batch_start = time.time()
            logger.info(f"\nBatch {batch}:")
            logger.info(f"  Downloading...")
            
            if batch > 1:
                time.sleep(delay)
            
            # Search for symbols with retry mechanism
            result = self.search_symbols(next_key)
            
            if result is None:
                consecutive_errors += 1
                logger.error(f"  Failed to get data for batch {batch} (consecutive errors: {consecutive_errors})")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"  Too many consecutive errors ({consecutive_errors}). Stopping download.")
                    logger.info("  You can resume later using the resume_from_batch parameter.")
                    break
                
                # Wait before continuing to next batch
                wait_time = 30 * consecutive_errors  # Exponential backoff
                logger.info(f"  Waiting {wait_time} seconds before trying next batch...")
                time.sleep(wait_time)
                continue
            
            # Reset error counter on success
            consecutive_errors = 0
            
            # Extract data
            symbol_list = result.get('symbolList', [])
            total_found = result.get('totalFound', 0)
            
            # Update total reported on first batch
            if batch == 1 and total_found > 0:
                total_reported = total_found
                logger.info(f"  Total symbols available: {total_reported:,}")
            
            if not symbol_list:
                logger.info("  No symbols returned, reached end of data")
                break
            
            # Save this batch
            df = self.save_symbols_batch(symbol_list, batch)
            all_dataframes.append(df)
            
            # Update counters
            batch_symbols = len(symbol_list)
            total_symbols += batch_symbols
            batch_time = time.time() - batch_start
            
            # Progress information
            logger.info(f"  Symbols in this batch: {batch_symbols:,}")
            logger.info(f"  Total symbols downloaded: {total_symbols:,}")
            if total_reported > 0:
                progress = (total_symbols / total_reported) * 100
                logger.info(f"  Progress: {progress:.1f}%")
                
                # Estimate time remaining
                if progress > 0:
                    elapsed_time = time.time() - start_time
                    estimated_total_time = elapsed_time / (progress / 100)
                    remaining_time = estimated_total_time - elapsed_time
                    logger.info(f"  Estimated time remaining: {remaining_time/60:.1f} minutes")
                    
            logger.info(f"  Batch download time: {batch_time:.1f} seconds")
            
            # Check if there are more results
            has_more = result.get('hasMore', False)
            next_key = result.get('nextKey', None)
            
            # Save state for resume capability
            state = {
                'batch': batch + 1,
                'next_key': next_key,
                'total_symbols': total_symbols,
                'total_reported': total_reported
            }
            state_file = os.path.join(self.output_dir, "download_state.json")
            with open(state_file, 'w') as f:
                json.dump(state, f)
            
            if not has_more or next_key is None:
                logger.info("\n  Reached end of data (no more symbols available)")
                break
            
            batch += 1
            
            # Safety limit
            if batch > 1000:
                logger.warning("Reached safety limit of 1000 batches, stopping...")
                break
        
        # Calculate total time
        total_time = time.time() - start_time
        
        logger.info("\n" + "="*60)
        logger.info("DOWNLOAD SUMMARY")
        logger.info("="*60)
        logger.info(f"Total batches downloaded: {len(all_dataframes)}")
        logger.info(f"Total symbols downloaded: {total_symbols:,}")
        if total_reported > 0:
            completion_percentage = (total_symbols / total_reported) * 100
            logger.info(f"Download completion: {completion_percentage:.1f}%")
        logger.info(f"Total download time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
        if len(all_dataframes) > 0:
            logger.info(f"Average time per batch: {total_time/len(all_dataframes):.1f} seconds")
        
        # Combine all results
        if all_dataframes:
            logger.info(f"\nCombining {len(all_dataframes)} batches...")
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Remove duplicates based on symbol
            original_count = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=['symbol'])
            duplicates_removed = original_count - len(combined_df)
            
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed:,} duplicate symbols")
            
            logger.info(f"Final unique symbol count: {len(combined_df):,}")
            
            # Save final combined file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_file = os.path.join(self.output_dir, f"all_symbols_{timestamp}.csv")
            combined_df.to_csv(final_file, index=False)
            logger.info(f"\nSaved to: {final_file}")
            
            # Also save as latest
            latest_file = os.path.join(self.output_dir, "all_symbols_latest.csv")
            combined_df.to_csv(latest_file, index=False)
            logger.info(f"Also saved as: {latest_file}")
            
            # File size information
            file_size = os.path.getsize(latest_file) / (1024 * 1024)  # MB
            logger.info(f"File size: {file_size:.2f} MB")
            
            # Remove state file on successful completion
            state_file = os.path.join(self.output_dir, "download_state.json")
            if os.path.exists(state_file):
                os.remove(state_file)
            
            return combined_df
        else:
            logger.warning("No data was collected")
            return None
    
    def cleanup_batch_files(self):
        """Remove temporary batch files"""
        logger.info("Cleaning up batch files...")
        for file in os.listdir(self.output_dir):
            if file.startswith("batch_") and file.endswith(".csv"):
                try:
                    os.remove(os.path.join(self.output_dir, file))
                except:
                    pass
        logger.info("Cleanup complete")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='DTN IQFeed Symbol Downloader')
    parser.add_argument('--resume', type=int, help='Resume from specific batch number', default=None)
    parser.add_argument('--delay', type=int, help='Delay between batches in seconds', default=2)
    args = parser.parse_args()
    
    downloader = DTNCorrectAPIDownloader()
    
    try:
        # Download all symbols
        result_df = downloader.download_all_symbols(delay=args.delay, resume_from_batch=args.resume)
        
        if result_df is not None:
            print(f"\n{'='*80}")
            print(f"{'DOWNLOAD COMPLETE!':^80}")
            print(f"{'='*80}")
            
            # Summary statistics
            print(f"\nFINAL STATISTICS:")
            print(f"-" * 40)
            print(f"Total unique symbols: {len(result_df):,}")
            print(f"Data columns: {', '.join(result_df.columns)}")
            
            # Detailed breakdown by security type
            if 'securityType' in result_df.columns:
                print(f"\nBREAKDOWN BY SECURITY TYPE:")
                print(f"-" * 40)
                sec_types = result_df['securityType'].value_counts()
                for sec_type, count in sec_types.items():
                    percentage = (count / len(result_df)) * 100
                    print(f"{sec_type:<30} {count:>8,} ({percentage:>5.1f}%)")
            
            # Detailed breakdown by exchange
            if 'exchange' in result_df.columns:
                print(f"\nBREAKDOWN BY EXCHANGE:")
                print(f"-" * 40)
                exchanges = result_df['exchange'].value_counts()
                for exchange, count in exchanges.head(20).items():  # Top 20 exchanges
                    percentage = (count / len(result_df)) * 100
                    print(f"{exchange:<30} {count:>8,} ({percentage:>5.1f}%)")
                
                if len(exchanges) > 20:
                    print(f"... and {len(exchanges) - 20} more exchanges")
            
            # Sample data
            print(f"\nSAMPLE DATA (First 10 symbols):")
            print(f"-" * 80)
            sample_cols = ['symbol', 'description', 'exchange']
            display_cols = [col for col in sample_cols if col in result_df.columns]
            print(result_df[display_cols].head(10).to_string(index=False))
            
            # File information
            print(f"\nFILE INFORMATION:")
            print(f"-" * 40)
            print(f"Output directory: {os.path.abspath(downloader.output_dir)}")
            print(f"Main file: all_symbols_latest.csv")
            
            # Check file size
            latest_file = os.path.join(downloader.output_dir, "all_symbols_latest.csv")
            if os.path.exists(latest_file):
                file_size = os.path.getsize(latest_file) / (1024 * 1024)
                print(f"File size: {file_size:.2f} MB")
            
            # Additional statistics
            print(f"\nADDITIONAL STATISTICS:")
            print(f"-" * 40)
            
            # Count symbols by type
            if 'symbol' in result_df.columns:
                futures = result_df[result_df['symbol'].str.startswith('@')].shape[0]
                options = result_df[result_df['symbol'].str.contains('[CP]\\d', regex=True)].shape[0]
                stocks = result_df[~result_df['symbol'].str.startswith('@')].shape[0] - options
                
                print(f"Futures symbols (@): {futures:,}")
                print(f"Options symbols: {options:,}")
                print(f"Other symbols: {stocks:,}")
            
            print(f"\n{'='*80}")
            print("Download completed successfully!")
            print(f"{'='*80}")
            
        else:
            print("\n" + "="*80)
            print("DOWNLOAD FAILED OR INCOMPLETE")
            print("="*80)
            
            # Check if we can resume
            state_file = os.path.join(downloader.output_dir, "download_state.json")
            if os.path.exists(state_file):
                try:
                    with open(state_file, 'r') as f:
                        state = json.load(f)
                        print(f"\nDownload can be resumed from batch {state['batch']}")
                        print(f"Symbols downloaded so far: {state['total_symbols']:,}")
                        print(f"\nTo resume, run: python {os.path.basename(__file__)} --resume {state['batch']}")
                except:
                    pass
    
    except KeyboardInterrupt:
        logger.info("\nDownload interrupted by user")
        print("\n" + "="*80)
        print("DOWNLOAD INTERRUPTED BY USER")
        print("="*80)
        
        # Check if we can resume
        state_file = os.path.join(downloader.output_dir, "download_state.json")
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    print(f"\nDownload can be resumed from batch {state['batch']}")
                    print(f"Symbols downloaded so far: {state['total_symbols']:,}")
                    print(f"\nTo resume, run: python {os.path.basename(__file__)} --resume {state['batch']}")
            except:
                pass
                
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print("\n" + "="*80)
        print("DOWNLOAD FAILED - Unexpected error")
        print("="*80)
    finally:
        # Only ask about cleanup if download was successful
        if result_df is not None and len(result_df) > 0:
            cleanup = input("\nDo you want to remove temporary batch files? (y/n): ")
            if cleanup.lower() == 'y':
                downloader.cleanup_batch_files()
                print("Batch files cleaned up.")
            else:
                print("Batch files retained for inspection.")
                print("="*80)

        # Optional: clean up batch files
        cleanup = input("\nDo you want to remove temporary batch files? (y/n): ")
        if cleanup.lower() == 'y':
            downloader.cleanup_batch_files()
            print("Batch files cleaned up.")
        else:
            print("Batch files retained for inspection.")

if __name__ == "__main__":
    print("DTN IQFeed Symbol Downloader (Enhanced with Retry & Resume)")
    print("=" * 60)
    print("\nFeatures:")
    print("- Automatic retry on errors with exponential backoff")
    print("- Resume capability if download is interrupted")
    print("- Progress tracking with time estimates")
    print("- Handles backend database errors gracefully")
    print("\nUsage:")
    print("  Normal run: python script.py")
    print("  Resume:     python script.py --resume 41")
    print("  Custom delay: python script.py --delay 5")
    print("\nStarting download...\n")
    
    main()