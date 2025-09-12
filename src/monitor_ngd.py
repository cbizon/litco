#!/usr/bin/env python3
"""
Monitor NGD cleaning process progress.

This script monitors the NGD cleaning process by:
1. Checking if the process is still running
2. Parsing log output to extract progress information
3. Monitoring output file growth
4. Providing periodic progress reports
"""

import time
import subprocess
import json
from pathlib import Path
import re
from datetime import datetime

def check_process_running(process_name="clean_ngd.py"):
    """Check if the NGD cleaning process is still running."""
    try:
        result = subprocess.run(
            ["ps", "aux"], 
            capture_output=True, 
            text=True, 
            check=True
        )
        return process_name in result.stdout
    except subprocess.CalledProcessError:
        return False

def parse_batch_progress(log_text):
    """Parse batch progress from log text."""
    # Look for "Processed batch X/Y" messages
    batch_matches = re.findall(r'Processed batch (\d+)/(\d+)', log_text)
    if batch_matches:
        current, total = batch_matches[-1]  # Get the most recent
        return int(current), int(total)
    return None, None

def count_output_records(output_file):
    """Count number of records in output JSONL file."""
    if not output_file.exists():
        return 0
    
    try:
        with open(output_file, 'r') as f:
            return sum(1 for line in f if line.strip())
    except (FileNotFoundError, IOError):
        return 0

def get_file_size_mb(file_path):
    """Get file size in MB."""
    if not file_path.exists():
        return 0
    return file_path.stat().st_size / (1024 * 1024)

def monitor_ngd_progress(check_interval=30, max_checks=None):
    """
    Monitor NGD cleaning progress.
    
    Args:
        check_interval: Seconds between checks
        max_checks: Maximum number of checks (None for unlimited)
    """
    output_dir = Path("cleaned/ngd")
    output_file = output_dir / "ngd_cleaned.jsonl"
    failed_file = output_dir / "ngd_failed_normalizations.txt"
    
    print(f"🔍 Monitoring NGD cleaning process...")
    print(f"📁 Output directory: {output_dir}")
    print(f"📄 Output file: {output_file}")
    print(f"⏱️  Check interval: {check_interval}s")
    print("=" * 60)
    
    check_count = 0
    start_time = time.time()
    last_record_count = 0
    last_file_size = 0
    
    while True:
        check_count += 1
        current_time = datetime.now().strftime("%H:%M:%S")
        elapsed_time = time.time() - start_time
        
        # Check if process is running
        is_running = check_process_running()
        
        # Check output file progress
        record_count = count_output_records(output_file)
        file_size_mb = get_file_size_mb(output_file)
        failed_count = count_output_records(failed_file) if failed_file.exists() else 0
        
        # Calculate rates
        records_since_last = record_count - last_record_count
        size_change_mb = file_size_mb - last_file_size
        
        # Display progress
        print(f"[{current_time}] Check #{check_count} (Elapsed: {elapsed_time:.0f}s)")
        print(f"  🏃 Process running: {'✅ YES' if is_running else '❌ NO'}")
        print(f"  📊 Records written: {record_count:,} (+{records_since_last:,})")
        print(f"  📁 Output file size: {file_size_mb:.1f} MB (+{size_change_mb:.1f} MB)")
        print(f"  ❌ Failed normalizations: {failed_count:,}")
        
        if records_since_last > 0:
            rate_per_min = (records_since_last / check_interval) * 60
            print(f"  ⚡ Rate: ~{rate_per_min:.0f} records/min")
        
        print("-" * 40)
        
        # Update for next iteration
        last_record_count = record_count
        last_file_size = file_size_mb
        
        # Check exit conditions
        if not is_running:
            if record_count > 0:
                print("🎉 Process completed! Final results:")
                print(f"  📊 Total records: {record_count:,}")
                print(f"  📁 Final file size: {file_size_mb:.1f} MB")
                print(f"  ❌ Failed normalizations: {failed_count:,}")
                
                # Check for other output files
                biolink_file = output_dir / "ngd_biolink_classes.json"
                if biolink_file.exists():
                    biolink_size = get_file_size_mb(biolink_file)
                    print(f"  🔗 Biolink classes file: {biolink_size:.1f} MB")
            else:
                print("❌ Process stopped but no output found. Check for errors.")
            break
            
        if max_checks and check_count >= max_checks:
            print(f"🛑 Reached maximum checks ({max_checks}). Stopping monitor.")
            break
        
        # Wait for next check
        time.sleep(check_interval)

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor NGD cleaning progress")
    parser.add_argument(
        "--interval", 
        type=int, 
        default=30, 
        help="Check interval in seconds (default: 30)"
    )
    parser.add_argument(
        "--max-checks", 
        type=int, 
        help="Maximum number of checks (default: unlimited)"
    )
    
    args = parser.parse_args()
    
    try:
        monitor_ngd_progress(
            check_interval=args.interval,
            max_checks=args.max_checks
        )
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped by user")
    except Exception as e:
        print(f"❌ Error during monitoring: {e}")

if __name__ == "__main__":
    main()