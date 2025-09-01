#!/usr/bin/env python3
"""
Main script to run both ADO and JIRA exports with separate logging
"""
import subprocess
import sys
import time
import signal
import os
import datetime
from pathlib import Path

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print("\nReceived shutdown signal. Stopping all processes...")
    sys.exit(0)

def setup_logging():
    """Create logs directory and return log file paths"""
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    ado_log = logs_dir / f"ado_export_{timestamp}.log"
    jira_log = logs_dir / f"jira_export_{timestamp}.log"
    
    return ado_log, jira_log

def main():
    """Main function to run both exports with separate logging"""
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("Starting ADO and JIRA sync services...")
    print("Press Ctrl+C to stop all services")
    
    # Check if both scripts exist
    if not os.path.exists('export_ado.py'):
        print("Error: export_ado.py not found")
        sys.exit(1)
        
    if not os.path.exists('export_jira.py'):
        print("Error: export_jira.py not found")
        sys.exit(1)
    
    # Setup logging
    ado_log, jira_log = setup_logging()
    print(f"ADO logs: {ado_log}")
    print(f"JIRA logs: {jira_log}")
    
    try:
        # Run both exports in parallel using subprocesses
        print("\n=== Starting both export services ===")
        
        # Start ADO export with logging to file
        with open(ado_log, 'w') as ado_log_file:
            ado_process = subprocess.Popen([sys.executable, 'export_ado.py'],
                                         stdout=ado_log_file,
                                         stderr=subprocess.STDOUT,
                                         universal_newlines=True,
                                         bufsize=1)
        
        # Start JIRA export with logging to file
        with open(jira_log, 'w') as jira_log_file:
            jira_process = subprocess.Popen([sys.executable, 'export_jira.py'],
                                          stdout=jira_log_file,
                                          stderr=subprocess.STDOUT,
                                          universal_newlines=True,
                                          bufsize=1)
        
        print("Both services started successfully")
        print("ADO export PID:", ado_process.pid)
        print("JIRA export PID:", jira_process.pid)
        print("\n=== Monitoring services ===")
        print("Use 'tail -f logs/ado_export_*.log' to see ADO logs")
        print("Use 'tail -f logs/jira_export_*.log' to see JIRA logs")
        
        # Monitor both processes
        while True:
            # Check if ADO process is still running
            if ado_process.poll() is not None:
                print("ADO export process has stopped")
                break
                
            # Check if JIRA process is still running
            if jira_process.poll() is not None:
                print("JIRA export process has stopped")
                break
                
            time.sleep(5)  # Check every 5 seconds
            
    except KeyboardInterrupt:
        print("\nReceived interrupt signal. Shutting down gracefully...")
        
        # Terminate both processes
        if 'ado_process' in locals():
            print("Stopping ADO export...")
            ado_process.terminate()
            ado_process.wait()
            
        if 'jira_process' in locals():
            print("Stopping JIRA export...")
            jira_process.terminate()
            jira_process.wait()
            
        print("All services stopped")

if __name__ == "__main__":
    main()
