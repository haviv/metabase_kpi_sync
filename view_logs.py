#!/usr/bin/env python3
"""
Log viewer for ADO and JIRA export processes
"""
import os
import time
import glob
from pathlib import Path

def find_latest_logs():
    """Find the latest log files for both exports"""
    logs_dir = Path("logs")
    if not logs_dir.exists():
        print("No logs directory found. Run the exports first.")
        return None, None
    
    # Find latest ADO log
    ado_logs = glob.glob(str(logs_dir / "ado_export_*.log"))
    ado_log = max(ado_logs, key=os.path.getctime) if ado_logs else None
    
    # Find latest JIRA log
    jira_logs = glob.glob(str(logs_dir / "jira_export_*.log"))
    jira_log = max(jira_logs, key=os.path.getctime) if jira_logs else None
    
    return ado_log, jira_log

def tail_log(log_file, lines=50):
    """Show the last N lines of a log file"""
    if not log_file or not os.path.exists(log_file):
        print(f"Log file not found: {log_file}")
        return
    
    try:
        with open(log_file, 'r') as f:
            all_lines = f.readlines()
            last_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
            print(f"\n=== Last {len(last_lines)} lines from {os.path.basename(log_file)} ===")
            for line in last_lines:
                print(line.rstrip())
    except Exception as e:
        print(f"Error reading log file: {e}")

def follow_log(log_file):
    """Follow a log file in real-time (like tail -f)"""
    if not log_file or not os.path.exists(log_file):
        print(f"Log file not found: {log_file}")
        return
    
    print(f"\n=== Following {os.path.basename(log_file)} (Press Ctrl+C to stop) ===")
    
    try:
        with open(log_file, 'r') as f:
            # Go to end of file
            f.seek(0, 2)
            
            while True:
                line = f.readline()
                if line:
                    print(line.rstrip())
                else:
                    time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nStopped following log")
    except Exception as e:
        print(f"Error following log: {e}")

def main():
    """Main function for log viewer"""
    print("JIRA and ADO Export Log Viewer")
    print("=" * 40)
    
    ado_log, jira_log = find_latest_logs()
    
    if not ado_log and not jira_log:
        return
    
    print(f"Latest ADO log: {ado_log}")
    print(f"Latest JIRA log: {jira_log}")
    
    while True:
        print("\nOptions:")
        print("1. Show last 50 lines of ADO log")
        print("2. Show last 50 lines of JIRA log")
        print("3. Follow ADO log in real-time")
        print("4. Follow JIRA log in real-time")
        print("5. Show both logs (last 20 lines each)")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            tail_log(ado_log, 50)
        elif choice == '2':
            tail_log(jira_log, 50)
        elif choice == '3':
            follow_log(ado_log)
        elif choice == '4':
            follow_log(jira_log)
        elif choice == '5':
            tail_log(ado_log, 20)
            tail_log(jira_log, 20)
        elif choice == '6':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please enter 1-6.")

if __name__ == "__main__":
    main()
