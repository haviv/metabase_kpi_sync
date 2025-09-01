#!/bin/bash

# Function to handle shutdown
cleanup() {
    echo "Received shutdown signal. Stopping all processes..."
    kill $ADO_PID $JIRA_PID 2>/dev/null
    wait $ADO_PID $JIRA_PID 2>/dev/null
    echo "All services stopped"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Create logs directory
mkdir -p logs

# Use fixed log file names (will be recreated each time)
ADO_LOG="logs/ado_export.log"
JIRA_LOG="logs/jira_export.log"

# Clear existing log files
echo "" > "$ADO_LOG"
echo "" > "$JIRA_LOG"

# Function to rotate logs if they get too large (over 10MB)
rotate_logs() {
    local log_file="$1"
    local max_size=$((10 * 1024 * 1024))  # 10MB in bytes
    
    if [ -f "$log_file" ] && [ $(stat -f%z "$log_file" 2>/dev/null || stat -c%s "$log_file" 2>/dev/null) -gt $max_size ]; then
        echo "Log file $log_file is too large, rotating..."
        mv "$log_file" "${log_file}.old"
        echo "" > "$log_file"
    fi
}

echo "Starting ADO and JIRA sync services..."
echo "Press Ctrl+C to stop all services"
echo "ADO logs: $ADO_LOG"
echo "JIRA logs: $JIRA_LOG"

# Start ADO export in background with logging (unbuffered)
echo "Starting ADO export..."
python3 -u export_ado.py > "$ADO_LOG" 2>&1 &
ADO_PID=$!

# Start JIRA export in background with logging (unbuffered)
echo "Starting JIRA export..."
python3 -u export_jira.py > "$JIRA_LOG" 2>&1 &
JIRA_PID=$!

echo "Both services started successfully"
echo "ADO export PID: $ADO_PID"
echo "JIRA export PID: $JIRA_PID"

# Verify processes are running
echo "Verifying processes are running..."
if kill -0 $ADO_PID 2>/dev/null; then
    echo "ADO process is running (PID: $ADO_PID)"
else
    echo "ADO process failed to start"
fi

if kill -0 $JIRA_PID 2>/dev/null; then
    echo "JIRA process is running (PID: $JIRA_PID)"
else
    echo "JIRA process failed to start"
fi
echo ""
echo "=== Monitoring both services ==="
echo "Use 'tail -f $ADO_LOG' to see ADO logs"
echo "Use 'tail -f $JIRA_LOG' to see JIRA logs"
echo "Use 'tail -f $ADO_LOG $JIRA_LOG' to see both"
echo ""

# Function to show recent logs from both services
show_logs() {
    echo "=== Recent ADO logs ==="
    tail -10 "$ADO_LOG"
    echo ""
    echo "=== Recent JIRA logs ==="
    tail -10 "$JIRA_LOG"
    echo ""
}

# Show initial logs after a few seconds
sleep 3
echo "Checking if log files exist and have content..."
ls -la logs/
echo ""
echo "ADO log file size: $(wc -l < "$ADO_LOG" 2>/dev/null || echo '0') lines"
echo "JIRA log file size: $(wc -l < "$JIRA_LOG" 2>/dev/null || echo '0') lines"
echo ""
show_logs

# Monitor both processes and show logs periodically
while true; do
    # Check if processes are still running
    if ! kill -0 $ADO_PID 2>/dev/null; then
        echo "ADO export process has stopped"
        break
    fi
    
    if ! kill -0 $JIRA_PID 2>/dev/null; then
        echo "JIRA export process has stopped"
        break
    fi
    
    # Show logs every 30 seconds
    sleep 30
    
    # Rotate logs if they get too large
    rotate_logs "$ADO_LOG"
    rotate_logs "$JIRA_LOG"
    
    show_logs
done

echo "One or both services have stopped"
show_logs
