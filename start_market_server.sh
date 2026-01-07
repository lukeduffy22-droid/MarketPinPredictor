#!/bin/bash
# Market data collection startup script - works around Python environment issues

echo "=========================================="
echo "MARKET DATA COLLECTION STARTUP"
echo "=========================================="
echo "Current time: $(date)"
echo ""

# Run the FastAPI server with uvicorn using nix-shell
# This ensures clean Python environment with all dependencies
nix-shell -p 'python311.withPackages(ps: with ps; [uvicorn fastapi websockets requests pandas pyarrow sqlalchemy pydantic])' \
  --run "cd /home/runner/workspace && python server.py" &

SERVER_PID=$!
echo "✓ Server started with PID: $SERVER_PID"
echo ""

# Give server time to start
sleep 5

# Check if server is running
if kill -0 $SERVER_PID 2>/dev/null; then
    echo "✓ Server process is alive"
    
    # Test health endpoint
    echo "Testing API health..."
    sleep 2
    curl -s http://localhost:8000/health 2>&1 | head -5 || echo "(health check not yet available)"
else
    echo "✗ Server failed to start"
    exit 1
fi

echo ""
echo "=========================================="
echo "Market data collection is ACTIVE"
echo "WebSocket connections to Polygon API starting..."
echo "=========================================="
echo ""

# Keep parent process alive
wait $SERVER_PID
