#!/bin/bash

# =============================================================================
# Financial Filing Analyzer - Startup Script
# =============================================================================
# This script starts the FastAPI backend and a simple Python HTTP server
# for the frontend.

# Ensure we're in the project root
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "🚀 Financial Filing Analyzer"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# -----------------------------------------------------------------------------
# 1. Cleanup existing processes
# -----------------------------------------------------------------------------
cleanup() {
    echo "🧹 Stopping services..."
    pkill -f "uvicorn backend.api:app" || true
    pkill -f "python3 -m http.server 3000" || true
    exit 0
}

trap cleanup SIGINT SIGTERM

echo "Checking ports..."
# Kill anything on port 8001 (Backend)
lsof -ti:8001 | xargs kill -9 2>/dev/null || true
# Kill anything on port 3000 (Frontend)
lsof -ti:3000 | xargs kill -9 2>/dev/null || true

# -----------------------------------------------------------------------------
# 2. Setup Data Directories
# -----------------------------------------------------------------------------
mkdir -p data/raw_filings
mkdir -p data/indexes
mkdir -p data/reports

# -----------------------------------------------------------------------------
# 3. Start Backend (FastAPI)
# -----------------------------------------------------------------------------
echo "Starting Backend (port 8001)..."
export PYTHONPATH=$PROJECT_DIR
uvicorn backend.api:app --host 0.0.0.0 --port 8001 --reload &
BACKEND_PID=$!

# Wait for backend to be ready
echo "Waiting for backend..."
for i in {1..30}; do
    if curl -s http://localhost:8001/api/health >/dev/null; then
        echo "✅ Backend is healthy!"
        break
    fi
    sleep 1
done

# -----------------------------------------------------------------------------
# 4. Start Frontend (Simple HTTP Server)
# -----------------------------------------------------------------------------
echo "Starting Frontend (port 3000)..."
cd frontend/public
python3 -m http.server 3000 &
FRONTEND_PID=$!
cd ../..

echo ""
echo "🎉 Application running!"
echo "➡️  Frontend: http://localhost:3000"
echo "➡️  Backend:  http://localhost:8001/docs"
echo ""
echo "Press Ctrl+C to stop."

# Keep script running
wait
