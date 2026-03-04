#!/bin/bash

echo "🦈 APEX PREDATOR V2 - STARTING BEAST MODE"
echo "=========================================="

# Check Python
python3 --version

# Check dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Start Redis
echo "Starting Redis..."
redis-server --daemonize yes

# Wait for Redis
sleep 2

# Start master controller
echo "🔥 STARTING MASTER CONTROLLER..."
python3 master_controller.py &
MASTER_PID=$!

# Start triangular nodes (5 máquinas)
echo "Starting triangular farming (5 nodes)..."
for i in {1..5}; do
    python3 triangular_beast.py &
done

# Start pump snipers (5 máquinas)
echo "Starting pump sniper (5 nodes)..."
for i in {1..5}; do
    python3 pump_sniper.py &
done

echo ""
echo "✅ APEX PREDATOR BEAST MODE ACTIVATED"
echo "📊 Dashboard: http://localhost:8000"
echo "📝 Logs: tail -f apex_master.log"
echo ""
echo "Processes running:"
ps aux | grep "python3" | grep -v grep

wait $MASTER_PID
