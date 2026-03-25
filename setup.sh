#!/usr/bin/env bash
set -e

PYTHON=/opt/homebrew/bin/python3.12
VENV=.venv

echo "=== APEX Bot Setup ==="

# Create venv
if [ ! -d "$VENV" ]; then
    echo "Creating virtual environment with $PYTHON..."
    $PYTHON -m venv $VENV
else
    echo "Virtual environment already exists."
fi

source $VENV/bin/activate

echo "Upgrading pip..."
pip install --upgrade pip --quiet

echo "Installing dependencies..."
pip install -r requirements.txt

# Create .env from template if not present
if [ ! -f .env ]; then
    cp .env.template .env
    echo ""
    echo "⚠️  Created .env from template — fill in your keys before running!"
else
    echo ".env already exists."
fi

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit .env and add your keys"
echo "  2. source $VENV/bin/activate"
echo "  3. python market_radar.py    # Market scanner"
echo "  4. python whale_sniper.py   # Whale copy-trader"
echo ""
echo "DRY_RUN=True by default — no real trades until you change it."
