#!/bin/bash

echo "======================================"
echo "  Gamma Model Local Setup (with GPU)"
echo "======================================"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "Python3 not found. Please install Python 3.8+ first."
    exit 1
fi

echo "Python found: $(python3 --version)"
echo ""

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

echo ""
echo "Installing PyTorch with CUDA support..."
echo "(This may take a few minutes)"
echo ""

# Install PyTorch with CUDA 11.8
pip install --upgrade pip
pip install torch --index-url https://download.pytorch.org/whl/cu118

# Install other dependencies
pip install numpy pandas

echo ""
echo "======================================"
echo "  Checking GPU availability..."
echo "======================================"
python3 -c "import torch; print('CUDA available:', torch.cuda.is_available()); print('GPU:', torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'None')"

echo ""
echo "======================================"
echo "  Setup Complete!"
echo "======================================"
echo ""
echo "To train a model, run:"
echo "  source venv/bin/activate"
echo "  python train_gamma_model.py <your_data.csv> SPX"
echo ""
echo "Example with NDJSON exports:"
echo "  python build_full_gamma_dataset.py"
echo "  python train_gamma_model.py all_indices_YYYY-MM-DD.csv SPX"
echo ""
