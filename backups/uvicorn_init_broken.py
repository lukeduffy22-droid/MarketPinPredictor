from uvicorn.config import Config
from uvicorn.main import Server, main, run

__version__ = "0.38.0"
__all__ = ["main", "run", "Config", "Server"]

python3 -c "import torch; print(torch.__version__, torch.cuda.is_available())"
# or
python3 -c "import tensorflow as tf; print(tf.config.list_physical_devices('GPU'))"
nvidia-smi
python3 -c "import torch; print(torch.__version__, torch.cuda.is_available())"
# or
python3 -c "import tensorflow as tf; print(tf.config.list_physical_devices('GPU'))"
# Use an NVIDIA CUDA runtime image matching the CUDA version you need
FROM nvidia/cuda:11.8.0-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv git && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 8000
CMD ["bash", "-lc", "PYTHONPATH=. uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload"]
# build
docker build -t my-ml-app .

# run with GPU access and port mapping
docker run --gpus all -p 8000:8000 -e PORT=8000 my-ml-app
conda create -n ml python=3.11
conda activate ml
# PyTorch example (pick the right CUDA wheel)
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu118
# or TensorFlow: pip install tensorflow (match CUDA)
pip install -r requirements.txt
PYTHONPATH=. uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload
