#!/usr/bin/env nix-shell
#!nix-shell -i bash -p python311Packages.uvicorn -p python311Packages.fastapi -p python311Packages.websockets -p python311Packages.requests -p python311Packages.pydantic -p python311Packages.pydantic-settings -p python311Packages.sqlalchemy

cd /home/runner/workspace
echo "Starting FastAPI server on port 8000..."
echo "Python: $(which python)"
python -c "import uvicorn; print('uvicorn:', uvicorn.__version__)"
python -c "import fastapi; print('fastapi:', fastapi.__version__)"

exec python server.py
