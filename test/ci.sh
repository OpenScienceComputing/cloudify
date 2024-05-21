conda install -c conda-forge pip -y
pip install -U .
mkdir logs
python scripts/testone.py > logs/log
