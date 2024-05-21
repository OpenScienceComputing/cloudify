conda install -c conda-forge pip python==3.11 -y
pip install -U .
mkdir logs
python scripts/testone.py > logs/log
