conda install -c conda-forge micromamba pip python==3.11 -y
micromamba env create -f environment.yaml
source activate cloudify
mkdir logs
python scripts/testone.py > logs/log
