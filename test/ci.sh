conda install -c conda-forge micromamba
micromamba create -c conda-forge -n cloudify pip python==3.11 eccodes scipy -y
source activate /root/micromamba/envs/cloudify
pip install -U .
mkdir logs
python scripts/testone.py > logs/log
