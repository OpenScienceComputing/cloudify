conda install -c conda-forge micromamba -y
micromamba env create -f environment.yaml -y
source activate /root/micromamba/envs/xpublish
python src/cataloghost.py > logs/log
