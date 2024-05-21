conda install -c conda-forge micromamba
micromamba env create -f environment.yaml -y 
source activate /root/micromamba/envs/cloudify
pip install -U .
