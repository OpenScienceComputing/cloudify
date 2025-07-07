conda install -c conda-forge micromamba
micromamba env create -f environment.yaml -y 
source activate /root/micromamba/envs/cloudify
pip install -U .
SITE_PACKAGES=$(python -c "import site; print(site.getsitepackages()[0])")
cp patches/utilszarr.py $SITE_PACKAGES/xpublish/utils/zarr.py
cp patches/pluginszarr.py $SITE_PACKAGES/xpublish/plugins/included/zarr.py
cp patches/intakeplugin.py $SITE_PACKAGES/xpublish_intake/plugins.py
