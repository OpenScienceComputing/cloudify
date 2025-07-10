#!/bin/bash
#conda install -c conda-forge micromamba
micromamba env create -f environment.yaml -y 
eval "$(micromamba shell hook --shell bash)"
micromamba activate cloudify
pip install -U git+https://gitlab.dkrz.de/data-infrastructure-services/cloudify.git
SITE_PACKAGES=$(python -c "import site; print(site.getsitepackages()[0])")
cp patches/utilszarr.py $SITE_PACKAGES/xpublish/utils/zarr.py
cp patches/pluginszarr.py $SITE_PACKAGES/xpublish/plugins/included/zarr.py
cp patches/intakeplugin.py $SITE_PACKAGES/xpublish_intake/plugins.py
