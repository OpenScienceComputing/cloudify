#!/bin/bash
### Batch Queuing System is SLURM
#SBATCH --partition=shared
#SBATCH --time=168:00:00
#SBATCH --mail-type=FAIL
#SBATCH --account=bm0021
#SBATCH --output=cloudify_%j.log
#SBATCH --error=cloudify_%j.err
#SBATCH --qos=esgf
#SBATCH --mem=16G

echo $HOSTNAME
#/scratch/k/k204210/temp/ngc4008_P1D_3.parq
source activate /work/bm0021/conda-envs/cloudify
python xpublish_references.py "$@"

