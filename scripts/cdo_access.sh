cwd=$(pwd)
port="9010"
dsname="example"
export SSL_CERT_FILE=${cwd}/cert.pem
export HDF5_PLUGIN_PATH=/fastdata/k20200/k202186/public/hdf5/plugins/
cdo="/work/bm0021/cdo_incl_cmor/cdo-test_cmortest_gcc/bin/cdo"
zarr_prefix="#mode=zarr,s3"
infile="https://${HOSTNAME}:${port}/datasets/${dsname}/zarr${zarr_prefix}"
#
$cdo sinfo $infile
echo "$cdo sinfo $infile"
