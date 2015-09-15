# This is for initial scriopt to merge and for geocode data
sas data_import.sas
sas merge_mls.sas
sas export_for_gis.sas

# Once you get the geocode data run following command 

gis_import.sas
merge_gis_back.sas
mls_reformat.sas

cp -u ../output1/*.sas7bdat ../sas_dataset_final/

