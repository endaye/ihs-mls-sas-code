/***************************************************************
   Only change for the year is the exportGLS(yr)
***************************************************************/
%let year = 2011 ;

/***************************************************************/

*libname mls "/home/datamain/MLS/sas_dataset/";
*libname gis "/home/datamain/MLS/data_source/GIS/";
libname mls "../sas_dataset/";
libname gis "../data_source/GIS/";
options compress = yes;


%macro exportGIS(byyear);
data gis.for_gis_&byyear;
set
mls.residentialproperty_&byyear
mls.rentals_&byyear
mls.lotsandland_&byyear
mls.commercial_&byyear
mls.detachedsingle_&byyear
mls.attachedsingle_&byyear
mls.mobilehomes_&byyear
mls.twotofour_&byyear
mls.residentialrental_&byyear
mls.deededparking_&byyear
mls.vacantland_&byyear
mls.multifamily_&byyear
mls.officetech_&byyear
mls.business_&byyear
mls.mixeduse_&byyear
mls.retailstores_&byyear
mls.instuttodevelop_&byyear
mls.industrial_&byyear
;
LN1 = LN*1;
LNG_X = LNG*1;
LAT_Y = LAT*1;
if LN = . then delete;
keep LN1 LAT_Y LNG_X;
run;

proc sort nodupkey; by LN1; run;

%mend;


* %exportGIS(2005);
* %exportGIS(2006);
* %exportGIS(2007);
* %exportGIS(2008);
* %exportGIS(2009);
* %exportGIS(2010);
%exportGIS(&year);

