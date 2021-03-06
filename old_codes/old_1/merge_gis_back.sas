/*******************************************************************
  Yearly setting is a headache
********************************************************************/
%let year = 2011 ;


*libname final "/home/datamain/MLS/output/";
*libname mls "/home/datamain/MLS/sas_dataset/";
*libname geo "/home/datamain/MLS/sas_dataset/GIS/";
libname final "../output/";
libname mls "../sas_dataset/";
libname geo "../sas_dataset/GIS/";
option compress = yes;


%macro mergeBack(mlsclass, byyear);
data gis;
set geo.gis_&byyear;
LN = LN1;

keep 
LN 
CCA          
COMMUNITY    
COUNTY       
CongsnDist   
IHS_Tract    
LSAD_TRANS   
Muni         
MuniCCA      
PUMA5        
TRACT        
TownRgn      
Township     
TractNm_No   
;
run;

data gis;
set gis;
LN1 = LN*1;
drop LN;
run;

proc sort nodupkey; by LN1; run;

data mls;
set mls.&mlsclass._&byyear;
LN1 = LN*1;
run;

proc sort; by LN1; run;

data final.&mlsclass._&byyear;
merge mls(in=a) gis(in = b);
by LN1;
if a = 1 and b = 1;
run;


%mend;
%mergeBack( multifamily, &year);
%mergeBack( twotofour, &year);      
%mergeBack( detachedsingle, &year);
%mergeBack( attachedsingle, &year);
%mergeBack( residentialproperty, &year);
%mergeBack( rentals, &year);
%mergeBack( lotsandland, &year);
%mergeBack( commercial, &year);
%mergeBack( mobilehomes, &year);
%mergeBack( residentialrental, &year);
%mergeBack( deededparking, &year);
%mergeBack( vacantland, &year);
%mergeBack( officetech, &year);
%mergeBack( business, &year);
%mergeBack( mixeduse, &year);
%mergeBack( retailstores, &year);
%mergeBack( instuttodevelop, &year);
%mergeBack( industrial, &year);

