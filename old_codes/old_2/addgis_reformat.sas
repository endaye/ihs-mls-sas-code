*******************************;
*        Yearly Change         ;
*******************************;
%let year = 2011;
*******************************;

option compress = yes;
%include "./importData.sas";
%let filepath = ../ ;
libname fout "&filepath.Format/";
libname mls "&filepath.sas_dataset/";
libname geo "&filepath.sas_dataset/GIS/";
libname mlsout "&filepath.output/";


%macro add_gis(mlsclass, gis_ds = gis_&year.);
*import gis file;
%importData(GIS, GIS, &gis_ds..txt, &gis_ds., N);

data gis; 
set geo.&gis_ds.;
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

data gis; set gis; LN1 = LN*1; drop LN; run;

proc sort data=gis nodupkey; by LN1; run;

data mls;
set mls.&mlsclass._&year;
LN1 = LN*1;
if compress(upcase(CNY),' ') = 'COOK' | compress(upcase(CNY),' ') = '';
run;

proc sort data=mls; by LN1; run;

data mlsout.&mlsclass._&year;
merge mls(in=a) gis(in=b);
by LN1;
if a=0 then delete;
if a=1 & b=0 & compress(upcase(CNY),' ')='' then delete;
run;

%mend add_gis;

%add_gis(AttachedSingle);
%add_gis(Business);
%add_gis(MixedUse);
%add_gis(Commercial);
%add_gis(DetachedSingle);
%add_gis(DeededParking);
%add_gis(Industrial);
%add_gis(LotsAndLand);
%add_gis(MultiFamily);
%add_gis(MobileHomes);
%add_gis(TwoToFour);
%add_gis(OfficeTech);
%add_gis(InstutToDevelop);
%add_gis(Rentals);
%add_gis(ResidentialProperty);
%add_gis(ResidentialRental);
%add_gis(RetailStores);
%add_gis(VacantLand);






/*
%macro tmp_addgis_all();
%do year = 2005 %to 2011;

%if &year <= 2009 %then %do;
%add_gis(AttachedSingle, gis_ds = gis_all_2005_2009);
%add_gis(Business, gis_ds = gis_all_2005_2009);
%add_gis(MixedUse, gis_ds = gis_all_2005_2009);
%add_gis(Commercial, gis_ds = gis_all_2005_2009);
%add_gis(DetachedSingle, gis_ds = gis_all_2005_2009);
%add_gis(DeededParking, gis_ds = gis_all_2005_2009);
%add_gis(Industrial, gis_ds = gis_all_2005_2009);
%add_gis(LotsAndLand, gis_ds = gis_all_2005_2009);
%add_gis(MultiFamily, gis_ds = gis_all_2005_2009);
%add_gis(MobileHomes, gis_ds = gis_all_2005_2009);
%add_gis(TwoToFour, gis_ds = gis_all_2005_2009);
%add_gis(OfficeTech, gis_ds = gis_all_2005_2009);
%add_gis(InstutToDevelop, gis_ds = gis_all_2005_2009);
%add_gis(Rentals, gis_ds = gis_all_2005_2009);
%add_gis(ResidentialProperty, gis_ds = gis_all_2005_2009);
%add_gis(ResidentialRental, gis_ds = gis_all_2005_2009);
%add_gis(RetailStores, gis_ds = gis_all_2005_2009);
%add_gis(VacantLand, gis_ds = gis_all_2005_2009);
%end;
%if &year >= 2010 %then %do;
%add_gis(AttachedSingle);
%add_gis(Business);
%add_gis(MixedUse);
%add_gis(Commercial);
%add_gis(DetachedSingle);
%add_gis(DeededParking);
%add_gis(Industrial);
%add_gis(LotsAndLand);
%add_gis(MultiFamily);
%add_gis(MobileHomes);
%add_gis(TwoToFour);
%add_gis(OfficeTech);
%add_gis(InstutToDevelop);
%add_gis(Rentals);
%add_gis(ResidentialProperty);
%add_gis(ResidentialRental);
%add_gis(RetailStores);
%add_gis(VacantLand);
%end;

%end;
%mend tmp_addgis_all;
%tmp_addgis_all();
*/
