option compress=yes;

%include "./importData.sas";
%let filepath = ../ ;
libname fout "&filepath.Format/";
libname folder "./";



***get update history data list***;

data update_old; set folder.update_list; run;
proc sort data=update_old nodupkey; by ds_date; run;

filename update pipe "ls &filepath.data_source/AT/*.mls";
data update_new;
infile update truncover;
input filein $200.;
format ds_date $8. ds_year $4. ds_mth $2. ds_day $2.;
ds_date = compress(scan(scan(filein, -1, '_'),1,'.'), ' ');
ds_year = substr(ds_date,1,4);
ds_mth = substr(ds_date,5,2);
ds_day = substr(ds_date,7,2);
run;
proc sort data=update_new nodupkey; by ds_date; run;

data new_add;
merge update_old(in=a) update_new(in=b);
by ds_date;
if a=0 & b=1;
run;

*out new update list;
data folder.update_list; set update_new; run;
***DONE***;



***daily data import and combine***;

%macro import_combine(dstype, dtype_out);
*import txt into sas datasets;
data data_list;
set new_add;
format filein $100. fileout $100.;
filein = cats(&dstype,'_',ds_date,'.txt');
fileout = cats(&dstype,'_',ds_date);
run;

%let dsnum = %eval(0);
data _NULL_;
set data_list;
call symputx("dsnum",max(_N_,0));
call symput(cats("dsin",_N_),filein);
call symput(cats("dsout",_N_),fileout);
run;

%put dsnum = &dsnum;

%if &dsnum >= 1 %then %do;
%do p = 1 %to &dsnum;
	%put &&dsin&p;
	%put &&dsout&p;
	%if &p = 1 %then %do;
		%importData(&dstype, &dstype, &&dsin&p, &&dsout&p, Y);
	%end;
	%if &p > 1 %then %do;
		%importData(&dstype, &dstype, &&dsin&p, &&dsout&p, N);
	%end;
%end;
%end;


*combine sas datasets;

libname sasin "&filepath.sas_dataset/&dstype./";
libname sasout "&filepath.sas_dataset/";

proc sort data=data_list(keep = ds_year) out=year_list nodupkey; by ds_year; run;
data _NULL_;
set year_list;
call symputx("yrnum", _N_);
call symput(cats("yr",_N_), ds_year);
run;

%do q = 1 %to &yrnum;
	data data_list2;
	set data_list;
	if ds_year = &&yr&q ;
	run;

	data _NULL_;
	set data_list2;
	call symputx("filenum", _N_);
	call symput(cats("file",_N_), fileout);
	run;

	%put year = &&yr&q;
	%put filenum = &filenum;

	%do r = 1 %to &filenum;
		%put file = &&file&r;
		data tmp_ds&r; set sasin.&&file&r; run;
		proc contents data=tmp_ds&r varnum noprint out=tmp_ctnt&r; run;
		data tmp_ctnt&r; set tmp_ctnt&r; if type=2; keep name length; rename length=length&r; run;
		proc sort data=tmp_ctnt&r; by name; run;
	%end;

	data tmp_ctnt_all;
	merge tmp_ctnt1 - tmp_ctnt&filenum;
	by name;
	max_len = max(of length1 - length&filenum);
	run;

	data _NULL_;
	set tmp_ctnt_all;
	call symput(cats('var',_N_), name);
	call symputx(cats('len',_N_), max_len);
	call symput(cats('fmt',_N_), cats('$',max_len,'.'));
	call symputx('max_var', _N_);
	run;
	
	data new_&dtype_out._&&yr&q;
	%do i = 1 %to &max_var;
	length &&var&i $ &&len&i;
	format &&var&i &&fmt&i;
	%end;
	set tmp_ds1 - tmp_ds&filenum;
	run;
%end;


***********************************************;
***output data;
%let old_obs = %eval(0);
data old_&dtype_out._&&yr&q;
***;
run;
***********************************************;

%mend import_combine;

%macro exportGIS(byyear);
*output for gis;
libname mls "../sas_dataset/";
libname gis "../data_source/GIS/";

data gis.for_gis_&byyear;
set mls.residentialproperty_&byyear
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
mls.industrial_&byyear;
if compress(upcase(CNY),' ') = 'COOK' | compress(upcase(CNY),' ') = '';
LN1 = LN*1;
LNG_X = LNG*1;
LAT_Y = LAT*1;
if LN = . then delete;
keep LN1 LAT_Y LNG_X;
run;

proc sort data=gis.for_gis_&byyear nodupkey; by LN1; run;
%mend;

%macro run_all();
%import_combine(AT, AttachedSingle);
%import_combine(BU, Business);
%import_combine(CO, MixedUse);
%import_combine(CommercialProperty, Commercial);
%import_combine(DE, DetachedSingle);
%import_combine(DP, DeededParking);
%import_combine(IN, Industrial);
%import_combine(LotsAndLand, LotsAndLand);
%import_combine(MF, MultiFamily);
%import_combine(MH, MobileHomes);
%import_combine(MU, TwoToFour);
%import_combine(OI, OfficeTech);
%import_combine(OT, InstutToDevelop);
%import_combine(RentalHome, Rentals);
%import_combine(ResidentialProperty, ResidentialProperty);
%import_combine(RN, ResidentialRental);
%import_combine(RS, RetailStores);
%import_combine(VL, VacantLand);

%if &year >= 2005 %then %do;
%exportGIS(&year);
%end;
%mend run_all;
%run_all();









/*
%macro tmp_import_all();
%do year = 2005 %to 2011;

%import_combine(AT, AttachedSingle);
%import_combine(BU, Business);
%import_combine(CO, MixedUse);
%import_combine(CommercialProperty, Commercial);
%import_combine(DE, DetachedSingle);
%import_combine(DP, DeededParking);
%import_combine(IN, Industrial);
%import_combine(LotsAndLand, LotsAndLand);
%import_combine(MF, MultiFamily);
%import_combine(MH, MobileHomes);
%import_combine(MU, TwoToFour);
%import_combine(OI, OfficeTech);
%import_combine(OT, InstutToDevelop);
%import_combine(RentalHome, Rentals);
%import_combine(ResidentialProperty, ResidentialProperty);
%import_combine(RN, ResidentialRental);
%import_combine(RS, RetailStores);
%import_combine(VL, VacantLand);

%exportGIS(&year);

%end;
%mend tmp_import_all;
%tmp_import_all();
*/
