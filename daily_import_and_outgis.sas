option compress=yes;

%include "./importData.sas";
%let filepath = ../ ;
libname fout "&filepath.Format/";
libname folder "./";



***get update history data list***;
data update_old; set folder.update_list; run;
proc sort data=update_old nodupkey; by ds_date; run;
data _NULL_;
set update_old;
call symput('last_in', compress(ds_date));
run;

filename update pipe "ls &filepath.data_source/*/*.mls";
data update_new;
infile update truncover;
input filein $200.;
format ds_date $8. ds_year $4. ds_mth $2. ds_day $2.;
ds_date = compress(scan(scan(filein, -1, '_'),1,'.'), ' ');
ds_year = substr(ds_date,1,4);
ds_mth = substr(ds_date,5,2);
ds_day = substr(ds_date,7,2);
keep ds_date ds_year ds_mth ds_day;
run;
proc sort data=update_new nodupkey; by ds_date; run;

data _NULL_;
set update_new;
call symput('new_in', compress(ds_date));
run;


data folder.update_list_bk; set folder.update_list; run;

*out new update list;
data folder.update_list;
merge update_old(in=a) update_new(in=b);
by ds_date;
if a=1 then new_add=0;
if a=0 then new_add=1;
run;

data new_add;
set folder.update_list;
if new_add=1;
run;

/*
data new_add;
set update_new;
run;

*out new update list;
data folder.update_list;
set update_new;
new_add=1;
run;
*/
***DONE***;



***daily data import and combine***;
libname sas_out "&filepath.sas_dataset/";

%macro import_combine(dstype, dtype_out);

*import txt into sas datasets;

data data_list;
set new_add;
format filein $100. fileout $100.;
filein = cats("&dstype",'_',ds_date,'.mls');
fileout = cats("&dstype",'_',ds_date);
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
	%importData(&dstype, &dstype, &&dsin&p, &&dsout&p, N);
%end;
%end;



*combine sas datasets for new updated year;

libname sas_in "&filepath.sas_dataset/&dstype./";

proc sort data=data_list(keep = ds_year) out=year_list nodupkey; by ds_year; run;
data _NULL_;
set year_list;
call symputx("yrnum", _N_);
call symput(cats("yr",_N_), ds_year);
run;

data _NULL_;
set update_old;
call symputx("yrold", ds_year);
run;

%do q = 1 %to &yrnum;
	data data_list2;
	***;
	*set folder.update_list;
	set data_list;
	***;
	if ds_year = &&yr&q ;
	format filein $100. fileout $100.;
	***;
	filein = cats("&dstype",'_',ds_date,'.mls');
	fileout = cats("&dstype",'_',ds_date);
	***;
	run;

	data _NULL_;
	set data_list2;
	call symputx("filenum", _N_);
	call symput(cats("file",_N_), compress(fileout));
	run;

	%put year = &&yr&q;
	%put filenum = &filenum;

	%do r = 1 %to &filenum;
		%put file = &&file&r;
		data tmp_ds&r; set sas_in.&&file&r; run;
		proc contents data=tmp_ds&r varnum noprint out=tmp_ctnt&r; run;
		data tmp_ctnt&r; set tmp_ctnt&r; if type=2; keep name length; rename length=length&r; run;
		proc sort data=tmp_ctnt&r; by name; run;
	%end;

	%if &&yr&q <= &yrold %then %do;
	***;
	data tmp_ds0; set sas_out.all_&dtype_out._&&yr&q; run;
	proc contents data=tmp_ds0 varnum noprint out=tmp_ctnt0; run;
	data tmp_ctnt0; set tmp_ctnt0; if type=2; keep name length; rename length=length0; run;
	proc sort data=tmp_ctnt0; by name; run;
	***;
	%end;

	data tmp_ctnt_all;
	%if &&yr&q <= &yrold %then %do;
	merge tmp_ctnt0 - tmp_ctnt&filenum;
	%end;
	%if &&yr&q > &yrold %then %do;
	merge tmp_ctnt1 - tmp_ctnt&filenum;
	%end;
	by name;
	max_len = max(of length0 - length&filenum);
	*max_len = max(of length1 - length&filenum);
	run;
	
	data _NULL_;
	set tmp_ctnt_all;
	call symput(cats('var',_N_), name);
	call symputx(cats('len',_N_), max_len);
	call symput(cats('fmt',_N_), cats('$',max_len,'.'));
	call symputx('max_var', _N_);
	run;
	
	***;
	*data sas_out.all_&dtype_out._&&yr&q.._bk;
	*set sas_out.all_&dtype_out._&&yr&q;
	*run;
	***;

	*output data;
	data sas_out.all_&dtype_out._&&yr&q;
	%do i = 1 %to &max_var;
	length &&var&i $ &&len&i;
	format &&var&i &&fmt&i;
	%end;
	%if &&yr&q <= &yrold %then %do;
	set tmp_ds0 - tmp_ds&filenum;
	%end;
	%if &&yr&q > &yrold %then %do;
	set tmp_ds1 - tmp_ds&filenum;
	%end;
	run;

	proc sort data=sas_out.all_&dtype_out._&&yr&q nodupkey; by ln ud st; run;

%end;

%mend import_combine;
*%import_combine(ResidentialProperty, ResidentialProperty);
*%import_combine(AT, AttachedSingle);


%macro exportGIS();
*output for gis;
libname gistxt "&filepath./data_source/GIS/";
libname gist_tmp "&filepath./data_source/GIS/Temp/";
libname gissas "&filepath./sas_dataset/GIS/";
libname giss_tmp "&filepath./sas_dataset/GIS/Temp/";

proc sort data=gissas.gis_all(keep=ln1) out=old_gis nodupkey; by LN1; run;

proc sort data=new_add(keep = ds_year) out=year_list nodupkey; by ds_year; run;
data _NULL_;
set year_list;
call symputx("yrnum", _N_);
call symput(cats("yr",_N_), ds_year);
run;

%do q = 1 %to &yrnum;
	data mls_yr&q;
	set 
	sas_out.all_residentialproperty_&&yr&q
	sas_out.all_rentals_&&yr&q
	sas_out.all_lotsandland_&&yr&q
	sas_out.all_commercial_&&yr&q
	sas_out.all_detachedsingle_&&yr&q
	sas_out.all_attachedsingle_&&yr&q
	sas_out.all_mobilehomes_&&yr&q
	sas_out.all_twotofour_&&yr&q
	sas_out.all_residentialrental_&&yr&q
	sas_out.all_deededparking_&&yr&q
	sas_out.all_vacantland_&&yr&q
	sas_out.all_multifamily_&&yr&q
	sas_out.all_officetech_&&yr&q
	sas_out.all_business_&&yr&q
	sas_out.all_mixeduse_&&yr&q
	sas_out.all_retailstores_&&yr&q
	sas_out.all_instuttodevelop_&&yr&q
	sas_out.all_industrial_&&yr&q
	;
	if compress(upcase(CNY),' ') = 'COOK' | compress(upcase(CNY),' ') = '';
	LN1 = LN*1;
	LNG_X = LNG*1;
	LAT_Y = LAT*1;
	if LN = . then delete;
	keep LN1 LAT_Y LNG_X;
	run;
%end;

data mls_all_newyr;
set mls_yr1 - mls_yr&yrnum;
run;
proc sort data=mls_all_newyr nodupkey; by LN1; run;

data gist_tmp.for_gis_&new_in;
merge mls_all_newyr(in=a) old_gis(in=b);
by ln1;
if a=1 & b=0;
run;

******;
* GIS part changed from ArcMap to XY_GIS sas dataset matching, 05Apr2012;
data _NULL_;
*call system("rm &filepath./data_source/GIS/mls_for_gis_new.dbf");
*call system("st &filepath./data_source/GIS/Temp/for_gis_&new_in..sas7bdat &filepath./data_source/GIS/mls_for_gis_new.dbf");
call system("rm &filepath./data_source/GIS/mls_for_gis_new.sas7bdat");
call system("cp &filepath./data_source/GIS/Temp/for_gis_&new_in..sas7bdat &filepath./data_source/GIS/mls_for_gis_new.sas7bdat");
run;
******;

%mend exportGIS;
*%exportGIS();


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

%exportGIS();

%mend run_all;
%run_all();


