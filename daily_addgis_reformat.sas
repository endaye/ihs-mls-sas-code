option compress = yes;

%include "./importData.sas";
%let filepath = ../ ;
libname folder "./";

libname fout "&filepath.Format/";
libname gis "&filepath.sas_dataset/GIS/";
libname gis_tmp "&filepath.sas_dataset/GIS/Temp/";
libname gis_in "&filepath.data_source/GIS/";

libname mlsin "&filepath.sas_dataset/";
libname mlsout "&filepath.output/";
libname other "&filepath.output/other/";
libname sh "/opt/data/datamain/MLS/sas_dataset_final_other";


data new_add;
set folder.update_list;
if new_add = 1;
call symput('new_in', compress(ds_date));
run;

proc sort data=new_add(keep = ds_year) out=year_list nodupkey; by ds_year; run;
data _NULL_;
set year_list;
call symputx("yrnum", _N_);
call symput(cats("yr",_N_), ds_year);
run;


*import new gis data and combine to gis_all;
%macro gis_import();

******;
* GIS part changed from ArcMap to XY_GIS sas dataset matching, 05Apr2012;

*%importData(GIS, GIS, gis_new_done.txt, gis_new_done, N);

*data gis_tmp.gis_&new_in; 
*set gis.gis_new_done(rename=(ln1=ln));
*ln1 = ln*1;
*keep LN1 CCA COMMUNITY COUNTY CongsnDist IHS_Tract LSAD_TRANS Muni MuniCCA PUMA5 TRACT TownRgn Township TractNm_No ;
*run;

%include '/opt/archive/datamain/XY_GIS/xy_gis_add_macro.sas';
%add_gis(gis_in.mls_for_gis_new, LNG_X, LAT_Y, gis.gis_new_done);

data gis_tmp.gis_&new_in;
set gis.gis_new_done;
COUNTY='031';
keep ln1 COMMUNITY COUNTY CongsnDist LSAD_TRANS Muni MuniCCA PUMA5 TRACT TownRgn Township;
run;
******;

data gis.gis_all_bk; set gis.gis_all; run;
data gis_all; set gis.gis_all gis_tmp.gis_&new_in; run;

proc sort data=gis_all out=gis.gis_all nodupkey; by ln1; run;

%mend gis_import;
*%gis_import();


%macro add_gis_refmt(mlsclass);

%do i = 1 %to &yrnum;
	data mls;
	set mlsin.all_&mlsclass._&&yr&i;
	LN1 = LN*1;
	*if compress(upcase(CNY),' ') = 'COOK' | compress(upcase(CNY),' ') = '';
	run;

	proc sort data=mls; by LN1; run;

	data mls_gis;
	merge mls(in=a) gis.gis_all(in=b);
	by LN1;
	if a=0 then delete;
	run;

	data other.other_&mlsclass._&&yr&i;
	set mls_gis;
	*if a=1 & b=0 & compress(upcase(CNY),' ')='' then delete;
	if compress(upcase(CNY),' ')='COOK' | (compress(upcase(CNY),' ')='' & b=1) then delete;
	drop 
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
	TractNm_No ;
	run;


	proc contents data=mls_gis noprint out=mls_ctnt(keep=name type); run;
	
	***date refomat;
	data var_date;
	set mls_ctnt;
	if upcase(name)='AUCTION_DATE' | upcase(name)='BMD' | upcase(name)='CLOSEDDATE' | upcase(name)='CONTRACTDATE' 
	| upcase(name)='LD' | upcase(name)='LDR' | upcase(name)='OD' | upcase(name)='OMD' 
	| upcase(name)='PHOTODATE' | upcase(name)='RECORDMODDATE' | upcase(name)='STD' | upcase(name)='UD' 
	| upcase(name)='XD' | upcase(name)='VTDATE' | upcase(name)='AVAILABLE_DATE';
	format dvar_out $50.;
	dvar_out = cats(Name,'1');
	run;

	data _NULL_;
	set var_date;
	call symputx('dvarnum', _N_);
	call symput(cats('dvarin',_N_), name);
	call symput(cats('dvarout',_N_), dvar_out);
	run;

	data mls_cln1;
	set mls_gis;
	%do j = 1 %to &dvarnum;
		if length(&&dvarin&j) = 6 then do;
			tmp_mth = substr(&&dvarin&j ,1,2)*1;
			tmp_day = substr(&&dvarin&j ,3,2)*1;
			tmp_yr = substr(&&dvarin&j ,5,2)*1;
			if tmp_yr <= 30 then tmp_yr = tmp_yr+2000;
			else if tmp_yr > 30 then tmp_yr = tmp_yr+1900;
		end;
		if length(&&dvarin&j) > 6 then do;
			tmp_yr = substr(&&dvarin&j ,1,4)*1;
			tmp_mth = substr(&&dvarin&j ,6,2)*1;
			tmp_day = substr(&&dvarin&j ,9,2)*1;
		end;
		format &&dvarout&j yymmdd10.;
		&&dvarout&j = mdy(tmp_mth, tmp_day, tmp_yr);
	%end;
	drop tmp_mth tmp_day tmp_yr;
	run;
	***;

	data var_str; set mls_ctnt; if type=2; run;
	data _NULL_;
	set var_str;
	call symputx('svarnum', _N_);
	call symput(cats('svar',_N_), compress(name));
	run;
	
	***max length reformat;
	data tmp_1;
	set mls_cln1;
	%do k = 1 %to &svarnum;
		len&k = length(&&svar&k);
	%end;
	keep len1 - len&svarnum;
	run;

	proc means data=tmp_1 noprint;
	output out=tmp_2 max(len1-len&svarnum)=maxlen1-maxlen&svarnum;
	run;

	%do m = 1 %to &svarnum;
		%if &m = 1 %then %do;
			data tmp_3; set tmp_2; keep maxlen&m; rename maxlen&m = maxlen; run;
		%end;
		%else %do;
			data tmp_3; set tmp_3 tmp_2(keep=maxlen&m rename=(maxlen&m=maxlen)); run;
		%end;
	%end;

	data _NULL_;
	set tmp_3;
	format fmt $10.;
	fmt = cats("$",maxlen,".");
	call symputx(cats("maxlen",_N_), maxlen);
	call symput(cats("fmt",_N_), fmt);
	run;

	data mls_cln2;
	%do n = 1 %to &svarnum;
	length &&svar&n $ &&maxlen&n;
	format &&svar&n &&fmt&n;
	informat &&svar&n &&fmt&n;
	%end;
	set mls_cln1;
	run;
	***;

	***upper case reformat;
	data mls_cln3;
	set mls_cln2;
	%do p = 1 %to &svarnum;
		if compress(&&svar&p,'abcdefghijklmnopqrstuvwxyz','k')^='' then &&svar&p = upcase( &&svar&p );
	%end;
	run;
	***;

	data mlsout.&mlsclass._&&yr&i sh.other_&mlsclass._&&yr&i;
	set mls_cln3;
	if compress(upcase(CNY),' ')='COOK' | (compress(upcase(CNY),' ')='' & b=1) then output 
mlsout.&mlsclass._&&yr&i;
	else output sh.other_&mlsclass._&&yr&i;
	run;
%end;

%mend add_gis_refmt;



%macro run_all();
%gis_import();

%add_gis_refmt(AttachedSingle);
%add_gis_refmt(Business);
%add_gis_refmt(MixedUse);
%add_gis_refmt(Commercial);
%add_gis_refmt(DetachedSingle);
%add_gis_refmt(DeededParking);
%add_gis_refmt(Industrial);
%add_gis_refmt(LotsAndLand);
%add_gis_refmt(MultiFamily);
%add_gis_refmt(MobileHomes);
%add_gis_refmt(TwoToFour);
%add_gis_refmt(OfficeTech);
%add_gis_refmt(InstutToDevelop);
%add_gis_refmt(Rentals);
%add_gis_refmt(ResidentialProperty);
%add_gis_refmt(ResidentialRental);
%add_gis_refmt(RetailStores);
%add_gis_refmt(VacantLand);
%mend run_all;
%run_all();

