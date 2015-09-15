option compress=yes;

libname oldin "../MLS_old/historic_pattern_cleaned/";
libname newin "../MLS_old/output1/";
libname cookout "../historic_clean/cook/";
libname otherout "../historic_clean/other/";
libname folder "./";

%macro hist_clean(dstype, yr_start, yr_end);
%do i = &yr_start %to &yr_end;
	
	***select data source, old pattern for 2004-2008 eight types, new pattern for else;
	%if &i <= 2008 & 
	(&dstype=AttachedSingle | &dstype=DeededParking | &dstype=DetachedSingle | &dstype=MobileHomes
	| &dstype=MultiFamily | &dstype=Rentals | &dstype=TwoToFour | &dstype=VacantLand) 
	%then %do;
		data ds_1 otherout.other_&dstype._&i;
		set oldin.&dstype._final_&i;
		if compress(upcase(CNY), ' ') = 'COOK' | (compress(upcase(CNY),' ')='' & COUNTY='031') then output ds_1;
		else output otherout.other_&dstype._&i;
		run;
	%end;
	%else %do;
		data ds_1;
		set newin.&dstype._&i;
		run;
	%end;


	***date reformat;
	proc contents data=ds_1 noprint out=mls_ctnt; run;

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

	data ds_2;
	set ds_1;
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
	drop tmp_mth tmp_day tmp_yr LD1_QR LD1_YR STD1_QR STD1_YR UD1_QR UD1_YR YEARD YEARTXT;
	run;
	***;

	***max length reformat;
	proc contents data=ds_2 noprint out=mls_ctnt1; run;

	data var_str; set mls_ctnt1; if type=2; run;
	data _NULL_;
	set var_str;
	call symputx('svarnum', _N_);
	call symput(cats('svar',_N_), compress(name));
	run;
	
	data tmp_1;
	set ds_2;
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

	data ds_3;
	%do n = 1 %to &svarnum;
	length &&svar&n $ &&maxlen&n;
	format &&svar&n &&fmt&n;
	informat &&svar&n &&fmt&n;
	%end;
	set ds_2;
	run;
	***;

	***upper case reformat;
	data ds_4;
	set ds_3;
	%do p = 1 %to &svarnum;
		if compress(&&svar&p,'abcdefghijklmnopqrstuvwxyz','k')^='' then &&svar&p = upcase( &&svar&p );
	%end;
	run;
	***;

	proc sort data=ds_4 nodupkey; by LN1 UD1 ST; run;

	data cookout.&dstype._&i;
	set ds_4;
	run;

%end;
%mend hist_clean;
/*
%hist_clean(AttachedSingle, 2004, 2011);
%hist_clean(Business, 2005, 2011);
%hist_clean(MixedUse, 2005, 2011);
%hist_clean(Commercial, 2005, 2011);
%hist_clean(DetachedSingle, 2004, 2011);
%hist_clean(DeededParking, 2004, 2011);
%hist_clean(Industrial, 2005, 2011);
%hist_clean(LotsAndLand, 2005, 2011);
%hist_clean(MultiFamily, 2004, 2011);
%hist_clean(MobileHomes, 2004, 2011);
%hist_clean(TwoToFour, 2004, 2011);
%hist_clean(OfficeTech, 2005, 2011);
%hist_clean(InstutToDevelop, 2005, 2011);
%hist_clean(Rentals, 2004, 2011);
%hist_clean(ResidentialProperty, 2005, 2011);
%hist_clean(ResidentialRental, 2005, 2011);
%hist_clean(RetailStores, 2005, 2011);
%hist_clean(VacantLand, 2004, 2011);
*/


libname final_c "../sas_dataset_final_cook/";
libname daily_c "../output/";
%macro mv_combine(dstype, yr_start, yr_end);
%do i = &yr_start %to &yr_end;
	%if &i < 2011 %then %do;
		data final_c.cook_&dstype._&i;
		set cookout.&dstype._&i;
		run;
	%end;
	%if &i = 2011 %then %do;
		data tmp_ds1; set cookout.&dstype._&i; run;
		data tmp_ds2; set daily_c.&dstype._&i; run;

		proc contents data=tmp_ds1 varnum noprint out=tmp_ctnt1; run;
		proc contents data=tmp_ds2 varnum noprint out=tmp_ctnt2; run;

		data tmp_ctnt1; set tmp_ctnt1; if type=2; 
		keep name length; rename length=length1;
		run;
		proc sort data=tmp_ctnt1; by name; run;

		data tmp_ctnt2; set tmp_ctnt2; if type=2;
		keep name length; rename length=length2;
		run;
		proc sort data=tmp_ctnt2; by name; run;

		data tmp_ctnt_all;
		merge tmp_ctnt1(in=a) tmp_ctnt2(in=b);
		by name;
		if a=1 & b=1;
		max_len = max(length1, length2);
		run;

		data _NULL_;
		set tmp_ctnt_all;
		call symput(cats('var',_N_), name);
		call symputx(cats('len',_N_), max_len);
		call symput(cats('fmt',_N_), cats('$',max_len,'.'));
		call symputx('max_var', _N_);
		run;

		data tmp_ds_all;
		%do j = 1 %to &max_var;
		length &&var&j $ &&len&j;
		format &&var&j &&fmt&j;
		%end;
		set tmp_ds1 tmp_ds2;
		run;

		proc sort data=tmp_ds_all nodupkey; by LN1 UD1 ST; run;
		data final_c.cook_&dstype._&i; set tmp_ds_all; run;
	%end;
	%if &i > 2011 %then %do;
		data final_c.cook_&dstype._&i;
		set daily_c.&dstype._&i;
		run;
	%end;
%end;
%mend mv_combine;
/*
%mv_combine(AttachedSingle, 2004, 2011);
%mv_combine(Business, 2005, 2011);
%mv_combine(MixedUse, 2005, 2011);
%mv_combine(Commercial, 2005, 2011);
%mv_combine(DetachedSingle, 2004, 2011);
%mv_combine(DeededParking, 2004, 2011);
%mv_combine(Industrial, 2005, 2011);
%mv_combine(LotsAndLand, 2005, 2011);
%mv_combine(MultiFamily, 2004, 2011);
%mv_combine(MobileHomes, 2004, 2011);
%mv_combine(TwoToFour, 2004, 2011);
%mv_combine(OfficeTech, 2005, 2011);
%mv_combine(InstutToDevelop, 2005, 2011);
%mv_combine(Rentals, 2004, 2011);
%mv_combine(ResidentialProperty, 2005, 2011);
%mv_combine(ResidentialRental, 2005, 2011);
%mv_combine(RetailStores, 2005, 2011);
%mv_combine(VacantLand, 2004, 2011);
*/


%macro run_all();
data yr_get;
set folder.update_list;
if new_add = 1;
run;

proc sort data=yr_get nodupkey; by ds_year; run;

data _NULL_;
set yr_get;
call symputx("endyear", ds_year);
if _N_ = 1 then do;
	call symputx("startyear", ds_year);
end;
run;

%put startyear = &startyear ;
%put endyear = &endyear ;

%mv_combine(AttachedSingle, &startyear., &endyear.);
%mv_combine(Business, &startyear., &endyear.);
%mv_combine(MixedUse, &startyear., &endyear.);
%mv_combine(Commercial, &startyear., &endyear.);
%mv_combine(DetachedSingle, &startyear., &endyear.);
%mv_combine(DeededParking, &startyear., &endyear.);
%mv_combine(Industrial, &startyear., &endyear.);
%mv_combine(LotsAndLand, &startyear., &endyear.);
%mv_combine(MultiFamily, &startyear., &endyear.);
%mv_combine(MobileHomes, &startyear., &endyear.);
%mv_combine(TwoToFour, &startyear., &endyear.);
%mv_combine(OfficeTech, &startyear., &endyear.);
%mv_combine(InstutToDevelop, &startyear., &endyear.);
%mv_combine(Rentals, &startyear., &endyear.);
%mv_combine(ResidentialProperty, &startyear., &endyear.);
%mv_combine(ResidentialRental, &startyear., &endyear.);
%mv_combine(RetailStores, &startyear., &endyear.);
%mv_combine(VacantLand, &startyear., &endyear.);

%mend run_all;
%run_all();
