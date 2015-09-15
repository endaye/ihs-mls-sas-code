*******************************;
*        Yearly Change         ;
*******************************;
%let year = 2011;
*******************************;

option compress=yes;

%include "./importData.sas";
%let filepath = ../ ;
libname fout "&filepath.Format/";
libname folder "./";

%macro import_combine(dstype, dtype_out);
*import txt into sas datasets;

filename dslst pipe "ls &filepath.data_source/&dstype./";

data data_list;
infile dslst truncover;
input filein $100.;
run;
data data_list;
set data_list;
*format fileout $100. class_name $50. year_in best12. week_in best12.;
format fileout $100. year_in best12. ;
fileout = scan(filein,1,".");
*class_name = scan(fileout,1,"_");
year_in = scan(fileout,2,"_")*1;
*week_in = scan(fileout,3,"_")*1;
if year_in = &year ;
run;

%let dsnum = %eval(0);
data _NULL_;
set data_list;
call symputx("dsnum",max(_N_,0));
*call symput("classin",class_name);
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
%if &year >= 2005 %then %do;

	libname sasin "&filepath.sas_dataset/&dstype./";
	libname sasout "&filepath.sas_dataset/";

	filename datals pipe "ls &filepath.sas_dataset/&dstype./*";
	data data_list2;
	infile datals truncover;
	input sasfile $100.;
	format fname $100.;
	fname = scan(scan(sasfile,-1,'/'), 1, '.');
	%if &year = 2005 %then %do;
		if scan(fname,2,'_')='2005' | scan(fname,2,'_')='2004';
	%end;
	%if &year > 2005 %then %do;
		if scan(fname,2,'_')=&year;
	%end;
	run;

	data _NULL_;
	set data_list2;
	call symputx("filenum", _N_);
	call symput(cats("file",_N_), fname);
	run;

	%put filenum = &filenum;

	%do q = 1 %to &filenum;
	%put &&file&q;
	data tmp_ds&q; set sasin.&&file&q; run;
	proc contents data=tmp_ds&q varnum noprint out=tmp_ctnt&q; run;
	data tmp_ctnt&q; set tmp_ctnt&q; if type=2; keep name length; rename length=length&q; run;
	proc sort data=tmp_ctnt&q; by name; run;
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

	data sasout.&dtype_out._&year;
	%do j = 1 %to &max_var;
	length &&var&j $ &&len&j;
	format &&var&j &&fmt&j;
	%end;
	set tmp_ds1 - tmp_ds&filenum;
	run;

%end;
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
