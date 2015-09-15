option compress=yes;

libname librep '../report';
libname libin "../sas_dataset_final_cook";
libname libout "./source_for_report";
libname libgeo "/opt/data/datamain/GIS_CORE/GIS_CURRENT";

filename junk dummy;
proc printto log=junk;run;

DATA _NULL_;
   call symputx("currentYear",year(date()));
RUN;

data libout.cook_attachedsingle;
set libin.cook_attachedsingle_2004 - libin.cook_attachedsingle_&currentYear;
run; 
data libout.cook_business;
set libin.cook_business_2005 - libin.cook_business_&currentYear;
run;
data libout.cook_commercial;
set libin.cook_commercial_2005 - libin.cook_commercial_&currentYear;
run; 
data libout.cook_deededparking;
set libin.cook_deededparking_2004 - libin.cook_deededparking_&currentYear;
run; 
data libout.cook_detachedsingle;
set libin.cook_detachedsingle_2004 - libin.cook_detachedsingle_&currentYear;
run; 
data libout.cook_industrial;
set libin.cook_industrial_2005 - libin.cook_industrial_&currentYear;
run; 
data libout.cook_instuttodevelop;
set libin.cook_instuttodevelop_2005 - libin.cook_instuttodevelop_&currentYear;
run; 
data libout.cook_lotsandland;
set libin.cook_lotsandland_2005 - libin.cook_lotsandland_&currentYear;
run; 
data libout.cook_mixeduse;
set libin.cook_mixeduse_2005 - libin.cook_mixeduse_&currentYear;
run; 
data libout.cook_mobilehomes;
set libin.cook_mobilehomes_2004 - libin.cook_mobilehomes_&currentYear;
run; 
data libout.cook_multifamily;
set libin.cook_multifamily_2004 - libin.cook_multifamily_&currentYear;
run; 
data libout.cook_officetech;
set libin.cook_officetech_2005 - libin.cook_officetech_&currentYear;
run; 
data libout.cook_rentals;
set libin.cook_rentals_2004 - libin.cook_rentals_&currentYear;
run; 
data libout.cook_residentialproperty;
set libin.cook_residentialproperty_2005 - libin.cook_residentialproperty_&currentYear;
run; 
data libout.cook_residentialrental;
set libin.cook_residentialrental_2005 - libin.cook_residentialrental_&currentYear;
run; 
data libout.cook_retailstores;
set libin.cook_retailstores_2005 - libin.cook_retailstores_&currentYear;
run; 
data libout.cook_twotofour;
set libin.cook_twotofour_2004 - libin.cook_twotofour_&currentYear;
run; 
data libout.cook_vacantland;
set libin.cook_vacantland_2004 - libin.cook_vacantland_&currentYear;
run;
 
proc printto;run;
 


%macro data_report(type);

data &type.;
format pin1 14.;
length flag_pin $10.;
length st $30.;
set libout.cook_&type.(keep=ud ln st pin);
st=upcase(st);
pin1 = compress(pin,,"dk") * 1;
if pin1=88888888888888  | missing(pin1)| pin1=0  then flag_pin="npin";
else flag_pin="ypin";
delivery_date=substr(ud,1,10);
if st="BACK ON MARKET" then st="back_mkt";
if st="TEMP OFF MARKET" then st="off_mkt";
if st="TEMPORARILY NO SHOWINGS" then st="no_show";
if st="RE-ACTIVATED" then st="react";
if st="CANCELLED" then st="cancel";
if st="CONTINGENT" then st="ctg";
if st="PRICE CHANGE" then st="pchange";
run;
proc freq; tables st; run;
***OBS;
proc freq data = &type. noprint;  
tables delivery_date / norow nocol nopercent   
out = obscheck1(drop=PERCENT rename=(count=&type._obs)); run;

data obscheck1;
set obscheck1;
attrib _all_ label=' '; 
run;

***pin;

proc freq data = &type. noprint;  
tables flag_pin * delivery_date / norow nocol nopercent   
out = pincheck; run;

proc sort data=pincheck; by delivery_date; run;

proc transpose data=pincheck out=pincheck1(drop=_name_ _label_) prefix=&type._;
	by delivery_date;
	id flag_pin;
	var count;
run;




***GIS;
proc sort data=&type.(where=(flag_pin="ypin")) out=mlgb; by pin1; run;
proc sort data=libgeo.geo_master(keep=pin1) out=geo_master; by pin1; run;

data gis_mls_check;
merge mlgb(in=a) geo_master(in=b);
by pin1; 
gis_mls1=cats(a,b)*1;
if gis_mls1=11 then gis_mls="ygis"; else gis_mls="ngis";
if a=1;
run;

proc freq data = gis_mls_check noprint;  
tables gis_mls * delivery_date / norow nocol nopercent   
out = gischeck; run;

proc sort data=gischeck; by delivery_date; run;

proc transpose data=gischeck out=gischeck1(drop=drop=_name_ _label_) prefix=&type._;
	by delivery_date;
	id gis_mls;
	var count;
run;

***Trans;

proc freq data = &type.(where=(flag_pin="npin")) noprint;  
tables st * delivery_date / norow nocol nopercent   
out = tran_np_check; run;

proc sort data=tran_np_check; by delivery_date; run;

proc transpose data=tran_np_check out=tran_np_check1(drop=drop=_name_ _label_) prefix=&type._npin_;
	by delivery_date;
	id st;
	var count;
run;

proc freq data = gis_mls_check(where=(gis_mls="ygis")) noprint;  
tables st * delivery_date / norow nocol nopercent   
out = tran_yg_check; run;

proc sort data=tran_yg_check; by delivery_date; run;

proc transpose data=tran_yg_check out=tran_yg_check1(drop=drop=_name_ _label_) prefix=&type._ygis_;
	by delivery_date;
	id st;
	var count;
run;

proc freq data = gis_mls_check(where=(gis_mls="ngis")) noprint;  
tables st * delivery_date / norow nocol nopercent   
out = tran_ng_check; run;

proc sort data=tran_ng_check; by delivery_date; run;

proc transpose data=tran_ng_check out=tran_ng_check1(drop=drop=_name_ _label_) prefix=&type._ngis_;
	by delivery_date;
	id st;
	var count;
run;

proc sort data=obscheck1; by delivery_date; run;
proc sort data=pincheck1; by delivery_date; run;
proc sort data=tran_np_check1; by delivery_date; run;
proc sort data=tran_yg_check1; by delivery_date; run;
proc sort data=tran_ng_check1; by delivery_date; run;


data librep.&type._report;
retain report_date;
merge obscheck1 pincheck1 tran_np_check1 tran_yg_check1 tran_ng_check1;
by delivery_date; 
report_date=mdy(substr(delivery_date,6,2)*1,substr(delivery_date,9,2)*1,substr(delivery_date,1,4)*1);
format report_date mmddyy10.;
drop delivery_date;
run;
%mend;

filename ls_sas pipe "ls ./source_for_report/cook_*.sas7bdat";
data librep.sas_list;
infile ls_sas truncover;
input sas_name $100.;
format data_name $100.;
data_name = scan(scan(scan(sas_name,-1,"/"),1,'.'),2,'_');
if data_name="residentialrental" then delete;
run;
proc sort data=librep.sas_list nodupkey; by data_name; run;


data _null_;
set librep.sas_list;
call symput(cats("ds_in",_N_),data_name);
call symput("obs_num",_N_);
run;



%macro loop;
%do i=1 %to 17;
%data_report(&&ds_in&i);
%end;
%mend loop;
%loop;


data residentialrental;
retain report_date;
set libout.cook_residentialrental(keep=st ud ln);
delivery_date=substr(ud,1,10);
if st="Back on Market" then st="back_mkt";
if st="Temp Off Market" then st="off_mkt";
if st="Temporarily No Showings" then st="no_show";
if st="Re-activated" then st="react";
if st="Cancelled" then st="cancel";
if st="Contingent" then st="ctg";
if st="Price Change" then st="pchange";
report_date=mdy(substr(delivery_date,6,2)*1,substr(delivery_date,9,2)*1,substr(delivery_date,1,4)*1);
format report_date mmddyy10.;
drop delivery_date;
run;




proc freq data = residentialrental noprint;  
tables report_date / norow nocol nopercent   
out = residentialrental_report(drop=PERCENT rename=(count=residentialrental_obs)); run;

data librep.residentialrental_report;
set residentialrental_report;
attrib _all_ label=' '; 
run;



data librep.mls_report1;
merge 
librep.attachedsingle_report
librep.business_report
librep.commercial_report
librep.deededparking_report
librep.detachedsingle_report
librep.industrial_report
librep.instuttodevelop_report
librep.lotsandland_report
librep.mixeduse_report
librep.mobilehomes_report
librep.multifamily_report
librep.officetech_report
librep.rentals_report
librep.residentialproperty_report
librep.residentialrental_report
librep.retailstores_report
librep.twotofour_report
librep.vacantland_report
;
by report_date;
run;



%macro combine;

data try1;
set librep.mls_report1;
if ~missing(report_date); run;


data try2;
set try1;
tmp_date=mdy(1,1,year(report_date));
format tmp_date mmddyy10.;
tmp_week=week(report_date);
run;

data try3;
set try2;
date1=tmp_date+tmp_week*7;
format date1 mmddyy10.;
drop tmp_date tmp_week report_date;
run;

proc sort data=try3; by date1; run;


proc transpose data=try3 out=trying;
var _all_;
run;

data _null_;
set trying;
call symput(cats("var",_N_),_name_);
call symput("obs",compress(_N_));
run;
%put totalthings = &obs;
%put totalthings = &var1;

%macro lop;
%do i=1 %to &obs.;

proc summary data=try3; output out=lala&i
SUM=&&var&i;
by date1;
var &&var&i;
run;
%end;

data mls_report;
merge lala1-lala&obs.;
by date1;
run;

data mls_report;
set mls_report;
drop _type_ _freq_; 
if year(date1)>=2011;
run;

data librep.mls_report_final;
set mls_report;
rename date1=report_date;
run;


%mend lop;
%lop;


proc datasets library=libout;
delete
cook_attachedsingle
cook_business
cook_commercial
cook_deededparking
cook_detachedsingle
cook_industrial
cook_instuttodevelop
cook_lotsandland
cook_mixeduse
cook_mobilehomes
cook_multifamily
cook_officetech
cook_rentals
cook_residentialproperty
cook_residentialrental
cook_retailstores
cook_twotofour
cook_vacantland;
run;


%mend combine;
%combine;


%let reppath=/opt/data/datamain/MLS/report;
 x "%str(! /usr/local/bin/st &reppath/mls_report_final.sas7bdat &reppath/mls_report_final.xlsx -y )" ;
