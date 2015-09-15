option compress=yes;
libname lib '/opt/data/datamain/MLS/report';

data lib.mls_report_res_final;
retain report_date;
set lib.mls_report_final;
keep report_date attachedsingle_obs attachedsingle_ypin attachedsingle_npin 
	detachedsingle_obs detachedsingle_ypin detachedsingle_npin 
	multifamily_obs multifamily_ypin multifamily_npin 
	rentals_obs rentals_ypin rentals_npin 
	twotofour_obs twotofour_ypin twotofour_npin 
	;
run;

%let reppath=/opt/data/datamain/MLS/report;
 x "%str(! /usr/local/bin/st &reppath/mls_report_res_final.sas7bdat &reppath/mls_report_res_final.xlsx -y )" ;
