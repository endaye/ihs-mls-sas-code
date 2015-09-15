/****************************************************************************************/
      /* Yearly Changes */
%let beginyr =  2011 ;
%let endyr =  2011 ;
/****************************************************************************************/

option compress = yes;

*%LET CInput = MF;
*%LET COutput = MultiFamily;

*%LET filepath = /home/datamain/MLS/;
%LET filepath = ../;

%macro runImport(CInput, COutput);

libname fout 	"&filepath.sas_dataset/";
libname fin	"&filepath.sas_dataset/&CInput/";

filename indata pipe "ls ../sas_dataset/&CInput/";
data file_list;
length fname $50;
infile indata truncover; /* infile statement for file names */
input fname $50.; /* read the file names from the directory */
run;

%macro importAll(YStart, YStop);

data file_list2;
set file_list;
fname2 = scan(fname,1,".");
Class_Name = scan(fname2,1,"_");
YEAR_IN = scan(fname2,2,"_")*1;
WEEK_IN = scan(fname2,3,"_")*1;
if &YStart <= YEAR_IN and &YStop >= YEAR_IN;
run;

data _null_;
set file_list2;
call symput ('numfile_List',_n_); /* store the record number in a macro variable */
run;

proc sort; by Year_IN Week_IN; run;

%do file1 = 1 %to &numfile_List;
data _null_;
	set file_list2;
	if _n_=&file1;
	call symput ('filein',fname);
	call symput ('fileout',fname2);
	call symput ('classin',Class_Name);
run;
%if &file1 = 1 %then %do;
	proc sql;
	     CREATE TABLE fout.&COutput._&YStop as
	     SELECT * FROM fin.&fileout
	     ;
	quit;
	run;
%end;
%else %do;
	proc sql;
	     CREATE TABLE TEMP01 as
	     SELECT * FROM fout.&COutput._&YStop
	     OUTER UNION CORR
	     SELECT * FROM fin.&fileout
	     ;
	quit;
	run;

	data fout.&COutput._&YStop;
	set TEMP01;
	run;
%end;

%end;
%mend;

*%importAll(2004,2005);
*%importAll(2006,2006);
*%importAll(2007,2007);
*%importAll(2008,2008);
*%importAll(2009,2009);
*%importAll(2010,2010);
%importAll(&beginyr,&endyr);

%mend runImport;


%runImport(ResidentialProperty,ResidentialProperty);
%runImport(RentalHome,Rentals);
%runImport(LotsAndLand,LotsAndLand);
%runImport(CommercialProperty,Commercial);
%runImport(DE,DetachedSingle);
%runImport(AT,AttachedSingle);
%runImport(MH,MobileHomes);
%runImport(MU,TwoToFour);
%runImport(RN,ResidentialRental);
%runImport(DP,DeededParking);
%runImport(VL,VacantLand);
%runImport(MF,MultiFamily);
%runImport(OI,OfficeTech);
%runImport(BU,Business);
%runImport(CO,MixedUse);
%runImport(RS,RetailStores);
%runImport(OT,InstutToDevelop);
%runImport(IN,Industrial);
