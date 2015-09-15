/**********************************************************************
    Yearly change 
***********************************************************************/
%let year =  2011 ;

/*************************************/

option compress = yes;

*%LET CInput = MF;

%include "./importData.sas";

*%LET filepath = /home/datamain/MLS/;
%LET filepath = ../;
libname fout 	"&filepath.Format/";

%macro runImport(CInput);

filename indata pipe "ls &filepath.data_source/&CInput./";

data file_list;
length fname $50;
infile indata truncover; /* infile statement for file names */
input fname $50.; /* read the file names from the directory */
call symput ('numfile_List',_n_); /* store the record number in a macro variable */
run;

data file_list2;
set file_list;
fname2 = scan(fname,1,".");
Class_Name = scan(fname2,1,"_");
YEAR_IN = scan(fname2,2,"_")*1;
WEEK_IN = scan(fname2,3,"_")*1;
if YEAR_IN =&year ;
run;

proc sort; by Year_IN Week_IN; run;

data _null_;
set file_list2;
call symput ('numfile_List',_n_); /* store the record number in a macro variable */
run;

%macro importAll();
%do file1 = 1 %to &numfile_List;
data _null_;
	set file_list2;
	if _n_=&file1;
	call symput ('filein',fname);
	call symput ('fileout',fname2);
	call symput ('classin',Class_Name);
run;
%if &file1 = 1 %then %do;
	%importData(&classin, &classin, &filein,&fileout, Y);
%end;
%else %do;
	%importData(&classin, &classin, &filein,&fileout, N);
%end;

%end;
%mend;

%importAll();

%mend runImport;

%runImport(MF);
%runImport(MU);
%runImport(AT);
%runImport(DE);
%runImport(RentalHome);
%runImport(ResidentialProperty);
%runImport(BU);
%runImport(CO);
%runImport(CommercialProperty);
%runImport(DP);
%runImport(IN);
%runImport(LotsAndLand);
%runImport(MH);
%runImport(OI);
%runImport(OT);
%runImport(RN);
%runImport(RS);
%runImport(VL);
