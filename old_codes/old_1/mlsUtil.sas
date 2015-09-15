%macro mlsMerge(base, ClassIN, dataout, yBegin, yEnd);

proc sql;
	CREATE TABLE &dataout as
	SELECT * FROM &base..&ClassIN._&yBegin
quit;


%do i = &yBegin + 1 %to &yEnd;
proc sql;
	CREATE TABLE MACRO_TEMP01 as
	SELECT * FROM &dataout
	OUTER UNION CORR
	SELECT * FROM &base..&ClassIN._&i
;
quit;

data &dataout;
set MACRO_TEMP01;
run;

%end;
proc datasets library = work nolist;
delete MACRO_TEMP01;
quit; run;
%mend;


%macro reFormat(base, datain, dataout);

data dsn;
set sashelp.vcolumn(where=(libname=upcase("&base") and memname=upcase("&datain") and type='char'));
call symput(cats("var",_n_),name);
id=_n_;
keep name id;
run;

*Counting number of character variables in the dataset;
%global nvars;
proc sql noprint;
select count(*)into :nvars from dsn;
quit;

*Computing max. length of each character variable in the dataset;
%do i = 1 %to &nvars;
proc sql noprint;
create table maxlen&i as
select max(length(&&var&i)) as mlen from &base..&datain
quit;
%end;

*Concatenating all the datasets;
data final;
id=_n_;
set %do i= 1 %to &nvars;
maxlen&i
%end;
;
run;



proc sort data=dsn;by id;run;
proc sort data=final;by id;run;

data check;
merge dsn final;
by id;
drop id;
run;

proc sort; by name; run;

%let j = %EVAL(&nvars);
proc datasets library = work nolist;
    delete maxlen1-maxlen&j dsn final;
quit;


data check;
set check;
		if mlen = 1 then mlen = 1;
		else if mlen < 5 then mlen = mlen +2;
		else if mlen < 10 then mlen = mlen +3;
		else if mlen < 50 then mlen = mlen +10;
		else if mlen < 200 then mlen = mlen +30;
		else mlen = 384;
		
		informat var_type $1.;
		informat var_val $50.;
		var_type = "C";
		var_val = CATS("$",put(mlen, 3.), ".");
run;

data _null_;
set check;
call symput('num_vars', _n_);
run;


data MACRO_TEMP01;
set &datain;
run;


%do i = 1 %to &num_vars;

data _null_;
set check;
if _n_ = &i;
	call symput('var_in',trim(name));
	call symput('fvar',trim(var_val));
run;

data MACRO_TEMP01;
set MACRO_TEMP01;
&var_in._ = &var_in;
drop &var_in;
run;

data MACRO_TEMP01;
set MACRO_TEMP01;
format &var_in &fvar;
&var_in = &var_in._;
drop &var_in._;
run;

%end;

data &dataout;
set MACRO_TEMP01;
run;

proc datasets library = work nolist;
delete MACRO_TEMP01;
quit; run;
%mend;

