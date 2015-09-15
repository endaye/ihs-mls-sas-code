libname mls "D:\Workspace\MLS\output";
option compress = yes;
%include "D:\Workspace\MLS\sas_code\mlsUtil.sas";

%LET ClassIN = MultiFamily;
%mlsMerge(mls, &ClassIN, &ClassIN, 2005, 2010);
data &ClassIN;
set &ClassIN;
format Property_Class $50.;
Property_Class = "&ClassIN";
keep LN LN1 PIN LD ST STD CCA COUNTY PUMA5 TRACT IHS_TRACT LP SP MUNI CIT chidum Property_Class;
run;

%LET ClassIN = TwoToFour;
%mlsMerge(mls, &ClassIN, &ClassIN, 2005, 2010);
data &ClassIN;
set &ClassIN;
format Property_Class $50.;
Property_Class = "&ClassIN";
keep LN LN1 PIN LD ST STD CCA COUNTY PUMA5 TRACT IHS_TRACT LP SP MUNI CIT chidum Property_Class;
run;

%LET ClassIN = AttachedSingle;
%mlsMerge(mls, &ClassIN, &ClassIN, 2005, 2010);
data &ClassIN;
set &ClassIN;
format Property_Class $50.;
Property_Class = "&ClassIN";
keep LN LN1 PIN LD ST STD CCA COUNTY PUMA5 TRACT IHS_TRACT LP SP MUNI CIT chidum Property_Class;
run;

%LET ClassIN = DetachedSingle;
%mlsMerge(mls, &ClassIN, &ClassIN, 2005, 2010);
data &ClassIN;
set &ClassIN;
format Property_Class $50.;
Property_Class = "&ClassIN";
keep LN LN1 PIN LD ST STD CCA COUNTY PUMA5 TRACT IHS_TRACT LP SP MUNI CIT chidum Property_Class;
run;

proc sql;
CREATE TABLE TEMP01 AS 
SELECT * FROM MultiFamily
OUTER UNION CORR
SELECT * FROM TwoToFour
OUTER UNION CORR
SELECT * FROM AttachedSingle
OUTER UNION CORR
SELECT * FROM DetachedSingle
;
quit;




data Temp02;
set Temp01;
CIT = UPCASE(CIT);
if TRACT*1 < 10000 then TRACT1 = TRACT*100; else TRACT1 = TRACT*1;
if CIT = "CHICAGO" then chidum = 1; else chidum = 0;
keep LN LN1 PIN LD ST STD CCA COUNTY PUMA5 TRACT1 IHS_TRACT LP SP MUNI CIT chidum  Property_Class;
run;

%reFormat(work, Temp02, Temp03);


proc freq; tables Property_Class; run;

%macro DateConvert(datain, dataout, vin, vout);
data &dataout;
set &datain;
tmpdate = scan(&vin,1,"T");
dayd = scan(tmpdate,3,"-")*1;
monthd = scan(tmpdate,2,"-")*1;
yeard = scan(tmpdate,1,"-")*1;
format &vout YYMMDD10.;
&vout = mdy(monthd, dayd, yeard);
&vout._YR = YEAR(&vout);
&vout._QR = QTR(&vout);
drop dayd monthd yeard tmpdate;
run;
%mend;
%DateConvert(Temp03, Temp04, LD, LD1);
%DateConvert(Temp04, Temp04, STD, STD1);

libname out "D:\Workspace\MLS\sas_code\";
/*data out.mls_residential_LD;
set Temp04;
run;
*/

data Temp05;
set out.mls_residential_LD;;
format PIN1 14.;
PIN1 = PIN*1;
if PIN1 = . then delete;
*if ST = "Closed";
run;

proc freq; tables ST; run;


proc sort; by PIN1; run;

libname forc "D:\Workspace\Foreclosure";
data forc01;
set forc.foreclosure_summary_w_pin;
if PIN1 = . then delete;
run;
proc sort nodupkey; by CASE_NUMBER; run;
proc sort; by PIN1; run;

data FORC_MLS_MERGE;
merge forc01(in = a) TEMP05(in = b);
by PIN1;
if a = 1 and b = 1 then ab = '11';
if a = 1 and b = 0 then ab = '10';
if a = 0 and b = 1 then ab = '01';
run;

proc freq; tables ab; run;

data MLS_ONLY;
set FORC_MLS_MERGE;
if ab = '01';
run;

data FORC_ONLY;
set FORC_MLS_MERGE;
if ab = '10';
run;

data FORC02;
set FORC_MLS_MERGE;
if ab = '11';
run;

data Forc03;
set Forc02;
if Filing_Date <= LD1 and Filing_Date + 540 > LD1 then do;
	FORC_SALE_DUMMY = 1;
	FILING_TO_LISTING = LD1 - FILING_DATE;
end;
else do;
	FORC_SALE_DUMMY = 0;
end;
run;

proc contents; run;
proc freq; tables ST; run;

data Forc04;
set Forc03;
if Forc_Sale_Dummy = 1;
run;

proc freq; tables ST; run;

data Forc05;
set Forc04;
if ST = "Closed" then LISTING_RANK = 1;
else if  ST = "Active" then LISTING_RANK = 2;
else if  ST = "Auction" then LISTING_RANK = 3;
else if  ST = "Contingent" then LISTING_RANK = 4;
else if  ST = "Contingent" then LISTING_RANK = 5;
else if  ST = "New" then LISTING_RANK = 6;
else if  ST = "Pending" then LISTING_RANK = 7;
else if  ST = "Expired" then LISTING_RANK = 8;
else if  ST = "Temp Off Market" then LISTING_RANK = 9;
else if  ST = "Price Change" then LISTING_RANK = 10;
else if  ST = "Re-activated" then LISTING_RANK = 11;
else if  ST = "Cancelled" then LISTING_RANK = 12;
else LISTING_RANK = 13;
run;

proc sort; by LISTING_RANK; run;
proc sort nodupkey; by CASE_NUMBER; run;

proc freq; tables ST; run;


data forc01;
set forc.foreclosure_summary_w_pin;
run;
proc sort nodupkey; by CASE_NUMBER; run;

data FORC_MLS_RE_MERGE;
merge forc01(in = a) forc05(in = b);
by CASE_NUMBER;
if a = 1 and b = 1 then ab = '11';
if a = 1 and b = 0 then ab = '10';
if a = 0 and b = 1 then ab = '01';
run;

proc freq; tables ab; run;

data forc.FORC_W_MLS;
set FORC_MLS_RE_MERGE;
drop ab LISTING_RANK;
run;


data MLS_W_FORC1;
set FORC_MLS_MERGE;
if ab = '11';
if Filing_Date <= LD1 and Filing_Date + 540 > LD1 then do;
	FORC_SALE_DUMMY = 1;
	FILING_TO_LISTING = LD1 - FILING_DATE;
end;
else do;
	FORC_SALE_DUMMY = 0;
end;
run;

data MLS_W_FORC2;
set MLS_W_FORC1;
if FORC_SALE_DUMMY = 1;
run;

proc sort nodupkey; by LN1; run;

proc sort data = TEMP05 out = TEMP06 nodupkey; by LN1; run;

data forc.MLS_W_FORC3;
merge MLS_W_FORC2(in = a) TEMP06(in = b);
by LN1;
if a ^= 1 then FORC_SALE_DUMMY = 0;
drop ab;
run;
