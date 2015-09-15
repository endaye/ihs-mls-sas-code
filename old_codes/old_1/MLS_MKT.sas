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


%macro getAval(datain, dataout, byyear);

data &dataout;
set &datain;
ARRAY AVAL(4) AVAL&byyear.Q1-AVAL&byyear.Q4;
DO I = 1 TO 4;
	if YYQ(year(LD1),QTR(LD1)) <= YYQ(&byyear, I) then do;
		if STD1 < YYQ(&byyear, I) then do;
				if 	   ST = "Cancelled"
					OR ST = "Closed"
					OR ST = "Expired"
					OR ST = "Temp Off Market"
					then AVAL(I) = 0;
				else AVAL(I) = 1;
		end;
		else AVAL(I) = 1;
	end;
	else AVAL(I) = 0;
END;
DROP I;

run;

%mend;

%getAval(Temp04, Temp06, 2005);
%getAval(Temp06, Temp06, 2006);
%getAval(Temp06, Temp06, 2007);
%getAval(Temp06, Temp06, 2008);
%getAval(Temp06, Temp06, 2009);
%getAval(Temp06, Temp06, 2010);


%macro getLP(datain, dataout, byyear);

data &dataout;
set &datain;
ARRAY AVAL(4) AVAL&byyear.Q1-AVAL&byyear.Q4;
ARRAY SUM_LP(4) SUM_LP_&byyear.Q1-SUM_LP_&byyear.Q4;
DO I = 1 TO 4;
	SUM_LP(I) = LP*AVAL(I);
END;
DROP I;

run;

%mend;

%getLP(Temp06, Temp07, 2005);
%getLP(Temp07, Temp07, 2006);
%getLP(Temp07, Temp07, 2007);
%getLP(Temp07, Temp07, 2008);
%getLP(Temp07, Temp07, 2009);
%getLP(Temp07, Temp07, 2010);






%macro getSP(datain, dataout, byyear);

data &dataout;
set &datain;

ARRAY AVAL(4) AVAL&byyear.Q1-AVAL&byyear.Q4;
ARRAY SUM_SP(4) SUM_SP_&byyear.Q1-SUM_SP_&byyear.Q4;

DO I = 1 TO 4;
	if YYQ(year(STD1),QTR(STD1)) = YYQ(&byyear, I) then do;
		if		ST = "Closed" then SUM_SP(I) = SP*1;		
		else 	SUM_SP(I) = 0;
	end;
	else SUM_SP(I) = 0;
END;
DROP I;

run;
run;

%mend;

%getSP(Temp07, Temp08, 2005);
%getSP(Temp08, Temp08, 2006);
%getSP(Temp08, Temp08, 2007);
%getSP(Temp08, Temp08, 2008);
%getSP(Temp08, Temp08, 2009);
%getSP(Temp08, Temp08, 2010);






%macro getSOLD(datain, dataout, byyear);

data &dataout;
set &datain;

ARRAY SUM_SOLD(4) SUM_SOLD_&byyear.Q1-SUM_SOLD_&byyear.Q4;

DO I = 1 TO 4;
	if YYQ(year(STD1),QTR(STD1)) = YYQ(&byyear, I) then do;
		if		ST = "Closed" then SUM_SOLD(I) = 1;		
		else 	SUM_SOLD(I) = 0;
	end;
	else SUM_SOLD(I) = 0;
END;
DROP I;

run;
run;

%mend;

%getSOLD(Temp08, Temp09, 2005);
%getSOLD(Temp09, Temp09, 2006);
%getSOLD(Temp09, Temp09, 2007);
%getSOLD(Temp09, Temp09, 2008);
%getSOLD(Temp09, Temp09, 2009);
%getSOLD(Temp09, Temp09, 2010);



proc sort; by ln descending STD1; run;
proc sort nodupkey; by ln; run;

proc sort; by Property_Class; run;

PROC SUMMARY
DATA=TEMP09 NWAY;
CLASS TRACT1;
VAR 
	AVAL2005Q1-AVAL2005Q4
	AVAL2006Q1-AVAL2006Q4
	AVAL2007Q1-AVAL2007Q4
	AVAL2008Q1-AVAL2008Q4
	AVAL2009Q1-AVAL2009Q4
	AVAL2010Q1-AVAL2010Q4

	SUM_LP_2005Q1-SUM_LP_2005Q4
	SUM_LP_2006Q1-SUM_LP_2006Q4
	SUM_LP_2007Q1-SUM_LP_2007Q4
	SUM_LP_2008Q1-SUM_LP_2008Q4
	SUM_LP_2009Q1-SUM_LP_2009Q4
	SUM_LP_2010Q1-SUM_LP_2010Q4

	SUM_SP_2005Q1-SUM_SP_2005Q4
	SUM_SP_2006Q1-SUM_SP_2006Q4
	SUM_SP_2007Q1-SUM_SP_2007Q4
	SUM_SP_2008Q1-SUM_SP_2008Q4
	SUM_SP_2009Q1-SUM_SP_2009Q4
	SUM_SP_2010Q1-SUM_SP_2010Q4

	SUM_SOLD_2005Q1-SUM_SOLD_2005Q4
	SUM_SOLD_2006Q1-SUM_SOLD_2006Q4
	SUM_SOLD_2007Q1-SUM_SOLD_2007Q4
	SUM_SOLD_2008Q1-SUM_SOLD_2008Q4
	SUM_SOLD_2009Q1-SUM_SOLD_2009Q4
	SUM_SOLD_2010Q1-SUM_SOLD_2010Q4
;

OUTPUT OUT=TEMP10 SUM=;
run;


PROC SUMMARY
DATA=TEMP09 NWAY;
CLASS TRACT1;
By Property_Class;
VAR 
	AVAL2005Q1-AVAL2005Q4
	AVAL2006Q1-AVAL2006Q4
	AVAL2007Q1-AVAL2007Q4
	AVAL2008Q1-AVAL2008Q4
	AVAL2009Q1-AVAL2009Q4
	AVAL2010Q1-AVAL2010Q4

	SUM_LP_2005Q1-SUM_LP_2005Q4
	SUM_LP_2006Q1-SUM_LP_2006Q4
	SUM_LP_2007Q1-SUM_LP_2007Q4
	SUM_LP_2008Q1-SUM_LP_2008Q4
	SUM_LP_2009Q1-SUM_LP_2009Q4
	SUM_LP_2010Q1-SUM_LP_2010Q4

	SUM_SP_2005Q1-SUM_SP_2005Q4
	SUM_SP_2006Q1-SUM_SP_2006Q4
	SUM_SP_2007Q1-SUM_SP_2007Q4
	SUM_SP_2008Q1-SUM_SP_2008Q4
	SUM_SP_2009Q1-SUM_SP_2009Q4
	SUM_SP_2010Q1-SUM_SP_2010Q4

	SUM_SOLD_2005Q1-SUM_SOLD_2005Q4
	SUM_SOLD_2006Q1-SUM_SOLD_2006Q4
	SUM_SOLD_2007Q1-SUM_SOLD_2007Q4
	SUM_SOLD_2008Q1-SUM_SOLD_2008Q4
	SUM_SOLD_2009Q1-SUM_SOLD_2009Q4
	SUM_SOLD_2010Q1-SUM_SOLD_2010Q4
;

OUTPUT OUT=TEMP11 SUM=;
run;

data Temp10;
retain Property_Class;
set Temp10;
format Property_Class $50.;
Property_Class = "ALL TYPE";
run;

libname out "D:\Workspace\MLS\sas_code\";

data Market_Activity;
set Temp10
	Temp11
;
run;




%macro getRatio(datain, dataout, byyear);

data &dataout;
set &datain;
ARRAY PERCENT_CLSD_ACTIVE(4) PERCENT_CLSD_ACTIVE_&byyear.Q1-PERCENT_CLSD_ACTIVE_&byyear.Q4;
ARRAY AVAL(4) AVAL&byyear.Q1-AVAL&byyear.Q4;
ARRAY SUM_SOLD(4) SUM_SOLD_&byyear.Q1-SUM_SOLD_&byyear.Q4;
DO I = 1 TO 4;
	if AVAL(I) ^= 0 then PERCENT_CLSD_ACTIVE(I) = SUM_SOLD(I)/AVAL(I); else PERCENT_CLSD_ACTIVE(I) = .;
END;
DROP I;
run;

%mend;

%getRatio(Market_Activity, Temp12, 2005);
%getRatio(Temp12, Temp12, 2006);
%getRatio(Temp12, Temp12, 2007);
%getRatio(Temp12, Temp12, 2008);
%getRatio(Temp12, Temp12, 2009);
%getRatio(Temp12, out.Market_Activity_TRACT1, 2010);


