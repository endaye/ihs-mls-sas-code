/**************************************************************
   Yearly Changes 
***************************************************************/
%let year = 2011 ;
%let pyear = 2010 ;


*libname mls "/home/datamain/MLS/output";
*libname out "/home/datamain/MLS/output1";
libname mls "../output";
libname out "../output1";
option compress = yes;

%include "./mlsUtil.sas";


%macro runALL(CINPUT);
%mlsMerge(mls, &CINPUT ,TEMP01, &pyear, &year);

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

%DateConvert(Temp01, Temp03, LD, LD1);
%DateConvert(Temp03, Temp04, STD, STD1);
%DateConvert(TEMP04, Temp05, UD, UD1);

data TEMP05;
set TEMP05;
yeard = year(UD1);
yearTXT = put(yeard, 4.);
if yeard = &year ; *Set this year to current year;
run;

%reFormat(work, TEMP05, TEMP06);

proc sort; by yearTXT; run;

%macro break(byval);
        data out.&CINPUT._&byval;
        set TEMP06(where=(yearTXT="&byval"));
        run;
%mend break;

data _null_;
  set TEMP06;
  by yearTXT;
  if first.yearTXT then
    call execute('%break('||trim(yearTXT)||')');
run;
%mend;

%runALL(multifamily);
%runALL(twotofour);
%runALL(detachedsingle);
%runALL(attachedsingle);
%runALL(rentals);
%runALL(residentialproperty);
%runALL(lotsandland);
%runALL(commercial);
%runALL(mobilehomes);
%runALL(residentialrental);
%runALL(deededparking);
%runALL(vacantland);
%runALL(officetech);
%runALL(business);
%runALL(mixeduse);
%runALL(retailstores);
%runALL(instuttodevelop);
%runALL(industrial);
