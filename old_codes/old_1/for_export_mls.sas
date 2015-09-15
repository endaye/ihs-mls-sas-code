%LET class = rental;
libname mls "..\sas_dataset";

data Temp01;
set mls.&class._2005-mls.&class._2010;
keep LN LAT LNG;
run;

proc sort nodupkey; by LN; run;

PROC EXPORT DATA= WORK.Temp01 
            OUTFILE= "..\&class..csv" 
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;
