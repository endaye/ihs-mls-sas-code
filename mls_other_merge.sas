option compress=yes;
libname d '/opt/data/datamain/MLS/output/other';
libname i '/opt/data/datamain/MLS/MLS_old/sas_dataset';
libname sh '/opt/data/datamain/MLS/sas_dataset_final_other';
libname o '/opt/data/datamain/MLS/MLS_old/historic_pattern_cleaned';
libname tmp '/opt/data/user/sliu14/MLS';

%let date=%sysfunc(year("&sysdate9."d));
%put~~&date.;
/*
%macro combine(datatype, datafrom, yearbegin, yearend);

%do i=&yearbegin. %to &yearend.;
        data &datafrom&datatype._&i;
        set &datafrom..&datatype._&i;

        ***change all the variables to uppercase;

        array vars(*) _character_;
        do j=1 to dim(vars);
           vars(j)=upcase(vars(j));
        end;
        run;
        data &datafrom&datatype._&i;
        set &datafrom&datatype._&i(where=(cny ne 'COOK'));
		run;

        ***format the date;

        proc contents data= &datafrom&datatype._&i noprint out=mls_ctnt; run;
        data var_date;
        set mls_ctnt;
 		if upcase(name)='AUCTION_DATE' | upcase(name)='BMD' | upcase(name)='CLOSEDDATE' | upcase(name)='CONTRACTDATE'
        | upcase(name)='LD' | upcase(name)='LDR' | upcase(name)='OD' | upcase(name)='OMD'
        | upcase(name)='PHOTODATE' | upcase(name)='RECORDMODDATE' | upcase(name)='STD' | upcase(name)='UD'
        | upcase(name)='XD' | upcase(name)='VTDATE' | upcase(name)='AVAILABLE_DATE';
        format dvar_out $50.;
        dvar_out = cats(Name,'1');
        run;


        data _NULL_;
        set var_date;
        call symputx('dvarnum', _N_);
        call symput(cats('dvarin',_N_), name);
        call symput(cats('dvarout',_N_), dvar_out);
        run;

        data tmp.&datafrom&datatype._&i;
        set &datafrom&datatype._&i; 
        %do j = 1 %to &dvarnum;
			&&dvarin&j=compress(&&dvarin&j,'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
                if length(&&dvarin&j) = 6 then do;
                        tmp_mth = substr(&&dvarin&j ,1,2)*1;
                        tmp_day = substr(&&dvarin&j ,3,2)*1;
                        tmp_yr = substr(&&dvarin&j ,5,2)*1;
                        if tmp_yr <= 30 then tmp_yr = tmp_yr+2000;
                        else if tmp_yr > 30 then tmp_yr = tmp_yr+1900;
 		end;
                if length(&&dvarin&j) > 6 then do;
                        tmp_yr = substr(&&dvarin&j ,1,4)*1;
                        tmp_mth = substr(&&dvarin&j ,6,2)*1;
                        tmp_day = substr(&&dvarin&j ,9,2)*1;
                end;              
                format &&dvarout&j yymmdd10.;
                &&dvarout&j = mdy(tmp_mth, tmp_day, tmp_yr);
        %end;
        LN1=compress(LN,'ABCDEFGHIJKLMNOPQRSTUVWXYZ')*1;
        ST1=compress(ST,'0123456789');
        year=&i;
        drop tmp_mth tmp_day tmp_yr LD1_QR LD1_YR STD1_QR STD1_YR UD1_QR UD1_YR YEARD YEARTXT ;
        run;

%end;
      

%mend combine(datatype, datafrom,, yearbegin, yearend);     

%combine(attachedsingle, i, 2005, 2011);                 
%combine(attachedsingle_final, o, 2004, 2008);                  
%combine(other_attachedsingle, d, 2011, &date.);

%combine(deededparking, i, 2005, 2011);         
%combine(deededparking_final, o, 2004, 2008);            
%combine(other_deededparking, d, 2011, &date.);           

%combine(detachedsingle, i, 2005, 2011);
%combine(detachedsingle_final, o, 2004, 2008);
%combine(other_detachedsingle, d, 2011, &date.);          

%combine(mobilehomes, i, 2005, 2011);                 
%combine(mobilehomes_final, o, 2004, 2008);
%combine(other_mobilehomes, d, 2011, &date.);

%combine(multifamily, i, 2005, 2011);
%combine(multifamily_final, o, 2004, 2008);
%combine(other_multifamily, d, 2011, &date.);

%combine(rentals, i, 2005, 2011);   
%combine(rentals_final, o, 2004, 2008);
%combine(other_rentals, d, 2011, &date.);

%combine(twotofour, i, 2005, 2011);
%combine(twotofour_final, o, 2004, 2008);                
%combine(other_twotofour, d, 2011, &date.);  

%combine(vacantland, i, 2005, 2011);
%combine(vacantland_final, o, 2004, 2008);      
%combine(other_vacantland, d, 2011, &date.);             

%combine(business, i, 2005, 2011);
%combine(other_business, d, 2011, &date.);    

%combine(commercial, i, 2005, 2011);                      
%combine(other_commercial, d, 2011, &date.);

%combine(industrial, i, 2005, 2011);       
%combine(other_industrial, d, 2011, &date.);       

%combine(instuttodevelop, i, 2005, 2011);
%combine(other_instuttodevelop, d, 2011, &date.);

%combine(lotsandland, i, 2005, 2011);
%combine(other_lotsandland, d, 2011, &date.);

%combine(mixeduse, i, 2005, 2011);       
%combine(other_mixeduse, d, 2011, &date.);

%combine(officetech, i, 2005, 2011);                     
%combine(other_officetech, d, 2011, &date.);  

%combine(residentialproperty, i, 2005, 2011);
%combine(other_residentialproperty, d, 2011, &date.); 

%combine(retailstores, i, 2005, 2011);
%combine(other_retailstores, d, 2011, &date.);

%combine(residentialrental, i, 2005, 2011);
%combine(other_residentialrental, d, 2011, &date.);             
*/
/****  merge the dataset & delete duplicate observations ****/ 

%macro merge(datatype);                      
data &datatype._all;
length remarks $420; 
set tmp.i&datatype._2005-tmp.i&datatype._2011
    tmp.dother_&datatype._2011-tmp.dother_&datatype._&date. tmp.o&datatype._final_2004-tmp.o&datatype._final_2008;
run;

proc sort data=&datatype._all nodupkey out=&datatype._all;
by year LN1 UD1 ST1 STD1;                
run;

/****  seperate the dataset into different years ****/ 

%do i=2004 %to &date.;
   data sh.other_&datatype._&i;
   set &datatype._all(where=(year=&i));
   run;
%end;

%mend merge(datatype);                
%merge(attachedsingle);
%merge(deededparking);                                     
%merge(detachedsingle);                                           
%merge(mobilehomes);
%merge(multifamily);
%merge(rentals);
%merge(twotofour);
%merge(vacantland);

/****  merge the dataset & delete duplicate observations ****/ 

%macro merge2(datatype);                     
data &datatype._all;                        
set tmp.i&datatype._2005-tmp.i&datatype._2011
    tmp.dother_&datatype._2011-tmp.dother_&datatype._&date.;
run;

proc sort data=&datatype._all nodupkey out=&datatype._all;
by year LN1 UD1 ST1 STD1;                
run;



%do i=2005 %to &date.;
   data sh.other_&datatype._&i;
   set &datatype._all(where=(year=&i));
   run;
%end;

%mend merge2(datatype);               

%merge2(business);
%merge2(commercial);
%merge2(industrial);
%merge2(instuttodevelop);
%merge2(lotsandland);                                      
%merge2(mixeduse);                                                
%merge2(residentialrental);
%merge2(officetech);
%merge2(residentialproperty);
%merge2(retailstores);
