option compress=yes;

libname libo "/opt/data/datamain/MLS/output/other";
libname libre "/opt/data/datamain/MLS/sas_dataset_final_oth";

%macro combine(dstype);
%do i =2011 %to 2011;
	proc contents data=libo.other_&dstype._&i. varnum out=dold;run;
	proc sort;by name;run;
	proc contents data=libre.other_&dstype._&i. varnum out=dnew;run;
	proc sort;by name;run;
	proc print data=dnew;run;
	data m;
	merge dold(in=a) dnew(in=b);run;
	by name; 
	if a=0 and b=1;
	run;
	proc contetnts;run;
/*
	data other_&dstype._&i.;
	set libo.other_&dstype._&i. libre.other_&dstype._&i.;
	run;

	proc sort nodup;by ln1 ud st;run;	
*/
%end;
%mend;

%combine(AttachedSingle);
/*
%combine(Business);
%combine(MixedUse);
%combine(Commercial);
%combine(DetachedSingle);
%combine(DeededParking);
%combine(Industrial);
%combine(LotsAndLand);
%combine(MultiFamily);
%combine(MobileHomes);
%combine(TwoToFour);
%combine(OfficeTech);
%combine(InstutToDevelop);
%combine(Rentals);
%combine(ResidentialProperty);
%combine(ResidentialRental);
%combine(RetailStores);
%combine(VacantLand);

*/
