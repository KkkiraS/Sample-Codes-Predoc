///_______________________________________Example Stata Code Silvestrovich Kira_______________________________///

///Info about the code: this code is from the project I am currently working on in New Economics School. It's related to analyses of economic situtaion in Russian monocities. As it is a private project I can not share the data and the whole code, but I want to show particular parts to show my skills. It these part I am making new dataset from scratch using the data I downloaded directly from API: I clean, merge, visualize data, make first regression analyses attempts 


///________________________///Cleaning spark data & dynamic okveds///______________________________________///

///in this file we clean 1) data from spark on firms with okved 05 and 19 2) dynamic okved file 

///import xlsx to dta

local filenames: dir "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/excel_row" files "*.xlsx*"

foreach f of local filenames {
	import excel "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/excel_row/`f'",  firstrow case(lower) clear
	save "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/excel_row/`f'.dta"
}


///merge all files

///rename files in order to merge successfuly: "Spark_Coal_OKVED05_2001_05.dta"->coal_1 ... "Spark_Coal_OKVED193_2016_20.dta" -> coal_10

 
///merge  
cd "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/excel_row"

///okved 19 
use coal_6.dta, clear

forvalues i = 7(1)10 { 
    merge 1:1 sparkid using  coal_`i'.dta
    drop _merge
}
desc stuff*
tostring stuff_1999 stuff_2000 stuff_2013, replace
save coal_19.dta, replace

///okved 05 
use coal_1.dta, clear

forvalues i = 2(1)5 { 
    merge 1:1 sparkid using  coal_`i'.dta
    drop _merge
}
save coal_05.dta, replace 

merge 1:1 sparkid  using "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/excel_row/coal_19.dta"


/*    Result                      Number of obs
    -----------------------------------------
    Not matched                         2,747
        from master                     2,643  (_merge==1)
        from using                        104  (_merge==2)

    Matched                                 0  (_merge==3)

*/

***2643 okved 05 firms 104 firms with okved 19, 2747 in total

drop _merge 
save "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/excel_row/coal0519_wide_All_variables.dta", replace

///Wide into long 

reshape long stuff_ total_assets_rub_ revenue_rub_ wage_rub_, i(sparkid) j(year) 
*2747 firms, 22 years, 60434 observations 


///basic housekeeping
rename наименованиенаанглийском name
rename адресместонахождения adress
rename датарегистрации date_registration
rename даталиквидации date_liquidation
rename кодосновноговидадеятельности okved

///Work with year of registration and liquidation_year and year variable

gen year_liquidation=year(date_liquidation)
gen year_registration=year(date_registration)


save "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/coal0519.dta", replace 


///Dynamic okveds 

///We want to keep companies which were at leat for some time coal companies

//Import row dynamic okved data into dta 

cd "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/OKVEDs"
local files : dir "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/OKVEDs" files "*.csv"
	foreach file of local files {
 drop _all
 insheet using `file'
  local outfile = subinstr("`file'",".csv","",.)
 save "`outfile'", replace
}
clear

///append in one file 

///string to append smoothly

use okveds57.dta, clear
tostring year, replace
save okveds57.dta, replace

///append 

use okveds1.dta, clear
local i=2
while `i'<=81{
append using "okveds`i'.dta"
local i=`i'+1
}

sum sparkid 
///removing OKVED codes from quarterly accounting reports.
gen yearnew = usubstr(year, -8, .)
drop if yearnew != ""
drop yearnew 
destring year, replace

///to be consistent with dataset from spark
tostring sparkid, replace 
rename okved okved_dynamic
gen fromdynamic = 1

cd "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data"
*for CS: cd "c:/Users/Carsten/Dropbox/Monocities_Shared/Kira/December/Data"

save "dynamic_wholeuniverse", replace

///Keep only forms that had coal okved at leat for 5 years. How we define coal okved? Okved "10" before 2016 and starting on "05" or "19.31", "19.32" or "19.33" starting from 2016

gen okved_2=ustrleft(okved_dynamic,2)
gen okved_3=ustrleft(okved_dynamic,4)
*CS: I changed the following to exclude "torf" (10.3 before 2016)
gen okved_coal=1 if okved_2=="05" & year>=2016 | okved_dynamic=="19.31"  & year>=2016 | okved_dynamic=="19.32"  & year>=2016 | okved_dynamic=="19.33"  & year>=2016 | okved_3=="10.1" & year<2016 | okved_3=="10.2" & year<2016 | okved_dynamic=="10" & year<2016


***calculate for how many years a firm had a coal okved 
bys sparkid: egen years_coal=total(okved_coal)

tab2 year years_coal 

sort sparkid year
bys sparkid: gen lastyear = year[_N]

sum lastyear

***for firms, for which last year is more then 2003 (so firm can exist in 1999,2000,2001,2002,2003) we drop all firms that have coal okved less then for five years
drop if lastyear>=2003 & years_coal<5 & lastyear!=. & sparkid!="531786"
***keep 531786: last year is 2003, 4 years coal but actually it is 5 (one missing year in between).

***The only exception is when the last year with coal OKVED is 2002 and we have 4 years (1999-2002). This is because our data starts in 1999 only.
drop if lastyear==2002 & years_coal<4

***Drop firms with last year 2001 as we start our analyses in 2002
drop if lastyear<=2001

by sparkid, sort: gen nvals = _n == 1  

sum year if nvals==1 
*we are left with 717 firms 

///Now we want to compare this sparkids with those that we have in dataset from spark 

drop okved_2 okved_3
drop nvals


save "dynamic.dta", replace 

///merge
merge 1:1 sparkid year using coal0519

/* 

     Result                      Number of obs
    -----------------------------------------
    Not matched                        55,069
        from master                       779  (_merge==1)
        from using                     54,290  (_merge==2)

    Matched                             6,144  (_merge==3)
    -----------------------------------------

*/

///How many firms didn't merge? -> 65
by sparkid, sort: gen nvals = _n == 1 
count if nvals==1 & _merge==1

///For these firms from dynamic we want to add additional data and we want to observe them for twenty two years, for now, for these firms we stop observing them after they are liquidated

///we want to have information from spark for them, in original excel file as usual rename variable that start on number and спарккод variable, in the same way as we did in the begininning of rhis do file

///import xlsx to dta
cd "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/dynamic_extra
* for CS: cd "c:/Users/Carsten/Dropbox/Monocities_Shared/Kira/December/Data/dynamic_extra"

foreach num of numlist 1/5 {
  import excel "dynamic_extra`num'.xlsx", sheet("report") firstrow clear
  save "dynamic_extra`num'.dta"
  }

///merge all files

use dynamic_extra1.dta, clear
forvalues i = 2(1)5 { 
    merge 1:1 sparkid using  dynamic_extra`i'.dta
    drop _merge
}

save fordynamicaddtitionaldata.dta, replace

///wide to long

reshape long stuff_ total_assets_rub_ revenue_rub_ wage_rub_, i(sparkid) j(year) 

///basic housekeeping
drop  A
rename Наименованиенаанглийском name
rename Адресместонахождения adress
rename Датарегистрации date_registration
rename Даталиквидации date_liquidation
rename Кодосновноговидадеятельности okved

///Work with year of registration and liquidation_year and year variable

gen year_liquidation=year(date_liquidation)
gen year_registration=year(date_registration)



save fordynamicaddtitionaldata.dta, replace

//////merge to our file with firms not matched from dynamic

cd "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data
* for CS: cd "c:/Users/Carsten/Dropbox/Monocities_Shared/Kira/December/Data"

use dynamic.dta
merge m:1 sparkid year using "dynamic_extra/fordynamicaddtitionaldata.dta"
/* 
  Result                      Number of obs
    -----------------------------------------
    Not matched                         8,851
        from master                         0  (_merge==1)
        from using                      8,851  (_merge==2)

    Matched                             6,923  (_merge==3)
    -----------------------------------------

 */

drop _merge okved_coal years_coal  lastyear
replace fromdynamic=1 if fromdynamic!=1

*rename variables to be consistnent with coal0519 dataset 
rename Наименование наименование
rename Регистрационныйномер регистрационныйномер 
rename Краткоенаименование краткоенаименование 
rename РуководительФИО руководительфио 
rename Регионрегистрации регионрегистрации 
rename КодОКАТО кодокато
rename НаименованиеОКАТО наименованиеокато 
rename КодОКТМО кодоктмо 
rename Виддеятельностиотрасль виддеятельностиотрасль 
rename Важнаяинформация важнаяинформация
rename Наименованиеполное наименованиеполное 
rename Статус статус 

save dynamic.dta, replace


///merge data on latitude & longitude 
merge m:1 sparkid using "/Users/kasilvestrovich/Dropbox/Monocities_Shared/Kira/API/long_lat_all_firms.dta"

/*      Result                      Number of obs
    -----------------------------------------
    Not matched                    12,272,282
        from master                       616  (_merge==1)
        from using                 12,271,666  (_merge==2)

    Matched                            61,336  (_merge==3)
    -----------------------------------------

 */
	
*for 28 firms we do not now langitude and latitude and 27 from them have been founded after 2016 so they would be excluded any way

drop if _merge==2

drop _merge 


///also we want to observe only firms that existed for more than 5 years, and as 2020 is the last year observed, we exclude firms that were founded after 2015 

keep if year_registration<2016
*we drop 292 such firms, 2518 firms left 

*Also we drop 1 firms for which we do not have data on langitude&latitude
keep if longitude!=""

*drop this firm as iit was included in dynamic okved ocasionally since it hade okved starting on 10 among observation before 2016, but its connected to meat

drop if sparkid=="129923" | sparkid=="7998"

*2515 firms are left 

///our final bacis dataset

save "/Users/kirasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/coal0519.dta", replace

///_______________________________///Revenue and total assets analyses///_________________________________________///


*summary stats
bys sparkid: egen median_revenue_rub=median(revenue_rub_)  if year>=2002 & year<=2020
bys sparkid: egen median_totassers_rub=median(total_assets_rub_) if year>=2002 & year<=2020

gen ln_median_revenue_rub=ln(median_revenue_rub)
gen ln_median_totassers_ru=ln(median_totassers_rub)

drop if year<2002
bys sparkid: gen nvals = _n == 1 
keep if nvals==1

sum median_revenue_rub, det 
sum median_totassers_rub, det 

hist ln_median_revenue_rub, graphregion(color(white)) title("log of median revenue")
hist ln_median_totassers_ru, graphregion(color(white)) title("log of median total assets")

*restrictions

gen revenue_available=1 if revenue_rub_!=. & year>=2002 & year<=2020
gen totas_available=1 if total_assets_rub_!=. & year>=2002 & year<=2020
gen ass_rev_available=1 if total_assets_rub_!=. & revenue_rub_!=. & year>=2002 & year<=2020
bys sparkid: egen years_rev_avail=total(revenue_available)
bys sparkid: egen years_asset_avail=total(totas_available)
bys sparkid: egen years_revasset_av=total(ass_rev_available)

bys sparkid: gen nvals = _n == 1 
keep if nvals==1

*1 year availability

count if years_rev_avail>=1
count if years_asset_avail >=1
count if years_revasset_av >=1

* 5 years availability
count if years_rev_avail>=5
count if years_asset_avail >=5
count if years_revasset_av >=5

*with division on village/city

*1 year availability

count if years_rev_avail>=1 & village==0
count if years_asset_avail >=1 & village==0
count if years_revasset_av >=1 & village==0

* 5 years availability
count if years_rev_avail>=5 & village==0
count if years_asset_avail >=5 & village==0
count if years_revasset_av >=5 & village==0

///________________________________________///Restrictions///_________________________________________________///

use "/Users/kirasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/coal0519.dta"
///keep only okved 05
gen okved_new=ustrleft(okved,2)
keep if okved_new=="05"

///add IPC and adjust tot assets and reveue to it 
merge m:1 year using "/Users/kirasilvestrovich/Dropbox/Monocities_Shared/Kira/December/Data/IPC.dta"
gen adj_rev_rub= revenue_rub_/mean_IPC_to2000
gen adj_totassets_rub= total_assets_rub_/mean_IPC_to2000
