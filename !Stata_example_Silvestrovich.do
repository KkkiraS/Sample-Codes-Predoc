///_______________________________________Example Stata Code Silvestrovich Kira_______________________________///

///Info about the code: this code is from the project I am currently working on in New Economics School. It's related to analyses of economic situtaion in Russian monocities. As it is a private project I can not share the data and the whole code, but I want to show particular parts to show my skills. It these part I am making new dataset from scratch using the data I downloaded directly from API: I clean, merge, visualize data, make first regression analyses attempts 


///___________________///Making dataset from scratch. Using OKVED from SPARK____________________________________///

///Merging financial data for okved 05 
clear
cd "/Users/gerhardtoews/Dropbox/Monocities_Shared/Kira"
*cd "c:/Users/Carsten/Dropbox/Monocities_Shared/Kira"

use "November/Data/OKDEVS_COAL_OCTOBER2021/spark_okved05_2016_2020.dta"
merge 1:1 sparkid using "November/Data/OKDEVS_COAL_OCTOBER2021/spark_okved05_2011_2015.dta"
drop _merge 
merge 1:1 sparkid using "November/Data/OKDEVS_COAL_OCTOBER2021/spark_okved05_2006_2010.dta"
drop _merge 
merge 1:1 sparkid using "November/Data/OKDEVS_COAL_OCTOBER2021/spark_okved05_2001_2005.dta"
drop _merge 
save "November/Data/OKDEVS_COAL_OCTOBER2021/spark_okved05_final_GT.dta", replace

///wide into long 

reshape long Staff Netassets revenue Salary, i(sparkid) j(year) string
gen yearnew = usubstr(year, -4, .)
gen yearnew2=real(yearnew)
drop year yearnew
rename yearnew2 year 
save "November/Data/OKDEVS_COAL_OCTOBER2021/spark_okved05_final_GT.dta", replace

///eliminate duplicate
egen id_year=group(sparkid year)
by id_year, sort: gen nvals = _n == 1 
keep if nvals==1
save "November/Data/OKDEVS_COAL_OCTOBER2021/spark_okved05_final_GT.dta", replace

clear
cd "/Users/gerhardtoews/Dropbox/Monocities_Shared/Kira"

///keep only if revenue is available at least for some of the years 
tab year if revenue!=.
tab year if Netasset!=.

bys sparkid: egen sumrevenue=sum(revenue)
drop if sumrevenue==0
drop if sumrevenue<0

///basic housekeeping
rename Наименованиенаанглийском name
rename Адресместонахождения adress
rename Датарегистрации date_registration
rename Даталиквидации date_liquidation
rename Кодосновноговидадеятельности okved

save "November/Data/OKDEVS_COAL_OCTOBER2021/spark_okved05_final_GT.dta", replace

///Work with year of registration and liquidation_year

use "November/Data/OKDEVS_COAL_OCTOBER2021/spark_okved05_final_GT.dta"
gen year_liquidation=year(date_liquidation)
gen year_registration=year(date_registration)
save "November/Data/OKDEVS_COAL_OCTOBER2021/spark_okved05_final_GT.dta", replace


///__________________________________________Work with dynamic okveds file____________________________________///

///We want to keep companies which were at leat for some time coal companies

//Import row okved data into dta 
cd "/Users/kirasilvestrovich/Dropbox/Мой Mac (MacBook Air — Кира)/Downloads/OKVEDs"
local files : dir "/Users/kirasilvestrovich/Dropbox/Мой Mac (MacBook Air — Кира)/Downloads/OKVEDs" files "*.csv"
	foreach file of local files {
 drop _all
 insheet using `file'
  local outfile = subinstr("`file'",".csv","",.)
 save "`outfile'", replace
}
clear
append using `: dir . files "*.dta"'

///merge it into one file  (here must be a loop I will change it)

use "November/Data/OKVEDSdta/okveds1.dta", clear
local i=2
while `i'<=81{
append using "November/Data/OKVEDSdta/okveds`i'.dta", force
local i=`i'+1
}

drop if okved=="" | year==""

///extract year date from string date 
gen yearnew = usubstr(year, -8, .)
drop if yearnew != ""
drop yearnew 

save "November/Data/OKVEDSdta/dynamic_okveds_final_GT.dta", replace 

///Keep only those firms which had coal okved !at least for 5 years! 

gen coal=1 if okved=="05" & year>="2016" | okved=="05.20.1" & year>="2016"| okved=="05.1" & year>="2016"| okved=="05.10" & year>="2016"| okved=="05.10.1" & year>="2016"| okved=="05.10.11" & year>="2016" | okved=="05.10.12"  & year>="2016" | okved=="05.10.13"  & year>="2016" | okved=="05.10.14"  & year>="2016"| okved=="05.10.15" & year>="2016" | okved=="05.10.16"  & year>="2016"| okved=="05.10.2" & year>="2016" | okved=="05.10.21"  & year>="2016"| okved=="05.10.22" & year>="2016" | okved=="05.10.23"  & year>="2016"| okved=="05.2"  & year>="2016"| okved=="05.20"  & year>="2016"| okved=="05.20.1" & year>="2016" | okved=="05.20.11" & year>="2016" | okved=="05.20.12" & year>="2016" | okved=="05.20.2" & year>="2016"

replace coal=1 if  okved=="10" & year<="2015"| okved=="10.1" & year<="2015"| okved=="10.10" & year<="2015"| okved=="10.10.1" & year<="2015"| okved=="10.10.11" & year<="2015"| okved=="10.10.12" & year<="2015" | okved=="10.10.2"& year<="2015" | okved=="10.10.21" & year<="2015"| okved=="10.10.22" & year<="2015"| okved=="10.2" & year<="2015"| okved=="10.20" & year<="2015" | okved=="10.20.1" & year<="2015"| okved=="10.20.11" & year<="2015"| okved=="10.20.12" & year<="2015" | okved=="10.20.2" & year<="2015"| okved=="10.20.21" & year<="2015"

bys sparkid: egen years_coal=total(coal)

keep if years_coal>=5

save "November/Data/OKVEDSdta/dynamic_okveds_coal_final_GT.dta", replace 

///rename variables to be consistent with our dataset from spark 

rename opendate date_registration
rename liquidation_year date_liquidation

gen year1=real(year)
drop year
rename year1 year 
drop if year<2001

save "/Users/kirasilvestrovich/Dropbox/Мой Mac (MacBook Air — Кира)/Downloads/Monocities_okdevs_oped_dates/OKVEDSdta/dynamic_okveds_coal_final.dta", replace

///Work with adresses: extract city and region 

rename adress Адресместонахождения

///make a copy in order not to lost original variab;e 
gen Адресместонахожденияcopy= Адресместонахождения
order Адресместонахожденияcopy
///Want to extract city and region names from heterogeneous adresses 

///extract regions
split Адресместонахожденияcopy, parse(г. )
order Адресместонахожденияcopy1 Адресместонахожденияcopy2 Адресместонахожденияcopy3
split Адресместонахожденияcopy1, parse(, )
drop Адресместонахожденияcopy11 Адресместонахожденияcopy13 Адресместонахожденияcopy14 Адресместонахожденияcopy15 Адресместонахожденияcopy16 Адресместонахожденияcopy17 Адресместонахожденияcopy18
rename Адресместонахожденияcopy12 region 
drop Адресместонахожденияcopy1
drop Адресместонахожденияcopy2 Адресместонахожденияcopy3

///extract citis and villages names 
split Адресместонахожденияcopy, parse(г. )
split Адресместонахожденияcopy2, parse(, )
replace Адресместонахожденияcopy1 ="" if   !strpos(Адресместонахожденияcopy1, "пос.") &   !strpos(Адресместонахожденияcopy1, "c.") &  !strpos(Адресместонахожденияcopy1, "пгт ") & !strpos(Адресместонахожденияcopy1, "пгт.") & !strpos(Адресместонахожденияcopy1, "c ")  &   !strpos(Адресместонахожденияcopy1, "пос ") &   !strpos(Адресместонахожденияcopy1, "хутор ")
drop Адресместонахожденияcopy23 Адресместонахожденияcopy24 Адресместонахожденияcopy25 Адресместонахожденияcopy26 Адресместонахожденияcopy27 Адресместонахожденияcopy28 Адресместонахожденияcopy29
drop Адресместонахожденияcopy2 Адресместонахожденияcopy3
replace Адресместонахожденияcopy22 ="" if   !strpos(Адресместонахожденияcopy22, "г.") &   !strpos(Адресместонахожденияcopy22, "г ")
replace Адресместонахожденияcopy21= Адресместонахожденияcopy22 if Адресместонахожденияcopy22!=""
drop Адресместонахожденияcopy22
rename Адресместонахожденияcopy21 city 
split Адресместонахожденияcopy1, parse(, )
drop Адресместонахожденияcopy12 Адресместонахожденияcopy11 Адресместонахожденияcopy15 Адресместонахожденияcopy16 Адресместонахожденияcopy17 Адресместонахожденияcopy18
gen poselok= Адресместонахожденияcopy13+ Адресместонахожденияcopy14 
drop poselok
replace Адресместонахожденияcopy13= Адресместонахожденияcopy14 if Адресместонахожденияcopy14!=""
replace city= Адресместонахожденияcopy13 if city==""
drop Адресместонахожденияcopy13 Адресместонахожденияcopy14 Адресместонахожденияcopy1
order city
replace Адресместонахожденияcopy="" if city!=""
split Адресместонахожденияcopy, parse(, )
sort Адресместонахожденияcopy
drop Адресместонахожденияcopy1 Адресместонахожденияcopy2 Адресместонахожденияcopy3 Адресместонахожденияcopy5 Адресместонахожденияcopy6 Адресместонахожденияcopy7 Адресместонахожденияcopy8
replace city= Адресместонахожденияcopy4 if city==""
drop Адресместонахожденияcopy4
split city, parse("г ")
replace city1= city2 if city1==""
drop city2 city
rename city1 city
sort city
drop if city==""
drop Адресместонахожденияcopy
drop A
replace region= city if region==""

///Work with outliers
gen Адресместонахожденияcopy= Адресместонахождения
replace Адресместонахожденияcopy="" in 1/39090
replace Адресместонахожденияcopy="" in 40334/40714

split Адресместонахожденияcopy, parse(, )
drop Адресместонахожденияcopy1 Адресместонахожденияcopy2 Адресместонахожденияcopy4 Адресместонахожденияcopy5 Адресместонахожденияcopy6

replace city="х Гуково" in 1/18
replace Адресместонахожденияcopy3 ="У Хурай-Хобок" in 39164/39172
replace Адресместонахожденияcopy3 ="с Глинка" in 39091/39109

replace city= Адресместонахожденияcopy3 if Адресместонахожденияcopy!=""
drop Адресместонахожденияcopy3
drop Адресместонахожденияcopy

save "/Users/kirasilvestrovich/Dropbox/Мой Mac (MacBook Air — Кира)/Downloads/Monocities_okdevs_oped_dates/OKVEDSdta/okved_05_19_final.dta"

///Define year of registration and liquidation

gen yearregistration=year(date_registration)
gen year_liquidation=year(date_liquidation)

save "/Users/kirasilvestrovich/Dropbox/final_dataset_okved05.dta", replace 

///Want to observe each city(village) for 20 year from 2001 t0 2021

use "/Users/kirasilvestrovich/Dropbox/final_dataset_okved05.dta", clear

egen city_region=group(city region)
bys city_region: gen nvals = _n == 1 
keep if nvals==1

keep city region monocity village
expand 20
sort city region
bysort city region: gen year=2000+_n

///merge with data on population 

merge 1:1 city region year using "/Users/kirasilvestrovich/Dropbox/Monocities_Shared/Kira/main_data/Cities/population_wiki_final.dta"
drop if _merge==2

///replace population in 2018 on population in 2017 if in 2018 its missing etc 
replace population=population[_n-1] if  year==2002 & population==. |  year==2010 & population==. |  year==2018 & population==.


///Visualize: calculate number of liquidated firms in each year 
bys year: egen numliquid_year_bancrup=total(numberofliquidations_city)
bys year: egen numliquid_year_allreasons=total(numberofliquidations_city_total)
graph bar (mean) numliquid_year_allreasons, over(year) blabel(bar) ytitle(Number of liquidations all reasons) title(Number of liquidations all reasons by year) scheme(plotplain) name(liquid_bancruptcy2)
graph export "/Users/kirasilvestrovich/Dropbox/Number of liquidations all reasons.png", as(png) name("liquid_bancruptcy2")
graph bar (mean) numliquid_year_bancrup, over(year) blabel(bar) ytitle(Number of liquidations due to bancruptcy) title(Number of liquidations due to bancruptcy by year) scheme(plotplain) name(liquid_bancruptcy3)
graph export "/Users/kirasilvestrovich/Dropbox/Number of liquidations due to bancruptcy by year.png", as(png) name("liquid_bancruptcy3")

gen lnpopulation=ln(population)

///if there were no firms at all in 2002 than number of firms active variable is missing, lets replace it by 0

replace firmsin2002=0 if firmsin2002==.

save "/Users/kirasilvestrovich/Dropbox/Monocities_Shared/Kira/November/final datasets/final_dataset_city_level_okved05.dta", replace


///__________________________________________________Regressions________________________________________________///

use "/Users/kirasilvestrovich/Dropbox/Monocities_Shared/Kira/November/final datasets/final_dataset_city_level_okved05.dta"

keep if year==2002 | year==2010 | year==2018

///Want to keep only those cities for wich we have population in all years (2002, 2010, 2018)
gen populat_all_years=1 if population!=.
bys city region: egen populat_all_years_check=total(populat_all_years)
drop if populat_all_years_check!=3


///Regression for the difference between 2018 and 2010 and 20010 and 2002, reason of liquidation - bancruptcy 

xtset city_region year
gen diffpop=lnpopulation-l8.lnpopulation
gen monocity_numberofliquidations=monocity*numberofliquidations_city
label monocity_numberofliquidations "interaction monocity and number of liquidations"


eststo:reg diffpop monocity numberofliquidations_city monocity_numberofliquidations if year==2018, robust 
esttab using "/Users/kirasilvestrovich/Dropbox/Monocities_Shared/Kira/November/regressions/regression1_1.tex", replace b(3) se(3) nomtitle label star(* 0.10 ** 0.05 *** 0.01) booktabs alignment(D{.}{.}{-1}) title(Population 2010-2018: number of liquidations due to bankruptcy table \label{reg1}) addnotes("Dependent variable: Difference in logs of population between 2018 and 2010.")

