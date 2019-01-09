gc()
rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,dplyr,data.table,lubridate,stringr,ISOweek,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
 
today <- today()
yesterday <- today()-1
last30days <- today()-30
last6months <- today()-180
last12months <- today()-360

thisyear <- year(today())
thisyear_start <- paste(thisyear,"-01-01",sep="")
lastyear <- thisyear-1
lastyear_start <- paste(lastyear,"-01-01",sep="")
lastyear_end <- paste(lastyear,"-12-31",sep="")

load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")

DB_Consumption_End <- as.data.table(tbl(con,"consumption") %>% dplyr::filter(date(date_consumption) >=yesterday &
                                                                             date(date_consumption) <=today))
                     
DB_Consumption_Diff <- anti_join(DB_Consumption_End,DB_Consumption)
DB_Consumption <- rbind(DB_Consumption,DB_Consumption_Diff) 
# DB_Consumption <- as.data.table(tbl(con,"consumption"))
save(DB_Consumption,file="/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")

DB_Consumption_last30days <- DB_Consumption[date(date_consumption)>=last30days]
DB_Consumption_last6months <- DB_Consumption[date(date_consumption)>=last6months]
DB_Consumption_last12months <- DB_Consumption[date(date_consumption)>=last12months]
DB_Consumption_thisyear <- DB_Consumption[date(date_consumption)>=thisyear_start]

write.fst(DB_Consumption_last30days,"/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_last30days.fst", 100) 
write.fst(DB_Consumption_last6months,"/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_last6months.fst", 100) 
write.fst(DB_Consumption_last12months,"/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_last12months.fst", 100) 
write.fst(DB_Consumption_thisyear,"/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_thisyear.fst", 100) 

rm(DB_Consumption)
gc(verbose=T)

load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Index.RData")
max.date_index <- date(max(DB_Index$current_index_date,na.rm = TRUE))-days(1)
DB_Index_End <- as.data.table(tbl(con,"index") %>% filter(current_index_date >= max.date_index)) 
DB_Index_Diff <- anti_join(DB_Index_End,DB_Index)
DB_Index <- rbind(DB_Index,DB_Index_Diff) %>% as.data.frame()
# DB_Index <- as.data.table(tbl(con,"index"))
save(DB_Index,file="/srv/shiny-server/DataAnalyticsPortal/data/DB_Index.RData")

DB_Index$current_index_date <- as.Date(DB_Index$current_index_date)
DB_Index_thisyear <- DB_Index %>% dplyr::filter(current_index_date>=thisyear_start)

DB_Index_last30days <- DB_Index %>% dplyr::filter(current_index_date>=last30days)
DB_Index_last6months <- DB_Index %>% dplyr::filter(current_index_date>=last6months)

write.fst(DB_Index_last30days,"/srv/shiny-server/DataAnalyticsPortal/data/DB_Index_last30days.fst", 100) 
write.fst(DB_Index_last6months,"/srv/shiny-server/DataAnalyticsPortal/data/DB_Index_last6months.fst", 100) 

servicepoint <- as.data.frame(tbl(con,"service_point"))

DB_Consumption_thisyear_servicepoint <- inner_join(DB_Consumption_thisyear,servicepoint,by=c("id_service_point"="id"))
DB_Consumption_last12months_servicepoint <- inner_join(DB_Consumption_last12months,servicepoint,by=c("id_service_point"="id"))

Punggol_thisyear <- DB_Consumption_thisyear_servicepoint %>%
  dplyr::mutate(Date=date(date_consumption),D=day(date_consumption),M=month(date_consumption)) %>%
  dplyr::filter(site %in% c("Punggol","Whampoa")) %>%
  dplyr::select_("service_point_sn","adjusted_consumption","interpolated_consumption","date_consumption",
                 "block","meter_type","floor","unit","room_type","Date","site") 

Yuhua_thisyear <- DB_Consumption_thisyear_servicepoint %>%
  dplyr::mutate(Date=date(date_consumption),D=day(date_consumption),M=month(date_consumption)) %>%
  dplyr::filter(site %in% c("YUHUA")) %>%
  dplyr::select_("service_point_sn","adjusted_consumption","interpolated_consumption","date_consumption",
                 "block","meter_type","floor","unit","room_type","Date","site") 

write.fst(Punggol_thisyear,"/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst", 100) 
#write.fst(Punggol_2017,"/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst", 100) 

rm(DB_Index)
gc(verbose=T)

## id added becauase usage_breakdown needs it
Punggol_last12months <- DB_Consumption_last12months_servicepoint %>%
  dplyr::mutate(Date=date(date_consumption)) %>%
  dplyr::filter(site %in% c("Punggol","Whampoa")) %>%
  dplyr::select_("service_point_sn","adjusted_consumption","interpolated_consumption","date_consumption",
                 "block","meter_type","floor","unit","room_type","Date","site") 

Punggol_last12months <- Punggol_last12months %>% mutate(Date.Time=round_date(ymd_hms(date_consumption),"hour"),
                                                        H=hour(date_consumption),D=day(date_consumption),
                                      M=month(date_consumption),Y=year(date_consumption),wd=weekdays(date_consumption))

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

Punggol_last12months$week <- gsub("-W","_",str_sub(date2ISOweek(Punggol_last12months$Date),end = -3)) # convert date to week

Punggol_last12months$Consumption <- Punggol_last12months$adjusted_consumption

Updated_DateTime_HourlyCons <- paste("Last Updated on ",now(),"."," Next Update on ",now()+60*60,".",sep="")

save(Punggol_last12months,Week.date,Updated_DateTime_HourlyCons, 
     file="/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last12months.RData")

save(Week.date,Updated_DateTime_HourlyCons, 
     file="/srv/shiny-server/DataAnalyticsPortal/data/Week.date_Updated_DateTime_HourlyCons.RData")
write.fst(Punggol_last12months,"/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last12months.fst", 100) 

Punggol_last6months <- Punggol_last12months %>% dplyr::filter(Date>=last6months)
write.fst(Punggol_last6months,"/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst", 100) 

Punggol_last30days <- Punggol_last12months %>% dplyr::filter(Date>=last30days)
write.fst(Punggol_last30days,"/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last30days.fst", 100) 

source('/srv/shiny-server/DataAnalyticsPortal/source/Weather Scrapping_V2.R', local=TRUE)
load("/srv/shiny-server/DataAnalyticsPortal/data/Weather.RData")
if(max(Weather$Date,na.rm=TRUE)<max(Punggol_last12months$Date)-1){
  tmp.weather <- Extract.Weather(max(Weather$Date,na.rm = TRUE)+1)
  Weather <- Weather %>% filter(Date < min(tmp.weather$Date))
  Weather <- rbind(Weather,tmp.weather)
save(Weather,file = '/srv/shiny-server/DataAnalyticsPortal/data/Weather.RData')
}

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_PunggolFinalDF_NEW successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)