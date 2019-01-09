rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,dplyr,data.table,lubridate,stringr,ISOweek)

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

today <- today()
yesterday <- today()-1

load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")
## some issues happen which max.date_consumption = 2017-03-31 occurs on 2017-03-11
# max.date_consumption <- date(max(DB_Consumption$date_consumption,na.rm = TRUE))-days(1)

#DB_Consumption_End <- as.data.table(tbl(con,"consumption") %>% filter(date_consumption >= max.date_consumption))
                                 
DB_Consumption_End <- as.data.table(tbl(con,"consumption") %>% dplyr::filter(date(date_consumption) >=yesterday &
                                                                             date(date_consumption) <=today))
                     
DB_Consumption_Diff <- anti_join(DB_Consumption_End,DB_Consumption)
DB_Consumption <- rbind(DB_Consumption,DB_Consumption_Diff)
# DB_Consumption <- as.data.table(tbl(con,"consumption"))
save(DB_Consumption,file="/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")

load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Index.RData")
max.date_index <- date(max(DB_Index$current_index_date,na.rm = TRUE))-days(1)
DB_Index_End <- as.data.table(tbl(con,"index") %>% filter(current_index_date >= max.date_index)) 
DB_Index_Diff <- anti_join(DB_Index_End,DB_Index)
DB_Index <- rbind(DB_Index,DB_Index_Diff)
save(DB_Index,file="/srv/shiny-server/DataAnalyticsPortal/data/DB_Index.RData")

### adjusted_date column to be added
### based on date_consumption minus one hour
DB_Consumption$adjusted_date <- DB_Consumption$date_consumption-lubridate::hours(1)

servicepoint <- as.data.table(tbl(con,"service_point"))

DB_Consumption_servicepoint <- inner_join(DB_Consumption,servicepoint,by=c("id_service_point"="id"))

# Punggol_All <- DB_Consumption_servicepoint %>%
#                dplyr::mutate(Date=date(adjusted_date)) %>%
#                dplyr::filter(site %in% c("Punggol","Whampoa") & Date >="2016-02-24") %>%
#                dplyr::select_("service_point_sn","adjusted_consumption","interpolated_consumption","date_consumption","adjusted_date",
#                               "block","meter_type","floor","unit","room_type","Date","site") 

## id added becauase usage_breakdown needs it
Punggol_All <- DB_Consumption_servicepoint %>%
  dplyr::mutate(Date=date(adjusted_date)) %>%
  dplyr::filter(site %in% c("Punggol","Whampoa") & Date >="2016-02-24") %>%
  dplyr::select_("service_point_sn","adjusted_consumption","interpolated_consumption","date_consumption","adjusted_date",
                 "block","meter_type","floor","unit","room_type","Date","site","id") 

Punggol_All <- Punggol_All %>% mutate(Date.Time=adjusted_date,H=hour(adjusted_date),D=day(adjusted_date),
                                      M=month(adjusted_date),Y=year(adjusted_date),wd=weekdays(adjusted_date))

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

Punggol_All$week <- gsub("-W","_",str_sub(date2ISOweek(Punggol_All$Date),end = -3)) # convert date to week

Punggol_All$Consumption <- Punggol_All$adjusted_consumption

save(Punggol_All, Week.date, file="/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")

source('/srv/shiny-server/DataAnalyticsPortal/source/Weather Scrapping_TEST.R', local=TRUE)
load("/srv/shiny-server/DataAnalyticsPortal/data/Weather.RData")
#if(max(Weather$Date,na.rm=TRUE)<max(Punggol_All$Date)){
if(max(Weather$Date,na.rm=TRUE)<max(Punggol_All$Date)-1){
Weather <- Extract.Weather(min(Punggol_All$Date))
save(Weather,file = '/srv/shiny-server/DataAnalyticsPortal/data/Weather.RData')
}

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_PunggolFinalDF_V2 successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)