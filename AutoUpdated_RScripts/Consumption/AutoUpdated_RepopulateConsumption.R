rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,dplyr,data.table,lubridate,stringr,ISOweek,fst)

thisyear <- year(today())
thisyear_start <- paste(thisyear,"-01-01",sep="")

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

DB_Consumption <- as.data.table(tbl(con,"consumption"))
save(DB_Consumption,file="/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")

DB_Consumption$adjusted_date <- DB_Consumption$date_consumption-lubridate::hours(1)

servicepoint <- as.data.table(tbl(con,"service_point"))
DB_Consumption_servicepoint <- inner_join(as.data.frame(DB_Consumption),as.data.frame(servicepoint),by=c("id_service_point"="id"))

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

Punggol_thisyear <- Punggol_All %>% dplyr::filter(Date>=thisyear_start)
fstwrite(Punggol_thisyear,"/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst", 100) 

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_RepopulateConsumption successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)