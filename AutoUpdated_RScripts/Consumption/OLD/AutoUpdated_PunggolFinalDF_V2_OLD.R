rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,dplyr,data.table,lubridate,stringr,ISOweek)

# Establish connection
con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")

load("/srv/shiny-server/DataAnalyticsPortal/data/consumption_Start.RData")
max.time <- max(consumption_Start$date_consumption)
consumption_End <- as.data.table(tbl(con,"consumption") %>% filter(date_consumption > max.time))

# consumption_End <- tbl(con,"consumption") %>% filter(date(date_consumption) >= "2016-11-01")

l <- list(consumption_Start,consumption_End)
consumption <- rbindlist(l)
consumption_Start <- consumption
save(consumption_Start,file = "/srv/shiny-server/DataAnalyticsPortal/data/consumption_Start.RData")

servicepoint <- as.data.table(tbl(con,"service_point"))

consumption_servicepoint <- inner_join(consumption,servicepoint,by=c("id_service_point"="id"))

Punggol_All <- consumption_servicepoint %>%
               dplyr::mutate(Date=date(date_consumption)) %>%
               dplyr::filter(site %in% c("Punggol","Whampoa") & Date >="2016-02-24") %>%
               dplyr::select_("service_point_sn","adjusted_consumption","interpolated_consumption","date_consumption","block","meter_type","floor","unit","room_type","Date","site") 
              
Punggol_All <- Punggol_All %>% mutate(Date.Time=date_consumption,H=hour(date_consumption),D=day(date_consumption),
                                      M=month(date_consumption),Y=year(date_consumption),wd=weekdays(date_consumption))

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

Punggol_All$week <- gsub("-W","_",str_sub(date2ISOweek(Punggol_All$Date),end = -3)) # convert date to week

Punggol_All$Consumption <- Punggol_All$adjusted_consumption

save(Punggol_All, Week.date, file="/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")

source('/srv/shiny-server/DataAnalyticsPortal/source/Weather Scrapping.R', local=TRUE)
load("/srv/shiny-server/DataAnalyticsPortal/data/Weather.RData")
if(max(Weather$Date,na.rm=TRUE)<max(Punggol_All$Date)){
Weather <- Extract.Weather(min(Punggol_All$Date))
save(Weather,file = '/srv/shiny-server/DataAnalyticsPortal/data/Weather.RData')
}

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_PunggolFinalDF_V2 successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)