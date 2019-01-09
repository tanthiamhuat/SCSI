rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

## To insert into daily_consumption for AHL (3100660792) on 8 March 2017

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

Punggol_All <- Punggol_All %>% filter(adjusted_date >="2017-01-01")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 
  
PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

DailyConsumption <- PunggolConsumption %>%
  dplyr::mutate(date=date(adjusted_date),wd=weekdays(date)) %>%
  group_by(service_point_sn,month,day,wd,date) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(service_point_sn !="3101127564" & n==24  & date >="2017-01-01") %>% # remove Child-Care, full count of 24 per day
  dplyr::select_("service_point_sn","DailyConsumption","date") %>% as.data.frame()
DailyConsumption$month <- NULL
DailyConsumption$day <- NULL
DailyConsumption$wd <- NULL
DailyConsumption$date <- as.character(DailyConsumption$date)
  
daily_consumption_DB <- as.data.frame(tbl(con,"daily_consumption"))
DB_daily_consumption <- daily_consumption_DB %>% 
                        dplyr::mutate(TotalDailyConsumption=nett_consumption+overconsumption) %>%
                        dplyr::select_("service_point_sn","TotalDailyConsumption","date_consumption") %>% unique()
DB_daily_consumption$date_consumption <- as.character(DB_daily_consumption$date_consumption)

Consistency <- inner_join(DB_daily_consumption,DailyConsumption,by=c("service_point_sn","date_consumption"="date")) %>%
               dplyr::mutate(Error=TotalDailyConsumption-DailyConsumption)

