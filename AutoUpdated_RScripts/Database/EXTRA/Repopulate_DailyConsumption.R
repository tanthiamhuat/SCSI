## Repopulate daily_consumption table, start_date 2017-01-01, 3100250238, DailyConsumption=168 on 7th Sept 2017

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

daily_consumption_DB <- as.data.frame(tbl(con,"daily_consumption"))

#Punggol_All <- Punggol_All %>% filter(date_consumption >="2017-01-01")

#days <- seq(as.Date("2017-01-02"),as.Date("2017-09-12"),by=1)
#days <- seq(as.Date("2017-09-07"),as.Date("2017-09-08"),by=1)  ## daily_consumption: date_consumption from 2017-09-06 to 2017-09-07
#days <- seq(as.Date("2017-09-11"),as.Date("2017-09-12"),by=1)  ## daily_consumption: date_consumption from 2017-09-10 to 2017-09-11
#days <- seq(as.Date("2017-10-02"),as.Date("2017-10-27"),by=1)  ## daily_consumption: date_consumption from 2017-10-01 to 2017-10-19
days_range <- seq(as.Date("2017-10-02"),as.Date("2017-11-01"),by=1)  ## daily_consumption: date_consumption from 2017-10-01 to 2017-10-31
#days <- seq(as.Date("2017-03-02"),as.Date("2017-05-23"),by=1)

daily_consumption <- list()

for (i in 1:length(days_range))
{
PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

# length(unique(PunggolConsumption_SUB$service_point_sn))=533, excluding ChildCare, and including AHL.
PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

date_past3months <- days_range[i]-days(90)

DailyConsumption <- PunggolConsumption %>%
  dplyr::mutate(date=date(adjusted_date),wd=weekdays(date)) %>%
  group_by(service_point_sn,month,day,wd,date) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(service_point_sn !="3101127564" & n==24) # remove Child-Care, full count of 24 per day

# average of the same day (e.g if today is Wednesday, then all the same 12* Wednesday) of last 3 months (individual) 
# extract daily consumption, combine with weekdays.
DailyConsumption_past3months <- DailyConsumption %>%
  group_by(service_point_sn,wd) %>%
  dplyr::filter(wd==weekdays(days_range[i]-1) & date >= date_past3months) %>%
  dplyr::summarise(average_consumption_past3months=round(mean(DailyConsumption)))

leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))

last7days <- c(days_range[i]-c(1:7))

DailyPunggolConsumption_last7days <- PunggolConsumption %>%
  dplyr::filter(date(adjusted_date) >= move_in_date) %>%  # take into consideration of move-in-date
  dplyr::mutate(date=date(adjusted_date),wd=weekdays(adjusted_date)) %>%
  group_by(service_point_sn,date,month,day,wd) %>%
  dplyr::filter(date %in% last7days) %>%
  dplyr::summarise(dailyconsumption_last7days=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(service_point_sn !="3101127564" & n==24) # exclude ChildCare, full count of 24 per day
## should have 526 unique service_point_sn, excluding ChildCare, and including AHL.

threshold=50
daily_consumption[[i]] <- inner_join(DailyPunggolConsumption_last7days,DailyConsumption_past3months,
                                     by=c("service_point_sn","wd")) %>%
  group_by(service_point_sn,wd) %>%
  dplyr::mutate(overconsumption=dailyconsumption_last7days-(1+(threshold/100))*average_consumption_past3months) %>%
  dplyr::mutate(overconsumption=round(ifelse(overconsumption<0,0,overconsumption))) %>%
  dplyr::mutate(nett_consumption=round(dailyconsumption_last7days-overconsumption)) %>%
  dplyr::filter(date==days_range[i]-1) %>%  
  dplyr::select_("service_point_sn","nett_consumption","overconsumption","date") %>%
  dplyr::rename(date_consumption=date)
}

daily_consumption <- as.data.frame(rbindlist(daily_consumption))

  leak_alarm$start_date <- as.Date(leak_alarm$start_date)
  leak_alarm$end_date <- as.Date(leak_alarm$end_date)

  leak_alarm1 <- leak_alarm %>% filter(service_point_sn %in% daily_consumption$service_point_sn)

  daily_consumption_leak_alarm <- left_join(daily_consumption,leak_alarm1,by=c("service_point_sn")) %>%
    dplyr::mutate(is_leak=ifelse(date_consumption==start_date+3|
                                 date_consumption==start_date+10,TRUE,FALSE))

  daily_consumption <- daily_consumption_leak_alarm %>%
    dplyr::select_("service_point_sn","nett_consumption","overconsumption","date_consumption","is_leak") %>%
    unique() %>%
    dplyr::mutate(is_leak=ifelse(is.na(is_leak),FALSE,is_leak)) %>%
    arrange(service_point_sn,date_consumption,desc(is_leak))
  
  daily_consumption <- daily_consumption[!duplicated(daily_consumption[1:4]),] # remove duplicates 
  
  total_rows <- nrow(daily_consumption)
  
  daily_consumption$id <- as.integer(seq(max(daily_consumption_DB$id)+1,
                                         max(daily_consumption_DB$id)+total_rows,1))

  #daily_consumption$id <- as.integer(seq(1,total_rows,1))
                                         
  daily_consumption <- daily_consumption[,c(ncol(daily_consumption),c(1:ncol(daily_consumption)-1))]

  daily_consumption <- as.data.frame(daily_consumption)

  daily_consumption <- daily_consumption %>% dplyr::select_("id","service_point_sn","nett_consumption",
                                                            "overconsumption","date_consumption","is_leak")
  ## daily appended table
  dbWriteTable(mydb, "daily_consumption", daily_consumption, append=TRUE, row.names=F, overwrite=FALSE) # append table
  dbDisconnect(mydb)
