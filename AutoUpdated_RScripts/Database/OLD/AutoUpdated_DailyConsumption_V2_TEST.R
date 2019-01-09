rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

load("/srv/shiny-server/DataAnalyticsPortal/data/PunggolConsumption.RData")
rm(PunggolConsumption_MAINSUB,PunggolConsumption_MAIN)

# Establish connection
mydb <- dbConnect(PostgreSQL(), dbname="amrstaging",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")

con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")
leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))
family <- as.data.frame(tbl(con,"family")) %>% dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE")
# family = 528 ACTIVE including AHL, include ChildCare
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))
daily_consumption_DB <- as.data.frame(tbl(con,"daily_consumption"))

day <- as.Date("2017-01-02")
# last7days <- c(Sys.Date()-c(1:7))
# last3month <- month(floor_date(Sys.Date() - months(c(1,2,3)), "month"))
last7days <- c(day-c(1:7))
last3month <- c(12,11,10)

# length(unique(PunggolConsumption_SUB$service_point_sn))=534, including ChildCare, and including AHL.
DailyPunggolConsumption_last7days <- inner_join(PunggolConsumption_SUB,family_servicepoint,by="service_point_sn") %>%
  dplyr::filter(date(date_consumption) >= move_in_date) %>%  # take into consideration of move-in-date
  dplyr::mutate(date=date(date_consumption),wd=weekdays(date_consumption)) %>%
  group_by(service_point_sn,date,month,day,wd) %>%
  dplyr::filter(date %in% last7days) %>%
  dplyr::summarise(dailyconsumption_last7days=sum(adjusted_consumption)) %>%
  dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 
## should have 527 unique service_point_sn, excluding ChildCare, and including AHL.

DailyConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by="service_point_sn") %>%
  dplyr::mutate(date=date(date_consumption),wd=weekdays(date)) %>%
  group_by(service_point_sn,month,day,wd) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption)) %>%
  dplyr::filter(service_point_sn !="3101127564") # remove Child-Care

# average of the same day (e.g if today is Wednesday, then all the same 12* Wednesday) of last 3 months (individual) 
# extract daily consumption, combine with weekdays.

DailyConsumption_past3months_weekdays <- DailyConsumption %>%
  group_by(service_point_sn,wd) %>%
  dplyr::filter(month %in% last3month) %>%
  dplyr::summarise(Avg_3mth_wd=round(mean(DailyConsumption))) 
threshold=50
daily_consumption <- inner_join(DailyPunggolConsumption_last7days,DailyConsumption_past3months_weekdays,
                                by=c("service_point_sn","wd")) %>%
  group_by(service_point_sn,wd) %>%
  dplyr::mutate(overconsumption=dailyconsumption_last7days-(1+(threshold/100))*Avg_3mth_wd) %>%
  dplyr::mutate(overconsumption=round(ifelse(overconsumption<0,0,overconsumption))) %>%
  dplyr::mutate(nett_consumption=round(dailyconsumption_last7days-overconsumption)) %>%
  dplyr::filter(date==as.Date("2017-01-01")) %>%  
  dplyr::select_("service_point_sn","nett_consumption","overconsumption","date") %>%
  dplyr::rename(date_consumption=date)

leak_alarm$start_date <- as.Date(leak_alarm$start_date) 
leak_alarm$end_date <- as.Date(leak_alarm$end_date)  

daily_consumption_leak_alarm <- left_join(daily_consumption,leak_alarm,by="service_point_sn") 
daily_consumption_leak_alarm_NA <- daily_consumption_leak_alarm %>% dplyr::filter(is.na(status)) %>% as.data.frame()
daily_consumption_leak_alarm_Open <- daily_consumption_leak_alarm %>% dplyr::filter(status=="Open") %>% as.data.frame()
daily_consumption_leak_alarm_Close <- daily_consumption_leak_alarm %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(status=="Close") %>%
  dplyr::filter(end_date==max(end_date)) %>%
  as.data.frame()
daily_consumption_leak_alarm_OpenClose <- rbind(daily_consumption_leak_alarm_Open,daily_consumption_leak_alarm_Close) %>%
  group_by(service_point_sn) %>%
  dplyr::filter(start_date==max(start_date)) %>%
  as.data.frame()

daily_consumption_leak_alarm <- rbind(daily_consumption_leak_alarm_OpenClose,daily_consumption_leak_alarm_NA) %>%
  dplyr::mutate(is_leak=ifelse(is.na(status),FALSE,
                               ifelse(date_consumption==start_date,TRUE,FALSE)))
daily_consumption <- daily_consumption_leak_alarm %>%
  dplyr::select_("service_point_sn","nett_consumption","overconsumption","date_consumption","is_leak")

number_customers <- length(daily_consumption$service_point_sn) 

daily_consumption$id <- as.integer(seq(nrow(daily_consumption_DB)+1,
                                       nrow(daily_consumption_DB)+number_customers,1))

daily_consumption <- daily_consumption[,c(ncol(daily_consumption),c(1:ncol(daily_consumption)-1))]

daily_consumption$notification_status <- rep("new")
daily_consumption <- as.data.frame(daily_consumption)

## daily appended table
dbWriteTable(mydb, "daily_consumption", daily_consumption, append=TRUE, row.names=F, overwrite=FALSE) # append table
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_DailyConsumption V2 successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)