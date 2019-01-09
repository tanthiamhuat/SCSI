rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

## to add rows to daily_consumption table which those rows are missing on dates 2017-03-11 and 2017-03-12
# date1 <- as.Date("2017-03-12")  ## write data for 2017-03-11
# date2 <- as.Date("2017-03-13")  ## write data for 2017-03-12
# date0 <- as.Date("2017-05-14")  ## write data for 2017-05-13
date0 <- as.Date("2017-09-08")  ## write data for 2017-09-07

ptm <- proc.time()

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

#yesterday <- today()-2
#all_days <- as.data.frame(seq.Date(as.Date("2017-01-01"),yesterday,by="days")) 
all_days <- as.data.frame(date0) 
colnames(all_days) <- "Dates"

## to check dates of daily_consumption table which are missing
daily_consumption <- as.data.frame(tbl(con,"daily_consumption"))
full_set <- as.data.frame(unique(daily_consumption$service_point_sn))
colnames(full_set) <- "service_point_sn"

tmp1=as.data.frame(lapply(full_set,rep,nrow(all_days)))
tmp2=as.data.frame(lapply(all_days,rep,nrow(full_set)))
full_set_all_days <- cbind(tmp1,tmp2)

dates_available <- daily_consumption %>% 
  group_by(service_point_sn) %>% 
  dplyr::select_("service_point_sn","date_consumption") %>%
  as.data.frame()

dates_available$date_consumption <- as.Date(dates_available$date_consumption) + days(1)
missing_dates=anti_join(full_set_all_days,dates_available,by=c("Dates"="date_consumption","service_point_sn")) %>% arrange(Dates)

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=date(date_consumption),month=month(date_consumption)) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,date_consumption,day,month) %>%
  arrange(date_consumption)

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

# length(unique(PunggolConsumption_SUB$service_point_sn))=533, excluding ChildCare, and including AHL.
PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

date_past3months <- date0-months(3)

DailyConsumption <- PunggolConsumption %>%
  dplyr::mutate(date=date(date_consumption),wd=weekdays(date)) %>%
  group_by(service_point_sn,month,day,wd,date) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(service_point_sn !="3101127564" & n==24) # remove Child-Care, full count of 24 per day

# average of the same day (e.g if today is Wednesday, then all the same 12* Wednesday) of last 3 months (individual) 
# extract daily consumption, combine with weekdays.
DailyConsumption_past3months <- DailyConsumption %>%
  group_by(service_point_sn,wd) %>%
  dplyr::filter(wd==weekdays(date0-1) & date >= date_past3months) %>%
  dplyr::summarise(average_consumption_past3months=round(mean(DailyConsumption)))

leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))

daily_consumption_DB <- as.data.frame(tbl(con,"daily_consumption"))

last7days <- c(date0-c(1:7))

DailyPunggolConsumption_last7days <- PunggolConsumption %>%
  dplyr::filter(date(date_consumption) >= move_in_date) %>%  # take into consideration of move-in-date
  dplyr::mutate(date=date(date_consumption),wd=weekdays(date_consumption)) %>%
  group_by(service_point_sn,date,month,day,wd) %>%
  dplyr::filter(date %in% last7days) %>%
  dplyr::summarise(dailyconsumption_last7days=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(service_point_sn !="3101127564" & n==24) # exclude ChildCare, full count of 24 per day
## should have 527 unique service_point_sn, excluding ChildCare, and including AHL.

threshold=50
daily_consumption <- inner_join(DailyPunggolConsumption_last7days,DailyConsumption_past3months,
                                     by=c("service_point_sn","wd")) %>%
  group_by(service_point_sn,wd) %>%
  dplyr::mutate(overconsumption=dailyconsumption_last7days-(1+(threshold/100))*average_consumption_past3months) %>%
  dplyr::mutate(overconsumption=round(ifelse(overconsumption<0,0,overconsumption))) %>%
  dplyr::mutate(nett_consumption=round(dailyconsumption_last7days-overconsumption)) %>%
  dplyr::filter(date==date0-1) %>%  
  dplyr::select_("service_point_sn","nett_consumption","overconsumption","date") %>%
  dplyr::rename(date_consumption=date)

new_data <- daily_consumption %>% select(service_point_sn,date_consumption) %>% as.data.frame()

  daily_consumption <- inner_join(daily_consumption,new_data)
  leak_alarm$start_date <- as.Date(leak_alarm$start_date) 
  leak_alarm$end_date <- as.Date(leak_alarm$end_date)  
  
  leak_alarm <- leak_alarm %>% filter(service_point_sn %in% daily_consumption$service_point_sn)
  
  daily_consumption_leak_alarm <- left_join(daily_consumption,leak_alarm,by=c("service_point_sn")) %>%
    dplyr::mutate(is_leak=ifelse(date_consumption==start_date+3),TRUE,FALSE) 
  
  daily_consumption <- daily_consumption_leak_alarm %>%
    dplyr::select_("service_point_sn","nett_consumption","overconsumption","date_consumption","is_leak")
  
  number_customers <- length(daily_consumption$service_point_sn) 
  
  daily_consumption$id <- as.integer(seq(max(daily_consumption_DB$id)+1,
                                         max(daily_consumption_DB$id)+number_customers,1))
  
  daily_consumption <- daily_consumption[,c(ncol(daily_consumption),c(1:ncol(daily_consumption)-1))]
  
  daily_consumption <- as.data.frame(daily_consumption)
  
  colnames(daily_consumption)[which(colnames(daily_consumption)=="date_consumption")] <- "date_consumption"
  
  daily_consumption <- daily_consumption %>% dplyr::select_("id","service_point_sn","nett_consumption",
                                                            "overconsumption","date_consumption","is_leak")
  ## daily appended table
  dbWriteTable(mydb, "daily_consumption", daily_consumption, append=TRUE, row.names=F, overwrite=FALSE) # append table
  dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_UserTrends_DailyConsumption successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)