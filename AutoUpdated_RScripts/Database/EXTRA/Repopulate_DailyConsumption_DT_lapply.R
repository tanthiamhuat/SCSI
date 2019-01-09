rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

daily_consumption_DB <- as.data.frame(tbl(proddb,"daily_consumption"))
leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))

consumption_thisyear_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_thisyear_servicepoint.fst",as.data.table=TRUE)

consumption_servicepoint <- consumption_thisyear_servicepoint
PunggolYuhua_SUB <- consumption_servicepoint[!(room_type %in% c("NIL","HDBCD","OTHER","Nil")) & !(is.na(room_type)) &
                                               meter_type =="SUB" & site %in% c("Punggol","Yuhua","Whampoa") &
                                               !is.na(adjusted_consumption)]

PunggolYuhua_SUB <- PunggolYuhua_SUB[,c("Date","wd"):= list(date(date_consumption),weekdays(date(date_consumption)))]

#days_range <- seq(as.Date("2017-01-02"),as.Date("2018-05-12"),by=1)  
## daily_consumption: date_consumption from 2017-01-01 to 2018-05-11

days_range <- seq(as.Date("2018-01-02"),as.Date("2018-08-01"),by=1) ## testing

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

PunggolYuhuaConsumption <- inner_join(PunggolYuhua_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

DailyConsumption <- PunggolYuhua_SUB %>%
  group_by(service_point_sn,wd,Date) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(n==24) 

daily_consumption_list <- lapply(days_range,function(day){
  date_past3months <- day-days(90)
  # average of the same day (e.g if today is Wednesday, then all the same 12* Wednesday) of last 3 months (individual) 
  # extract daily consumption, combine with weekdays.
  DailyConsumption_past3months <- DailyConsumption %>%
    group_by(service_point_sn,wd) %>%
    dplyr::filter(wd==weekdays(day-1) & Date >= date_past3months) %>%
    dplyr::summarise(average_consumption_past3months=round(mean(DailyConsumption)))
  
  last7days <- c(day-c(1:7))
  DailyPunggolYuhuaConsumption_last7days <- PunggolYuhuaConsumption %>%
                                            dplyr::filter(Date >= move_in_date) %>%  # take into consideration of move-in-date
                                            group_by(service_point_sn,Date,wd) %>%
                                            dplyr::filter(Date %in% last7days) %>%
                                            dplyr::summarise(dailyconsumption_last7days=sum(interpolated_consumption,na.rm=TRUE),n = n()) %>%
                                            dplyr::filter(n==24) 

  threshold=50
  return(inner_join(DailyPunggolYuhuaConsumption_last7days,DailyConsumption_past3months,
                                     by=c("service_point_sn","wd")) %>%
         group_by(service_point_sn,wd) %>%
         dplyr::mutate(overconsumption=dailyconsumption_last7days-(1+(threshold/100))*average_consumption_past3months) %>%
         dplyr::mutate(overconsumption=round(ifelse(overconsumption<0,0,overconsumption))) %>%
         dplyr::mutate(nett_consumption=round(dailyconsumption_last7days-overconsumption)) %>%
         dplyr::filter(Date==day-1) %>%  
         dplyr::select_("service_point_sn","nett_consumption","overconsumption","Date") 
  )
})

daily_consumption <- rbindlist(daily_consumption_list)

leak_alarm$start_date <- as.Date(leak_alarm$start_date)

leak_alarm1 <- leak_alarm %>% filter(service_point_sn %in% daily_consumption$service_point_sn)

names(daily_consumption)[names(daily_consumption) == 'Date'] <- 'date_consumption'
daily_consumption_leak_alarm <- left_join(daily_consumption,leak_alarm1,by=c("service_point_sn")) %>%
                                dplyr::group_by(service_point_sn) %>%
                                dplyr::mutate(is_leak=ifelse(date_consumption==start_date+3|
                                                             date_consumption==start_date+10,TRUE,FALSE))

daily_consumption_leak_alarm[which(is.na(daily_consumption_leak_alarm$is_leak)),ncol(daily_consumption_leak_alarm)] <- FALSE

daily_consumption <- daily_consumption_leak_alarm %>%
  dplyr::select_("service_point_sn","nett_consumption","overconsumption","date_consumption","is_leak") %>% 
  dplyr::arrange(date_consumption,desc(is_leak)) %>%
  unique() 
 
daily_consumption <- daily_consumption[!duplicated(daily_consumption[1:4]),] # remove duplicates 

total_rows <- nrow(daily_consumption)

daily_consumption$id <- as.integer(seq(1,total_rows,1))

daily_consumption <- daily_consumption[,c(ncol(daily_consumption),c(1:ncol(daily_consumption)-1))]

daily_consumption <- as.data.frame(daily_consumption)

save(daily_consumption,file="/srv/shiny-server/DataAnalyticsPortal/data/daily_consumption_20180101_20180731.RData")

dbWriteTable(proddb,"daily_consumption",daily_consumption, append=FALSE, row.names=F, overwrite=TRUE) # overwrite table
dbWriteTable(mydb,"daily_consumption",daily_consumption, append=FALSE, row.names=F, overwrite=TRUE) # overwrite table
dbDisconnect(proddb)
  
time_taken <- proc.time() - ptm
ans <- paste("Apend_DailyConsumption_DT_lapply successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)
  
cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)