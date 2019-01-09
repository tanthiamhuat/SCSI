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

consumption_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2017.fst",as.data.table=TRUE)

servicepoint <- as.data.table(tbl(con,"service_point"))

# set the ON clause as keys of the tables:
setkey(consumption_2017,id_service_point)
setkey(servicepoint,id)

consumption_2017_servicepoint <- consumption_2017[servicepoint, nomatch=0]
consumption_2017_servicepoint <- consumption_2017_servicepoint[, .(service_point_sn,block,floor,unit,room_type,site,meter_type,
                                                                   interpolated_consumption,adjusted_consumption,date_consumption)]

consumption_thisyear_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_thisyear_servicepoint.fst",as.data.table=TRUE)

consumption_servicepoint <- rbind(consumption_2017_servicepoint,consumption_thisyear_servicepoint)

PunggolYuhua_SUB <- consumption_servicepoint[!(room_type %in% c("NIL","HDBCD","OTHER","Nil")) & !(is.na(room_type)) &
                                               meter_type =="SUB" & site %in% c("Punggol","Yuhua","Whampoa") &
                                               !is.na(adjusted_consumption)]

PunggolYuhua_SUB <- PunggolYuhua_SUB[,c("Date","wd"):= list(date(date_consumption),weekdays(date(date_consumption)))]

#days_range <- seq(as.Date("2017-01-02"),as.Date("2018-05-12"),by=1)  
## daily_consumption: date_consumption from 2017-01-01 to 2018-05-11

days_range <- seq(as.Date("2018-01-02"),as.Date("2018-02-01"),by=1) ## testing
daily_consumption_list <- list()

for (i in 1:length(days_range))
{
  family <- as.data.frame(tbl(con,"family")) %>% 
            dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
  servicepoint <- as.data.frame(tbl(con,"service_point"))
  family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

  PunggolYuhuaConsumption <- inner_join(PunggolYuhua_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

  date_past3months <- days_range[i]-days(90)

  DailyConsumption <- PunggolYuhua_SUB %>%
    group_by(service_point_sn,wd,Date) %>%
    dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
    dplyr::filter(n==24) 

# average of the same day (e.g if today is Wednesday, then all the same 12* Wednesday) of last 3 months (individual) 
# extract daily consumption, combine with weekdays.
  DailyConsumption_past3months <- DailyConsumption %>%
    group_by(service_point_sn,wd) %>%
    dplyr::filter(wd==weekdays(days_range[i]-1) & Date >= date_past3months) %>%
    dplyr::summarise(average_consumption_past3months=round(mean(DailyConsumption)))

last7days <- c(days_range[i]-c(1:7))

DailyPunggolYuhuaConsumption_last7days <- PunggolYuhuaConsumption %>%
  dplyr::filter(Date >= move_in_date) %>%  # take into consideration of move-in-date
  group_by(service_point_sn,Date,wd) %>%
  dplyr::filter(Date %in% last7days) %>%
  dplyr::summarise(dailyconsumption_last7days=sum(interpolated_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(n==24) 

threshold=50
daily_consumption_list[[i]] <- inner_join(DailyPunggolYuhuaConsumption_last7days,DailyConsumption_past3months,
                                     by=c("service_point_sn","wd")) %>%
  group_by(service_point_sn,wd) %>%
  dplyr::mutate(overconsumption=dailyconsumption_last7days-(1+(threshold/100))*average_consumption_past3months) %>%
  dplyr::mutate(overconsumption=round(ifelse(overconsumption<0,0,overconsumption))) %>%
  dplyr::mutate(nett_consumption=round(dailyconsumption_last7days-overconsumption)) %>%
  dplyr::filter(Date==days_range[i]-1) %>%  
  dplyr::select_("service_point_sn","nett_consumption","overconsumption","Date") 
}

daily_consumption <- as.data.frame(rbindlist(daily_consumption_list))

  leak_alarm_YH <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/Leak_Yuhua.csv")
  leak_alarm_YH_open <- leak_alarm_YH %>% filter(status=="Open")
  
  leak_alarm_PG <- as.data.frame(tbl(con,"leak_alarm"))
  leak_alarm_PG_open <- leak_alarm_PG %>% dplyr::filter(status=="Open" & site=="Punggol")
  
  leak_alarm_open <- c(leak_alarm_PG_open$service_point_sn,leak_alarm_YH_open$service_point_sn)

  daily_consumption <- daily_consumption %>% 
                       dplyr::mutate(is_leak=ifelse(service_point_sn %in% leak_alarm_open,TRUE,FALSE)) 
  
  daily_consumption["wd"] <- NULL
  colnames(daily_consumption)[4] <- "date_consumption"
 
  daily_consumption <- daily_consumption[!duplicated(daily_consumption),] # remove duplicates 
 
  daily_consumption$id <- as.integer(rownames(daily_consumption))
  
  daily_consumption <- daily_consumption %>% 
                       dplyr::select_("id","service_point_sn","nett_consumption","overconsumption","date_consumption","is_leak")
  
  # dbSendQuery(mydb, "delete from daily_consumption")
  # 
  # dbWriteTable(mydb, "daily_consumption", daily_consumption, append=FALSE, row.names=F, overwrite=TRUE)
  # dbDisconnect(mydb)
  
time_taken <- proc.time() - ptm
ans <- paste("Overwrite_DailyConsumption_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)
  
cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)