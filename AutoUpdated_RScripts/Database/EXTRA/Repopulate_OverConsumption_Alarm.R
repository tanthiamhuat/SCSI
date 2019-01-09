rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

#Punggol_All <- Punggol_All %>% filter(adjusted_date >="2016-09-01")

#days <- seq(as.Date("2017-01-01"),as.Date("2017-07-16"),by=7)
days <- seq(as.Date("2017-08-13"),as.Date("2017-08-13"),by=7)
overconsumption_alarm_FINAL <- list()

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

PunggolConsumption_SUB <- Punggol_All %>%
                          dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
                          dplyr::mutate(day=D,month=M) %>%                  
                          select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
                          arrange(adjusted_date)

family <- as.data.table(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))
servicepoint <- as.data.table(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

meter <- as.data.frame(tbl(con,"meter"))
servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate")) %>%
                      dplyr::select_("service_point_sn","block","meter_sn")

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

WeeklyOccupancy <- as.data.frame(tbl(con,"weekly_occupancy")) %>% filter(lastupdated == max(lastupdated)) %>%
                   filter(occupancy_rate >50) %>% dplyr::select_("service_point_sn")

OpenLeak <- as.data.frame(tbl(con,"leak_alarm") %>% filter(status=="Open")) %>% dplyr::select_("service_point_sn")


overconsumption_alarm_DB <- as.data.frame(tbl(con,"overconsumption_alarm"))

for (i in 1:length(days))
{
  last7days <- c(days[i]+1-c(1:7))
  past3months <- unique(substr(seq(days[i]+1-days(90),days[i]+1-days(30),by=1),1,7))

DailyPunggolConsumption_last7days <- PunggolConsumption %>%
  dplyr::filter(date(adjusted_date) >= move_in_date) %>%  # take into consideration of move-in-date
  dplyr::mutate(date=date(adjusted_date)) %>%
  group_by(service_point_sn,date) %>%
  dplyr::filter(date %in% last7days) %>%
  dplyr::summarise(dailyconsumption_last7days=sum(adjusted_consumption,na.rm=TRUE)) %>%
  group_by(service_point_sn) %>%
  dplyr::summarise(averagedailyconsumption_last7days=round(mean(dailyconsumption_last7days))) 

DailyPunggolConsumption_past3months <- PunggolConsumption %>%
  dplyr::filter(date(adjusted_date) >= move_in_date) %>%  # take into consideration of move-in-date
  dplyr::filter(as.numeric(difftime(days[i],move_in_date,units="days"))>90) %>%
  dplyr::mutate(date=date(adjusted_date),yearmonth=substr(date,1,7)) %>%
  group_by(service_point_sn,date,yearmonth) %>%
  dplyr::filter(yearmonth %in% past3months) %>%
  dplyr::summarise(dailyconsumption_last3months=sum(adjusted_consumption,na.rm=TRUE)) %>%
  group_by(service_point_sn) %>%
  dplyr::summarise(averagedailyconsumption_last3months=round(mean(dailyconsumption_last3months))) 

threshold=50
  Overconsumption <- inner_join(DailyPunggolConsumption_last7days,DailyPunggolConsumption_past3months,
                                        by=c("service_point_sn")) %>%
    group_by(service_point_sn) %>%
    dplyr::mutate(overconsumption=averagedailyconsumption_last7days-(1+(threshold/100))*averagedailyconsumption_last3months) %>%
    dplyr::mutate(overconsumption=round(ifelse(overconsumption<0,0,overconsumption))) %>%
    dplyr::filter(overconsumption>0)
  
  Overconsumption$overconsumption_date <- last7days[1]
  Overconsumption$date_created <- days[i]+1
  
  overconsumption_alarm <- Overconsumption %>% dplyr::select_("service_point_sn","overconsumption_date","date_created")
  
  overconsumption_alarm$notification_status <- rep("new")
  
  overconsumption_alarm <- overconsumption_alarm %>% filter(service_point_sn %in% WeeklyOccupancy$service_point_sn)

  overconsumption_alarm_FINAL[[i]] <- overconsumption_alarm %>% filter(!(service_point_sn %in% OpenLeak$service_point_sn))
}

overconsumption_alarm_FINAL1 <- as.data.frame(rbindlist(overconsumption_alarm_FINAL))

total_rows <- nrow(overconsumption_alarm_FINAL1)

overconsumption_alarm_FINAL1$id <- as.integer(seq(max(overconsumption_alarm_DB$id)+1,
                                           max(overconsumption_alarm_DB$id)+total_rows,1))

overconsumption_alarm_FINAL1 <- overconsumption_alarm_FINAL1[,c(ncol(overconsumption_alarm_FINAL1),c(1:ncol(overconsumption_alarm_FINAL1)-1))]

#dbWriteTable(mydb, "overconsumption_alarm", overconsumption_alarm_FINAL1, append=FALSE, row.names=F, overwrite=TRUE)
dbWriteTable(mydb, "overconsumption_alarm", overconsumption_alarm_FINAL1, append=TRUE, row.names=F, overwrite=FALSE)
dbDisconnect(mydb)
