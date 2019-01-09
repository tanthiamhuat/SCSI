rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.RData")
today <- today()

PunggolConsumption_SUB <- Punggol_2017 %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

meter <- as.data.frame(tbl(con,"meter"))
servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate")) %>%
                      dplyr::select_("service_point_sn","block","meter_sn")

overconsumption_alarm_DB <- as.data.frame(tbl(con,"overconsumption_alarm"))

last7days <- c(Sys.Date()-c(1:7))
past3months <- unique(substr(seq(Sys.Date()-days(90),Sys.Date()-days(30),by=1),1,7))

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

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
  dplyr::filter(as.numeric(difftime(today,move_in_date,units="days"))>90) %>%
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

overconsumption_alarm <- as.data.frame(Overconsumption$service_point_sn)
colnames(overconsumption_alarm)[1] <- "service_point_sn"

number_customers <- length(overconsumption_alarm$service_point_sn)  
overconsumption_alarm$id <- as.integer(seq(max(overconsumption_alarm_DB$id)+1,
                                        max(overconsumption_alarm_DB$id)+number_customers,1))

overconsumption_alarm$overconsumption_date <- last7days[1]
overconsumption_alarm$date_created <- rep(today())

overconsumption_alarm <- overconsumption_alarm %>% dplyr::select_("id","service_point_sn","overconsumption_date","date_created")
   
overconsumption_alarm$notification_status <- rep("new")

WeeklyOccupancy <- as.data.frame(tbl(con,"weekly_occupancy")) %>% filter(lastupdated == max(lastupdated)) %>%
                   filter(occupancy_rate >50) %>% dplyr::select_("service_point_sn")

overconsumption_alarm <- overconsumption_alarm %>% filter(service_point_sn %in% WeeklyOccupancy$service_point_sn)

OpenLeak <- as.data.frame(tbl(con,"leak_alarm") %>% filter(status=="Open")) %>% dplyr::select_("service_point_sn")

overconsumption_alarm <- overconsumption_alarm %>% filter(!(service_point_sn %in% OpenLeak$service_point_sn))

dbWriteTable(mydb, "overconsumption_alarm", overconsumption_alarm, append=TRUE, row.names=F, overwrite=FALSE)
dbDisconnect(mydb)

# List of Overconsumption : to be populated in the “Anomalies and Leak Alert” section 
# (similar presentation as the “list of leaks”). Display block#, SP-SN and Meter-SN and date.

overconsumption_alarm_DB <- as.data.frame(tbl(con,"overconsumption_alarm"))

overconsumption_DAP <- inner_join(overconsumption_alarm_DB,servicepoint_meter,by="service_point_sn") %>%
                       dplyr::arrange(desc(overconsumption_date)) %>%
                       dplyr::select_("block","service_point_sn","meter_sn","overconsumption_date")

family_servicepoint_overconsumption_DAP <- inner_join(family_servicepoint,overconsumption_DAP,by=c("service_point_sn","block")) %>%
  dplyr::select_("id","service_point_sn","meter_sn","block","overconsumption_date") %>%
  dplyr::rename(family_id=id) %>% 
  dplyr::arrange(desc(overconsumption_date))

save(family_servicepoint_overconsumption_DAP, file="/srv/shiny-server/DataAnalyticsPortal/data/overconsumption.RData")
write.csv(family_servicepoint_overconsumption_DAP,"/srv/shiny-server/DataAnalyticsPortal/data/overconsumption.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_OverConsumption_Alarm successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
