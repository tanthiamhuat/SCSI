## Take into consideration of Leak (leak_alarm, start_date) and Monthly Occupancy > 20%
## 

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
rm(PunggolConsumption_MAINSUB)

# Establish connection
mydb <- dbConnect(PostgreSQL(), dbname="amrstaging",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")

con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")
family <- as.data.frame(tbl(con,"family")) %>% dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE")
servicepoint <- as.data.frame(tbl(con,"service_point"))
leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))
monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy"))
meter <- as.data.frame(tbl(con,"meter"))
servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate")) %>%
                      dplyr::select_("service_point_sn","block","meter_sn")
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))
overconsumption_alarm_DB <- as.data.frame(tbl(con,"overconsumption_alarm"))

last7days <- c(Sys.Date()-c(1:7))
last3month <- month(floor_date(Sys.Date() - months(c(1,2,3)), "month"))

DailyPunggolConsumption_last7days <- inner_join(PunggolConsumption_SUB,family_servicepoint,by="service_point_sn") %>%
  dplyr::filter(date(date_consumption) >= move_in_date) %>%  # take into consideration of move-in-date
  dplyr::mutate(date=date(date_consumption),wd=weekdays(date_consumption)) %>%
  group_by(service_point_sn,date,month,day,wd) %>%
  dplyr::filter(date %in% last7days) %>%
  dplyr::summarise(dailyconsumption_last7days=sum(adjusted_consumption)) %>%
  group_by(service_point_sn) %>%
  dplyr::summarise(averagedailyconsumption_last7days=round(mean(dailyconsumption_last7days))) %>%
  dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 

DailyPunggolConsumption_past3months <- inner_join(PunggolConsumption_SUB,family_servicepoint,by="service_point_sn") %>%
  dplyr::filter(date(date_consumption) >= move_in_date) %>%  # take into consideration of move-in-date
  dplyr::mutate(date=date(date_consumption),wd=weekdays(date_consumption)) %>%
  group_by(service_point_sn,date,month,day,wd) %>%
  dplyr::filter(month %in% last3month) %>%
  dplyr::summarise(dailyconsumption_last3months=sum(adjusted_consumption)) %>%
  group_by(service_point_sn) %>%
  dplyr::summarise(averagedailyconsumption_last3months=round(mean(dailyconsumption_last3months))) %>%
  dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 

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
overconsumption_alarm$id <- as.integer(seq(nrow(overconsumption_alarm_DB)+1,
                                        nrow(overconsumption_alarm_DB)+number_customers,1))

overconsumption_alarm$overconsumption_date <- last7days[1]
overconsumption_alarm$date_created <- rep(today())
  
overconsumption_alarm <- overconsumption_alarm %>% dplyr::select_("id","service_point_sn","overconsumption_date","date_created")
   
overconsumption_alarm$notification_status <- rep("new")

dbWriteTable(mydb, "overconsumption_alarm", overconsumption_alarm, append=TRUE, row.names=F, overwrite=FALSE)
dbDisconnect(mydb)

# List of Overconsumption : to be populated in the “Anomalies and Leak Alert” section 
# (similar presentation as the “list of leaks”). Display block#, SP-SN and Meter-SN and date.

overconsumption_alarm_DB <- as.data.frame(tbl(con,"overconsumption_alarm"))

overconsumption_DAP <- inner_join(overconsumption_alarm_DB,servicepoint_meter,by="service_point_sn") %>%
                       dplyr::select_("block","service_point_sn","meter_sn","overconsumption_date")

save(overconsumption_DAP, file="/srv/shiny-server/DataAnalyticsPortal/data/overconsumption.RData")
write.csv(overconsumption_DAP,"/srv/shiny-server/DataAnalyticsPortal/data/overconsumption.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_OverConsumption_Alarm successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
