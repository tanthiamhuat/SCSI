rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

today <- today()
index_StartEnd <- as.data.frame(tbl(con,"index") %>% filter(date(current_index_date) >= "2018-04-01" & date(current_index_date) <= "2018-09-12"))

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_available <- servicepoint %>% dplyr::select_("id","site","block","service_point_sn") %>%
                          filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")
servicepoint_available_Punggol <- servicepoint_available %>% filter(site=="Punggol")
servicepoint_available_Yuhua <- servicepoint_available %>% filter(site=="Yuhua")
servicepoint_available_Tuas <- servicepoint_available %>% filter(site=="Tuas")

servicepoint_available_Punggol_Blocks <- servicepoint_available_Punggol %>% 
                                         group_by(block) %>%
                                         dplyr::summarise(MeterCount=n())

servicepoint_available_Yuhua_Blocks <- servicepoint_available_Yuhua %>% 
                                       group_by(block) %>%
                                       dplyr::summarise(MeterCount=n())

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

servicepoint_meter <- inner_join(servicepoint_available,meter,by=c("service_point_sn"="id_real_estate"))

index_servicepoint_meter <- inner_join(servicepoint_meter,index_StartEnd,by=c("id.x"="id_service_point"))

index_data_hourly <- index_servicepoint_meter %>%
  select(meter_sn,current_index_date,block,site) %>%
  mutate(date=substr(current_index_date,1,10),hour=hour(current_index_date)) %>%
  dplyr::filter(meter_sn!="WJ003984A") # exclude AHL

index_data_hourly$date <- as.Date(index_data_hourly$date)

colnames(index_data_hourly) <- c("MeterSerialNumber","ReadingDate","block","site","date","hour")

index_data_daily <- index_data_hourly %>%
                    select(MeterSerialNumber,ReadingDate,block,site) %>%
                    dplyr::mutate(date=substr(ReadingDate,1,10),hour=hour(ReadingDate))

DailyIndexReadingRate <- index_data_daily %>%
                         group_by(date) %>%
                         dplyr::summarise(TotalMeters=sum(n_distinct(MeterSerialNumber))) %>%
                         dplyr::mutate(DailyIndexReadingRate=round(TotalMeters/(nrow(servicepoint_available_Punggol)+
                                                                                nrow(servicepoint_available_Tuas)+
                                                                                nrow(servicepoint_available_Yuhua))*100))
  
HourlyIndexReadingRate <- index_data_hourly %>% 
                          group_by(date,hour) %>%
                          dplyr::summarise(CountPerHour=n())%>%
                          group_by(date) %>%
                          dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                           HourlyIndexReadingRate=round(MeanCountPerHour/(nrow(servicepoint_available_Punggol)+
                                                                                    nrow(servicepoint_available_Tuas)+
                                                                                    nrow(servicepoint_available_Yuhua))*100))

DailyHourlyIndexReadingRate <- cbind(DailyIndexReadingRate[,c(1,3)],HourlyIndexReadingRate[,3]) 

write.csv(DailyHourlyIndexReadingRate,file="/srv/shiny-server/DataAnalyticsPortal/data/DailyHourlyIndexReadingRate.csv")
write.csv(index_data_daily,index_data_hourly,file="/srv/shiny-server/DataAnalyticsPortal/data/index_data_StartEnd.RData")
