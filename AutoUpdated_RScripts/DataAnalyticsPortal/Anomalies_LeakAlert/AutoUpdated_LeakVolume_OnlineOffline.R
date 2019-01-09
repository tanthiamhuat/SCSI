rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,xts)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

flow <- as.data.frame(tbl(con,"flow"))

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                                        & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")) %>%
                filter(!is.na(floor) & !is.na(unit)) # only Punggol HH units, exclude main meters, include HDBCD + HL
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))
family_servicepoint_online <- family_servicepoint %>% dplyr::filter(online_status=="ACTIVE" & site=="Punggol")
family_servicepoint_offline <- family_servicepoint %>% dplyr::filter(online_status=="INACTIVE" & site=="Punggol")

meter <- as.data.frame(tbl(con,"meter"))

servicepoint <- servicepoint %>% filter(!is.na(floor) & !is.na(unit)) # only Punggol HH units, exclude main meters, include HDBCD + HL

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate"))

flow_servicepoint_meter <- inner_join(servicepoint_meter,flow,by=c("id.x"="id_service_point"))

flow_servicepoint_meter <- subset(flow_servicepoint_meter, select = c("service_point_sn","block","meter_sn","min_5_flow","flow_date"))

LeakVolumePerDay_Total <- flow_servicepoint_meter %>% 
  group_by(date(flow_date)) %>%
  filter(date(flow_date)>="2016-03-18") %>%
  mutate(LeakVolumePerDay=min_5_flow*24) %>%
  dplyr::summarise(LeakVolumePerDay_Total=sum(LeakVolumePerDay))
colnames(LeakVolumePerDay_Total)[1] <- "Date"

LeakVolumePerDay_Online <- flow_servicepoint_meter %>% 
                           group_by(date(flow_date)) %>%
                           filter(date(flow_date)>="2016-03-18" & service_point_sn %in% family_servicepoint_online$service_point_sn) %>%
                           mutate(LeakVolumePerDay=min_5_flow*24) %>%
                           dplyr::summarise(LeakVolumePerDay_Online=sum(LeakVolumePerDay))
colnames(LeakVolumePerDay_Online)[1] <- "Date"

LeakVolumePerDay_Offline <- flow_servicepoint_meter %>% 
                            group_by(date(flow_date)) %>%
                            filter(date(flow_date)>="2016-03-18" & service_point_sn %in% family_servicepoint_offline$service_point_sn) %>%
                            mutate(LeakVolumePerDay=min_5_flow*24) %>%
                            dplyr::summarise(LeakVolumePerDay_Offline=sum(LeakVolumePerDay))
colnames(LeakVolumePerDay_Offline)[1] <- "Date"

Updated_DateTime_LeakVolume <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

LeakVolumePerDay_Online_xts <- xts(LeakVolumePerDay_Online$LeakVolumePerDay_Online,LeakVolumePerDay_Online$Date)
LeakVolumePerDay_Offline_xts <- xts(LeakVolumePerDay_Offline$LeakVolumePerDay_Offline,LeakVolumePerDay_Offline$Date)
LeakVolumePerDay_Total_xts <- xts(LeakVolumePerDay_Total$LeakVolumePerDay_Total,LeakVolumePerDay_Total$Date)

save(LeakVolumePerDay_Online_xts,LeakVolumePerDay_Offline_xts,
     LeakVolumePerDay_Total_xts,Updated_DateTime_LeakVolume,file="/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay_OnlineOfflineTotal.RData")

LeakVolumePerDay_OnlineOfflineTotal <- merge(merge(LeakVolumePerDay_Online,LeakVolumePerDay_Offline,by="Date",all=TRUE),LeakVolumePerDay_Total,by="Date",all=TRUE)
write.csv(LeakVolumePerDay_OnlineOfflineTotal,"/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay_OnlineOfflineTotal.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_LeakVolume_OnlineOffline successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
