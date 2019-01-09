rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,xts,tidyr)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

last12months <- today()-360

flow <- as.data.frame(tbl(con,"flow"))

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                                        & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))

servicepoint <- as.data.frame(tbl(con,"service_point")) %>% dplyr::filter(!service_point_sn %in% c("3100507837M","3100507837B","3100660792")) %>%
                filter(!is.na(floor) & !is.na(unit)) # only Punggol HH & YH units, exclude main meters, include HDBCD, exclude AHL
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))

meter <- as.data.frame(tbl(con,"meter"))

servicepoint <- servicepoint %>% filter(!is.na(floor) & !is.na(unit)) # only Punggol HH units, exclude main meters, include HDBCD + HL

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate"))

flow_servicepoint_meter <- inner_join(servicepoint_meter,flow,by=c("id.x"="id_service_point"))

flow_servicepoint_meter <- subset(flow_servicepoint_meter, select = c("service_point_sn","block","meter_sn","min_5_flow","flow_date"))

LeakVolumePerDay_Min5 <- flow_servicepoint_meter %>% 
  group_by(date(flow_date)) %>%
  filter(date(flow_date)>=last12months) %>%
  mutate(LeakVolumePerDay=min_5_flow*24) %>%
  dplyr::summarise(Punggol=sum(LeakVolumePerDay))
colnames(LeakVolumePerDay_Min5)[1] <- "Date"

load("/srv/shiny-server/DataAnalyticsPortal/data/MinCons_60.RData")
LeakVolumePerDay_Min60 <- Yuhua_SUB_MinCon %>% dplyr::group_by(Date) %>%
                           dplyr::summarise(Yuhua=sum(MinCons)) 

LeakVolumePerDay <- inner_join(LeakVolumePerDay_Min5,LeakVolumePerDay_Min60,by="Date")
LeakVolumePerDay_xts <- xts(LeakVolumePerDay,LeakVolumePerDay$Date)

LeakVolumePerDayPerPunggolBlock <- flow_servicepoint_meter %>% 
  dplyr::mutate(Date=date(flow_date)) %>%
  group_by(Date,block) %>%
  filter(date(flow_date)>=last12months) %>%
  mutate(LeakVolumePerDay=min_5_flow*24) %>%
  dplyr::summarise(LeakVolume=sum(LeakVolumePerDay))

LeakVolumePerDayPerYuhuaBlock <- Yuhua_SUB_MinCon %>% dplyr::group_by(Date,block) %>%
  dplyr::summarise(LeakVolume=sum(MinCons))  

LeakVolumePerDayPerBlock <- rbind(LeakVolumePerDayPerPunggolBlock,LeakVolumePerDayPerYuhuaBlock)
LeakVolumePerDayPerBlock_wide <- spread(LeakVolumePerDayPerBlock,block,LeakVolume)
LeakVolumePerDayPerBlock_xts <- xts(LeakVolumePerDayPerBlock_wide,LeakVolumePerDayPerBlock_wide$Date)

Updated_DateTime_LeakVolume <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(LeakVolumePerDay_xts,LeakVolumePerDayPerBlock_xts,Updated_DateTime_LeakVolume,
     file="/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay_Min5Min60.RData")

write.csv(LeakVolumePerDayPerBlock,file="/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDayPerBlock.csv")

LeakVolumePerDay$TotalLeakVolumePerDay <- LeakVolumePerDay$Punggol+LeakVolumePerDay$Yuhua
write.csv(LeakVolumePerDay,file="/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay.csv")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_LeakVolume_Min5Min60 successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)
