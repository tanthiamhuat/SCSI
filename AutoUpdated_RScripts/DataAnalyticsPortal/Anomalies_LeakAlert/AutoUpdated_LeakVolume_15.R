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
servicepoint <- as.data.frame(tbl(con,"service_point"))
meter <- as.data.frame(tbl(con,"meter"))

servicepoint <- servicepoint %>% filter(!is.na(floor) & !is.na(unit)) # only Punggol HH units, exclude main meters, include HDBCD + HL

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate"))

flow_servicepoint_meter <- inner_join(servicepoint_meter,flow,by=c("id.x"="id_service_point"))

flow_servicepoint_meter <- subset(flow_servicepoint_meter, select = c("service_point_sn","block","meter_sn","min_15_flow","flow_date"))

LeakVolume15PerDay <- flow_servicepoint_meter %>% 
                    group_by(date(flow_date)) %>%
                    filter(date(flow_date)>="2016-03-18") %>%
                    mutate(LeakVolumePerDay=min_15_flow*24) %>%
                    dplyr::summarise(TotalLeakVolume15PerDay=sum(LeakVolumePerDay))
colnames(LeakVolume15PerDay)[1] <- "Date"

LeakVolume15PerDay_xts <- xts(LeakVolume15PerDay$TotalLeakVolume15PerDay,LeakVolume15PerDay$Date)
save(LeakVolume15PerDay_xts,file="/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay15.RData")
write.csv(LeakVolume15PerDay,"/srv/shiny-server/DataAnalyticsPortal/data/LeakVolume15PerDay.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_LeakVolume_15 successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
