rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,RPushbullet,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

daily_occupancy_DB <- as.data.frame(tbl(con,"daily_occupancy"))
monthly_occupancy_DB <- as.data.frame(tbl(con,"monthly_occupancy"))

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                                        & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

family_servicepoint_Online <- family_servicepoint %>% dplyr::filter(online_status=="ACTIVE")
family_servicepoint_Offline <- family_servicepoint %>% dplyr::filter(online_status=="INACTIVE")

daily_occupancy_online <- daily_occupancy_DB %>% dplyr::filter(service_point_sn %in% family_servicepoint_Online$service_point_sn) %>%
                          dplyr::summarise(AvgDailyOccupancyRate=mean(occupancy_rate))

daily_occupancy_offline <- daily_occupancy_DB %>% dplyr::filter(service_point_sn %in% family_servicepoint_Offline$service_point_sn) %>%
  dplyr::summarise(AvgDailyOccupancyRate=mean(occupancy_rate))

monthly_occupancy_online <- monthly_occupancy_DB %>% dplyr::filter(service_point_sn %in% family_servicepoint_Online$service_point_sn) %>%
  dplyr::summarise(AvgmonthlyOccupancyRate=mean(occupancy_rate))

monthly_occupancy_offline <- monthly_occupancy_DB %>% dplyr::filter(service_point_sn %in% family_servicepoint_Offline$service_point_sn) %>%
  dplyr::summarise(AvgmonthlyOccupancyRate=mean(occupancy_rate))
