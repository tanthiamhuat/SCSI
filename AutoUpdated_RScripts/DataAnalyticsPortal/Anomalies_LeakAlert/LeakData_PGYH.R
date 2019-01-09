## leak start date and end date is on first day when min_5_flow > 0 and min_5_flow=0 respectively.

# Can you give me a distribution of the leak volumes of the 30 Punggol total unique households having leaks. 
# Ex. Household 1 = 20 litres, household 2 = 12 litres, …, household 30 = 22 litres.

# Same question for Yuhua 98 total unique households having leaks.

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

leak_alarm_PG <- as.data.frame(tbl(con,"leak_alarm")) %>% filter(site=="Punggol" & start_date >="2018-01-01")
unique(leak_alarm_PG$service_point_sn)
leak_alarm_YH <- as.data.frame(tbl(con,"leak_alarm")) %>% filter(site=="Yuhua" & start_date >="2018-04-01" & service_point_sn !="3100660792")
unique(leak_alarm_YH$service_point_sn)

leak_alarm_PG_distribution <- leak_alarm_PG %>% dplyr::group_by(service_point_sn) %>%
  dplyr::summarise(LeakVolume=round(sum(min5flow_ave*24*duration)))

leak_alarm_YH_distribution <- leak_alarm_YH %>% dplyr::group_by(service_point_sn) %>%
  dplyr::summarise(LeakVolume=round(sum(min5flow_ave*24*duration)))

write.csv(leak_alarm_PG_distribution,file="/srv/shiny-server/DataAnalyticsPortal/data/leak_PG.csv")
write.csv(leak_alarm_YH_distribution,file="/srv/shiny-server/DataAnalyticsPortal/data/leak_YH.csv")
