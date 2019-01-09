## leak start date and end date is on first day when min_5_flow > and min_5_flow=0 respectively.
rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

# Leak turn around time P2 vs P3 for Online/Offline group

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

# Phase_2 – leak alarm only (from 2016-04-20 to 2017-06-10)
# Phase_3 – leak alarm + app (> 2017-06-10)

# Average duration of the leaks and 
Punggol_Leak <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/leak_alarm_Punggol.csv") %>% filter(duration <400)
Punggol_Leak$end_date <- as.Date(Punggol_Leak$end_date)

servicepoint <- as.data.frame(tbl(con,"service_point"))
family <- as.data.frame(tbl(con,"family"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))
family_servicepoint_online <- family_servicepoint %>% dplyr::filter(online_status=="ACTIVE")

turnAround_P2_Online <- Punggol_Leak %>%
   dplyr::filter(online_status=="ACTIVE" & status=="Close" & end_date >="2016-04-20" & end_date <="2017-06-10") %>%
   dplyr::summarise(average_duration = mean(duration))

turnAround_P2_Offline <- Punggol_Leak %>%
   dplyr::filter(online_status=="INACTIVE" & status=="Close" & end_date >="2016-04-20" & end_date <="2017-06-10") %>%
   dplyr::summarise(average_duration = mean(duration))

turnAround_P2 <- Punggol_Leak %>%
  dplyr::filter(status=="Close" & end_date >="2016-04-20" & end_date <="2017-06-10") %>%
  dplyr::summarise(average_duration = mean(duration))

turnAround_P3_Online <- Punggol_Leak %>%
  dplyr::filter(online_status=="ACTIVE" & status=="Close" & end_date >="2017-06-10") %>%
  dplyr::summarise(average_duration = mean(duration))

turnAround_P3_Offline <- Punggol_Leak %>%
  dplyr::filter(online_status=="INACTIVE" & status=="Close" & end_date >="2017-06-10") %>%
  dplyr::summarise(average_duration = mean(duration))

turnAround_P3 <- Punggol_Leak %>%
  dplyr::filter(status=="Close" & end_date >="2017-06-10") %>%
  dplyr::summarise(average_duration = mean(duration))

# Leak Volume Per Day
load("/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay_OnlineOffline.RData")
LeakVolume_Online <- data.frame(date=index(LeakVolumePerDay_Online_xts), coredata(LeakVolumePerDay_Online_xts))
LeakVolume_Offline <- data.frame(date=index(LeakVolumePerDay_Offline_xts), coredata(LeakVolumePerDay_Offline_xts))
LeakVolume_Total <- data.frame(date=index(LeakVolumePerDay_xts), coredata(LeakVolumePerDay_xts))
                                 
LeakVolume_Online_P2 <- LeakVolume_Online %>% dplyr::filter(date >="2016-04-20" & date <="2017-06-10") %>%
                        dplyr::summarise(average_LeakVolume = mean(coredata.LeakVolumePerDay_Online_xts.))

LeakVolume_Offline_P2 <- LeakVolume_Offline %>% dplyr::filter(date >="2016-04-20" & date <="2017-06-10") %>%
                         dplyr::summarise(average_LeakVolume = mean(coredata.LeakVolumePerDay_Offline_xts.))
LeakVolume_Total_P2 <- LeakVolume_Total %>% dplyr::filter(date >="2016-04-20" & date <="2017-06-10") %>%
                       dplyr::summarise(average_LeakVolume = mean(coredata.LeakVolumePerDay_xts.))

LeakVolume_Online_P3 <- LeakVolume_Online %>% dplyr::filter(date >="2017-06-10") %>%
  dplyr::summarise(average_LeakVolume = mean(coredata.LeakVolumePerDay_Online_xts.))

LeakVolume_Offline_P3 <- LeakVolume_Offline %>% dplyr::filter(date >="2017-06-10") %>%
  dplyr::summarise(average_LeakVolume = mean(coredata.LeakVolumePerDay_Offline_xts.))
LeakVolume_Total_P3 <- LeakVolume_Total %>% dplyr::filter(date >="2017-06-10") %>%
  dplyr::summarise(average_LeakVolume = mean(coredata.LeakVolumePerDay_xts.))

