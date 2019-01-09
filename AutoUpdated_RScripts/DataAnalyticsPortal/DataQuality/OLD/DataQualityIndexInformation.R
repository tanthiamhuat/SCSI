rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table,xts)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

today <- today()

## load Raw Index files
load("/srv/shiny-server/DataAnalyticsPortal/data/RawIndex.RData")

## Count Percentage of Index Available Per Day
load("/srv/shiny-server/DataAnalyticsPortal/data/RawIndex.RData")
RawIndexCount <- RawIndex %>% dplyr::mutate(Date=date(ReadingDate)) %>%
  dplyr::group_by(Date) %>%
  dplyr::summarise(Count=n(),IndexAvailability=round(Count/(24*(nrow(servicepoint_available_Punggol)+nrow(servicepoint_available_Tuas)))*100)) %>% 
  dplyr::filter(Date >= "2016-03-15" & Date < today)

