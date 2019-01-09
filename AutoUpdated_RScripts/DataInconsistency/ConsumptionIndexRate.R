rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

## to check number of hourly consumption for each day
## Example if there are total 607 meters, per day count of total of hourly consumption should be 24*607=14568

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

consumption <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_thisyear.fst")  
consumption$date_consumption <- as.POSIXct(consumption$date_consumption, origin="1970-01-01")
consumption$adjusted_date <- as.POSIXct(consumption$adjusted_date, origin="1970-01-01")

consumption <- consumption %>% dplyr::mutate(Date=date(adjusted_date))

ConsumptionRate <- consumption %>% dplyr::group_by(Date) %>%
                   dplyr::summarise(Count=n(),Rate=round(Count/(608*24)*100,2)) %>% arrange(Date)

index <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/DB_Index_last6months.fst")  
index$current_index_date <- as.POSIXct(index$current_index_date, origin="1970-01-01")

index <- index %>% dplyr::mutate(Date=date(current_index_date))

IndexRate <- index %>% dplyr::group_by(Date) %>%
  dplyr::summarise(Count=n(),Rate=round(Count/(608*24)*100,2)) %>% arrange(Date)