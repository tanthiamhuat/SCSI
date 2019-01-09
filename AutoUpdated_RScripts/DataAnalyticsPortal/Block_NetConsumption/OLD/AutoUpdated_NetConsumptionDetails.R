rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,stringr,ISOweek,lubridate)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/WeeklyNetConsumption.RData")

today_week <- gsub("-W","_",str_sub(date2ISOweek(today()),end = -3)) # convert date to week

WeeklyPunggolConsumptionCSV <- WeeklyPunggolNetConsumptionDetails_NA %>%
                               dplyr::filter(week!=today_week) 

WeeklyPunggolConsumptionCSV_PG_B1 <- WeeklyPunggolConsumptionCSV %>% dplyr::filter(block=="PG_B1" & week>="2017_40")
  
library(pivottabler)
qhpvt(WeeklyPunggolConsumptionCSV_PG_B1,c("block","supply","meter_type","meter_sn"),"week",  
     calculations = "sum(WeeklyConsumption)",
     formats=list("%.0f"))

# https://cran.r-project.org/web/packages/pivottabler/vignettes/v11-shiny.html