rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

daily_consumption_DB <- as.data.frame(tbl(con,"daily_consumption"))

daily_consumption_DB_Incomplete <- daily_consumption_DB %>% 
                                   dplyr::group_by(service_point_sn) %>%
                                   dplyr::filter(date_consumption==max(date_consumption) & date_consumption!=today()-1) 
daily_consumption_DB_Incomplete["id"] <- NULL

service_point <- as.data.frame(tbl(con,"service_point"))
family <- as.data.frame(tbl(con,"family"))
family_service_point <- inner_join(family,service_point,by=c("id_service_point"="id"))

daily_consumption_DB_Incomplete_family <- inner_join(daily_consumption_DB_Incomplete,family_service_point,by="service_point_sn") %>%
                                          dplyr::select_("id","service_point_sn","date_consumption","address.x") %>%
                                          dplyr::rename("family_id"="id")

write.csv(daily_consumption_DB_Incomplete_family,file="Incomplete_dailyconsumption.csv")
