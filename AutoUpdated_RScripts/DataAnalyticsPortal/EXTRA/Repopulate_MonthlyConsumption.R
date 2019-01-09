## Repopulate Monthly Consumption

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,tidyr,RPostgreSQL,data.table,xts,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

consumption_2016_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2016_servicepoint.fst",as.data.table = TRUE)
consumption_2017_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2017_servicepoint.fst",as.data.table = TRUE)
consumption_thisyear_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_thisyear_servicepoint.fst",as.data.table = TRUE)

consumption_All <- rbind(rbind(consumption_2016_servicepoint,consumption_2017_servicepoint),consumption_thisyear_servicepoint)

PunggolConsumption_SUB <- consumption_All[site=="Punggol" & !(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792"]
YuhuaConsumption_SUB <- consumption_All[site=="Yuhua" & !(room_type %in% c("NIL","OTHER")) & service_point_sn !="3100660792"]

PunggolMonthlyConsumption <- PunggolConsumption_SUB %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(date_consumption),Month=month(date_consumption)) %>%
  group_by(Year,Month) %>%
  dplyr::summarise(ConsumptionPerMonth=sum(adjusted_consumption,na.rm = TRUE)) 

PunggolMonthlyConsumption$Month <- factor(month.abb[PunggolMonthlyConsumption$Month],levels = month.abb)

YuhuaMonthlyConsumption <- YuhuaConsumption_SUB %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(date_consumption),Month=month(date_consumption)) %>%
  group_by(Year,Month) %>%
  dplyr::summarise(ConsumptionPerMonth=sum(adjusted_consumption,na.rm = TRUE)) %>% as.data.frame()

YuhuaMonthlyConsumption$Month <- factor(month.abb[YuhuaMonthlyConsumption$Month],levels = month.abb)
YuhuaMonthlyConsumption <- YuhuaMonthlyConsumption[4:nrow(YuhuaMonthlyConsumption),]
save(YuhuaMonthlyConsumption,file="/srv/shiny-server/DataAnalyticsPortal/data/DT/YuhuaMonthlyConsumption.RData")
