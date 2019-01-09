rm(list=ls(all=TRUE));invisible(gc());
cat('\014')

load("/srv/shiny-server/DataAnalyticsPortal/Profiling_V2/Output/01-Punggol_Indicators.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

Customers <- indicator %>% dplyr::select_("ID","block","num_house_member","adc") 

Customers_Details <- inner_join(Customers,family_details,by=c("ID"="service_point_sn","num_house_member")) %>%
  dplyr::select_("ID","block","num_house_member","online_status","adc") 
colnames(Customers_Details)[1] <- "service_point_sn"

OnlineCustomers <- Customers_Details %>% dplyr::filter(online_status=="ACTIVE")
OfflineCustomers <- Customers_Details %>% dplyr::filter(online_status=="INACTIVE")
  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/Database/Customers_LPCD_Monthly.R')

## AverageLPCD, Online
CustomerOnline_AverageLPCD_Before <- Customers_LPCD_Before %>% dplyr::filter(service_point_sn %in% OnlineCustomers$service_point_sn) %>%
  dplyr::summarize(AverageLPCD=round(mean(my_average_lpcd)))

CustomerOnline_AverageLPCD_After <- Customers_LPCD_After %>% dplyr::filter(service_point_sn %in% OnlineCustomers$service_point_sn) %>%
  dplyr::summarize(AverageLPCD=round(mean(my_average_lpcd)))

## AverageLPCD, Offline
CustomerOffline_AverageLPCD_Before <- Customers_LPCD_Before %>% dplyr::filter(service_point_sn %in% OfflineCustomers$service_point_sn) %>%
  dplyr::summarize(AverageLPCD=round(mean(my_average_lpcd)))

CustomerOffline_AverageLPCD_After <- Customers_LPCD_After %>% dplyr::filter(service_point_sn %in% OfflineCustomers$service_point_sn) %>%
  dplyr::summarize(AverageLPCD=round(mean(my_average_lpcd)))

## AverageLPCD, Online, March2016
CustomerOnline_AverageLPCD_March2016 <- Customers_March2016 %>% dplyr::filter(service_point_sn %in% OnlineCustomers$service_point_sn) %>%
  dplyr::summarize(AverageLPCD=round(mean(my_average_lpcd)))

## AverageLPCD, Offline, March2016
CustomerOffline_AverageLPCD_March2016 <- Customers_March2016 %>% dplyr::filter(service_point_sn %in% OfflineCustomers$service_point_sn) %>%
  dplyr::summarize(AverageLPCD=round(mean(my_average_lpcd)))

## AverageLPCD, both Offline and Online, March2016
CustomerOnlineOffline_AverageLPCD_March2016 <- Customers_March2016 %>% 
  dplyr::summarize(AverageLPCD=round(mean(my_average_lpcd)))

## AverageLPCD, both Offline and Online, Before
CustomerOnlineOffline_AverageLPCD_Before <- Customers_LPCD_Before %>% 
  dplyr::summarize(AverageLPCD=round(mean(my_average_lpcd)))

## AverageLPCD, both Offline and Online, After
CustomerOnlineOffline_AverageLPCD_After <- Customers_LPCD_After%>% 
  dplyr::summarize(AverageLPCD=round(mean(my_average_lpcd)))
