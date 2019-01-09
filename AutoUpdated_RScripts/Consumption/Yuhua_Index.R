rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,dplyr,data.table,lubridate,stringr,ISOweek,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_YH <- servicepoint %>% filter(site=="Yuhua" & !room_type %in% c("OTHER","NIL") & meter_type=="SUB") 

index_YH <- as.data.table(tbl(con,"index")) %>% 
  dplyr::filter(id_service_point %in% servicepoint_YH$id & date(current_index_date)>="2018-04-01")

YH_MinCon <- index_YH %>% 
             dplyr::select_("id_service_point","current_index_date","index") %>%
             dplyr::group_by(id_service_point) %>%
             dplyr::arrange(current_index_date) %>%
             dplyr::mutate(NonInterCon=c(NA,diff(index,differences=1))) %>%
             dplyr::mutate(date=date(current_index_date)) %>%
             dplyr::group_by(id_service_point,date) %>%
             dplyr::summarise(MinCons_60=min(NonInterCon,na.rm = TRUE)) %>% 
             dplyr::filter(MinCons_60>=0 & MinCons_60 < 10000)

write.csv(YH_MinCon,file="/srv/shiny-server/DataAnalyticsPortal/data/YH_flow.csv")

## above is base on the Index, below is base on Consumption.
consumption_last90days_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last90days_servicepoint.fst",as.data.table = TRUE)
Yuhua_SUB <- consumption_last90days_servicepoint[site=="Yuhua" & meter_type=="SUB" & adjusted_consumption != "NA"] 

Yuhua_SUB_MinCon <- Yuhua_SUB %>% 
                    dplyr::mutate(Date=date(date_consumption)) %>%
                    dplyr::group_by(service_point_sn,Date) %>%
                    dplyr::summarise(MinCons=min(adjusted_consumption,na.rm = TRUE))  
