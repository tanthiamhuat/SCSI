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
servicepoint_PG <- servicepoint %>% filter(site=="Punggol" & !room_type %in% c("OTHER","NIL") & meter_type=="SUB") 

index_PG <- as.data.table(tbl(con,"index")) %>% 
  dplyr::filter(id_service_point %in% servicepoint_PG$id & date(current_index_date)>="2017-01-01")

## remove first 3 index for each servicepoint

PG_MinCon <- index_PG %>% 
             dplyr::select_("id_service_point","current_index_date","index") %>%
             dplyr::group_by(id_service_point) %>%
             dplyr::arrange(current_index_date) %>%
          #   slice(4:n()) %>%
             dplyr::mutate(NonInterCon=c(NA,diff(index,differences=1))) %>%
             dplyr::mutate(date=date(current_index_date)) %>%
             dplyr::group_by(id_service_point,date) %>%
           #  dplyr::mutate(Count=n()) %>% 
           #    dplyr::filter(Count==24) %>%  ??
             dplyr::summarise(MinCons_60=min(NonInterCon,na.rm = TRUE)) %>% 
             dplyr::filter(MinCons_60>=0 & MinCons_60 < 10000)

write.csv(PG_MinCon,file="/srv/shiny-server/DataAnalyticsPortal/data/PG_flow.csv")

flowCount <- as.data.table(tbl(con,"flow")) %>%
             dplyr::mutate(Date=date(flow_date)) %>%
             dplyr::group_by(Date) %>%
             dplyr::summarise(Count=n()) %>%
             dplyr::arrange(desc(Date))
             