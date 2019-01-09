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

## remove first 3 index for each servicepoint

NonInterCon_MinCon <- as.data.table(tbl(con,"index")) %>% 
               dplyr::select_("id_service_point","current_index_date","index") %>%
               dplyr::group_by(id_service_point) %>%
               dplyr::arrange(current_index_date) %>%
               slice(4:n()) %>%
               dplyr::mutate(NonInterCon=c(NA,diff(index,differences=1))) %>%
               dplyr::mutate(date=date(current_index_date)) %>%
               dplyr::group_by(id_service_point,date) %>%
               dplyr::mutate(Count=n()) %>% 
           #    dplyr::filter(Count==24) %>%  ??
               dplyr::summarise(MinCons_60=min(NonInterCon,na.rm = TRUE)) %>% 
               dplyr::filter(MinCons_60>=0 & MinCons_60 < 10000)

# MinCon <-  NonInterCon_Test %>% 
#            dplyr::mutate(date=date(current_index_date)) %>%
#            dplyr::group_by(id_service_point,date) %>%
#            dplyr::summarise(MinCons_60=min(NonInterCon,na.rm = TRUE)) %>% 
#            dplyr::filter(MinCons_60>0 & MinCons_60 < 10000)

flow <- as.data.frame(tbl(con,"flow")) %>% 
        dplyr::select_("id_service_point","min_5_flow","flow_date") %>%
        dplyr::mutate(date=date(flow_date))

servicepoint <- as.data.frame(tbl(con,"service_point"))
meter <- as.data.frame(tbl(con,"meter"))

servicepoint <- servicepoint %>% filter(!is.na(floor) & !is.na(unit)) # only Punggol HH units, exclude main meters, include HDBCD + HL

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate"))

flow_servicepoint_meter <- inner_join(servicepoint_meter,flow,by=c("id.x"="id_service_point"))

flow_servicepoint_meter <- subset(flow_servicepoint_meter, select = c("service_point_sn","id.x","min_5_flow","date"))

Min_5_60_flow <- inner_join(flow_servicepoint_meter,NonInterCon_MinCon,by=c("id.x"="id_service_point","date"))
write.csv(Min_5_60_flow,file="/srv/shiny-server/DataAnalyticsPortal/data/Min5_60flow.csv")

write.csv(NonInterCon_MinCon,file="/srv/shiny-server/DataAnalyticsPortal/data/Min60flow.csv")

