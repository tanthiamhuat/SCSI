rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table,xts)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_available <- servicepoint %>% dplyr::select_("id","site","block","service_point_sn") %>%
  filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")
servicepoint_available_Punggol <- servicepoint_available %>% filter(site=="Punggol")
servicepoint_available_Yuhua <- servicepoint_available %>% filter(site=="Yuhua")
servicepoint_available_Tuas <- servicepoint_available %>% filter(site=="Tuas")

today <- today()

## load Raw Consumption files
load("/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption.RData")

RawConsumption_servicepoint <- inner_join(RawConsumption,servicepoint,by=c("ExternalMeteringPointReference"="service_point_sn")) %>%
                               dplyr::select_("ExternalCustomerReference","MeterSerialNumber","ReadingDate","site")

Punggol_RawConsumptionCount <- RawConsumption_servicepoint %>% dplyr::filter(site=="Punggol") %>%
                               dplyr::mutate(Date=date(ReadingDate)) %>%
                               dplyr::group_by(Date) %>%
                               dplyr::summarise(Count=n(),CCFilesAvailability=round(Count/(24*nrow(servicepoint_available_Punggol))*100)) %>% 
                               dplyr::filter(Date >= "2016-03-15" & Date < today)

Tuas_RawConsumptionCount <- RawConsumption_servicepoint %>% dplyr::filter(site=="Tuas") %>%
                            dplyr::mutate(Date=date(ReadingDate)) %>%
                            dplyr::group_by(Date) %>%
                            dplyr::summarise(Count=n(),CCFilesAvailability=round(Count/(24*nrow(servicepoint_available_Tuas))*100)) %>% 
                            dplyr::filter(Date >= "2016-03-15" & Date < today)

Yuhua_RawConsumptionCount <- RawConsumption_servicepoint %>% dplyr::filter(site=="Yuhua") %>%
  dplyr::mutate(Date=date(ReadingDate)) %>%
  dplyr::group_by(Date) %>%
  dplyr::summarise(Count=n(),CCFilesAvailability=round(Count/(24*nrow(servicepoint_available_Yuhua))*100)) %>% 
  dplyr::filter(Date >= "2016-03-15" & Date < today)

