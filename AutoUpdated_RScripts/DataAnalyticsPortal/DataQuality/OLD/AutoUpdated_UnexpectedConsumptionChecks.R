# -	Service point with monthly occupancy rate > 0 AND with status = vacant 

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,dplyr)

last30days <- today()-30

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

servicepoint <- tbl(con,"service_point") %>% 
  dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B") %>%
  as.data.frame()

meter <- tbl(con,"meter") %>% as.data.frame()

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate","id"))

family_vacant <- tbl(con,"family") %>% 
                 as.data.frame() %>% 
                 dplyr::filter(status=="VACANT" & is.na(move_out_date)) %>%
                 dplyr::select_("id_service_point") 
family_vacant_servicepoint_meter <- inner_join(family_vacant,servicepoint_meter,by=c("id_service_point"="id")) %>% 
                                    dplyr::select_("service_point_sn","meter_sn","block","floor","unit","room_type")

monthly_occupancy_last30days <- tbl(con,"monthly_occupancy") %>% as.data.frame() %>% 
                                dplyr::filter(date(lastupdated)>=last30days)

unexpected_consumption <- inner_join(family_vacant_servicepoint_meter,monthly_occupancy_last30days,by="service_point_sn") %>%
  dplyr::filter(occupancy_rate >0) %>%
  dplyr::select_("service_point_sn","meter_sn","block","floor","unit","room_type") %>%
  unique()

unexpected_consumption <- inner_join(family_vacant_servicepoint_meter,monthly_occupancy_last30days,by="service_point_sn") %>%
                          dplyr::filter(occupancy_rate >0) %>%
                          dplyr::select_("service_point_sn","meter_sn","block","floor","unit","room_type","occupancy_rate","lastupdated") %>%
                          dplyr::group_by(service_point_sn) %>%
                          dplyr::filter(lastupdated==max(lastupdated))

save(unexpected_consumption,file="/srv/shiny-server/DataAnalyticsPortal/data/UnexpectedConsumptionChecks.RData")

time_taken <- proc.time() - ptm
ans <- paste("UnexpectedConsumptionChecks successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)