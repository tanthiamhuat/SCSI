# -	Service point with monthly occupancy rate > 0 AND with status = vacant 

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,dplyr,lubridate)

last5days <- today()-5

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

servicepoint <- tbl(con,"service_point") %>% 
  dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B") %>%
  as.data.frame()

meter <- tbl(con,"meter") %>% as.data.frame()

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate","id"))

Punggol_Vacant <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id=="EMPTY" & status=="VACANT")
## need to check whether those id_service_point with status=VACANT has also status=ACTIVE which has later move_in_date
## e.g id_service_point=213,222,492

Punggol_Not_Vacant <-  as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(id_service_point %in% Punggol_Vacant$id_service_point) %>%
  dplyr::filter(status=="ACTIVE")
Punggol_Real_Vacant <- setdiff(Punggol_Vacant$id_service_point,Punggol_Not_Vacant$id_service_point)
Punggol_Real_Vacant_ServicePoint_Sn <- servicepoint %>% dplyr::filter(id %in% Punggol_Real_Vacant) %>% dplyr::select_("service_point_sn")

Punggol_RealVacant_Sn <- servicepoint_meter %>% dplyr::filter(service_point_sn %in% Punggol_Real_Vacant_ServicePoint_Sn$service_point_sn) %>% dplyr::select_("service_point_sn","meter_sn","block","floor","unit","room_type")

## to be modified. based on daily_occupancy, not monthly occupancy
daily_occupancy_last5days <- tbl(con,"daily_occupancy") %>% as.data.frame() %>% 
                              dplyr::filter(date(date_consumption)>=last5days)

unexpected_consumption <- inner_join(Punggol_RealVacant_Sn,daily_occupancy_last5days,by="service_point_sn") %>%
  dplyr::filter(occupancy_rate >0) %>%
  dplyr::select_("service_point_sn","meter_sn","block","floor","unit","room_type","occupancy_rate","date_consumption") %>%
  dplyr::group_by(service_point_sn) %>%
  dplyr::filter(date_consumption==max(date_consumption))

Updated_DateTime_UnexpectedConsumption <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")
 
save(unexpected_consumption,Updated_DateTime_UnexpectedConsumption,file="/srv/shiny-server/DataAnalyticsPortal/data/UnexpectedConsumption.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_UnexpectedConsumption successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)