rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,dplyr,xlsx,data.table,lubridate,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

DB_Consumption <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_last30days.fst")
#DB_Consumption <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_last6months.fst")
DB_Consumption$date_consumption <- as.POSIXct(DB_Consumption$date_consumption, origin="1970-01-01")
DB_Consumption$adjusted_date <- as.POSIXct(DB_Consumption$adjusted_date, origin="1970-01-01")

### adjusted_date column to be added
### based on date_consumption minus one hour
DB_Consumption$adjusted_date <- DB_Consumption$date_consumption-lubridate::hours(1)

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))) %>%
  group_by(id_service_point) %>%
  dplyr::filter(move_in_date==max(move_in_date))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
meter <- as.data.frame(tbl(con,"meter"))
servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) %>% as.data.frame()

Punggol_Occupied <- family_servicepoint %>% dplyr::filter(site=="Punggol" & status=="ACTIVE")
Punggol_Vacant <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id=="EMPTY" & status=="VACANT")
## need to check whether those id_service_point with status=VACANT has also status=ACTIVE which has later move_in_date
## e.g id_service_point=213,222,492

Punggol_Not_Vacant <-  as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(id_service_point %in% Punggol_Vacant$id_service_point) %>%
  dplyr::filter(status=="ACTIVE")
Punggol_Real_Vacant <- setdiff(Punggol_Vacant$id_service_point,Punggol_Not_Vacant$id_service_point)
Punggol_Real_Vacant_ServicePoint_Sn <- servicepoint %>% dplyr::filter(id %in% Punggol_Real_Vacant) %>% dplyr::select_("service_point_sn")

DB_Consumption_ServicePoint <- inner_join(DB_Consumption,servicepoint,by=c("id_service_point"="id")) %>%
  dplyr::select_("service_point_sn","adjusted_consumption","index_value","adjusted_date") 

ZeroTotalConsumption_last30days <- DB_Consumption_ServicePoint %>%
                                   group_by(service_point_sn) %>%
                                   dplyr::summarize(total_consumption = sum(adjusted_consumption,na.rm=TRUE))%>%
                                   filter(total_consumption==0)

ZeroTotalConsumption <- ZeroTotalConsumption_last30days %>% 
                        dplyr::filter(!(service_point_sn %in% Punggol_Real_Vacant_ServicePoint_Sn$service_point_sn))

ZeroConsumption <- servicepoint_meter[match(ZeroTotalConsumption$service_point_sn,servicepoint_meter$service_point_sn),] %>%
                         dplyr::select_("service_point_sn","meter_sn","block","floor","unit","site") %>%
                         dplyr::arrange(site)

row.names(ZeroConsumption) <- NULL

DB_Consumption_last6months <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_last6months.fst")
DB_Consumption_last6months$date_consumption <- as.POSIXct(DB_Consumption_last6months$date_consumption, origin="1970-01-01")
DB_Consumption_last6months$adjusted_date <- as.POSIXct(DB_Consumption_last6months$adjusted_date, origin="1970-01-01")

DB_Consumption_last6months$adjusted_date <- DB_Consumption_last6months$date_consumption-lubridate::hours(1)

# Count number of Consecutive Days of Zero Consumption for the past 6 months
DB_Consumption_last6months_ServicePoint <- inner_join(DB_Consumption_last6months,servicepoint,by=c("id_service_point"="id")) %>%
  dplyr::select_("service_point_sn","adjusted_consumption","index_value","date_consumption") %>%
  dplyr::mutate(Date=date(date_consumption)) %>%
  dplyr::select_("service_point_sn","adjusted_consumption","Date") %>%
  dplyr::group_by(service_point_sn,Date) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE)) %>%
  dplyr::filter(DailyConsumption==0) %>%
  dplyr::mutate(consecutiveDay = c(NA,diff(Date)==1)) %>%
  dplyr::filter(!is.na(consecutiveDay)) %>%
  dplyr::mutate(ZeroConsumption_consecutiveDays = ave(consecutiveDay, cumsum(consecutiveDay == FALSE), FUN = cumsum)) %>%
  # cumulative sum that resets when FALSE is encountered
  dplyr::filter(Date==today()-1 & ZeroConsumption_consecutiveDays!=0) %>%
  dplyr::select_("service_point_sn","ZeroConsumption_consecutiveDays")

ZeroConsumptionCount <- inner_join(ZeroConsumption,DB_Consumption_last6months_ServicePoint,by="service_point_sn")

Updated_DateTime_ZeroConsumption <- paste("Last Updated on",now())

save(ZeroConsumptionCount,Updated_DateTime_ZeroConsumption,file="/srv/shiny-server/DataAnalyticsPortal/data/ZeroConsumptionCount.RData")
write.csv(ZeroConsumptionCount,"/srv/shiny-server/DataAnalyticsPortal/data/ZeroConsumptionCount.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ZeroConsumption successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)