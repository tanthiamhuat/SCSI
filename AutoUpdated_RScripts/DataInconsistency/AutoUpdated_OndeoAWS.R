rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

last3days <- today()-3

load("/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption.RData")  # from FTP Server
consumption_Raw <- RawConsumption %>%
                   dplyr::mutate(date=date(ReadingDate)) %>%
                   dplyr::filter(date>=as.Date(last3days)) %>%
                   select(ExternalMeteringPointReference,Consumption,Index,MeterSerialNumber,ExternalCustomerReference,ReadingDate)
colnames(consumption_Raw) <- c("service_point_sn_Raw","consumption_Raw","index_Raw","meter_sn_Raw","pub_cust_id_Raw","ReadingDate_Raw")
consumption_Raw <- consumption_Raw %>%
                   dplyr::arrange(service_point_sn_Raw,ReadingDate_Raw)
consumption_Raw$consumption_Raw <- as.numeric(consumption_Raw$consumption_Raw)
consumption_Raw$index_Raw <- as.numeric(consumption_Raw$index_Raw)

load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")  # from DB
consumption_DB <- DB_Consumption %>% 
                  dplyr::mutate(date=date(date_consumption)) %>%
                  dplyr::filter(date>=as.Date(last3days)) %>%
                  as.data.frame()

service_point <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
consumption_DB <- inner_join(consumption_DB,service_point,by=c("id_service_point"="id")) %>%
                  dplyr::select_("id_service_point","service_point_sn","interpolated_consumption","index_value","date_consumption") 

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

family_Punggol_MAIN <- as.data.frame(tbl(con,"family") %>% 
                                       dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & address=="PUNGGOL")) 

family_Punggol_SUB <- as.data.frame(tbl(con,"family") %>% 
                                      dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & address!="TUAS")) %>%
  group_by(id_service_point) %>%
  dplyr::filter(move_in_date==max(move_in_date))

family_Tuas <- as.data.frame(tbl(con,"family") %>% 
                               dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & address=="TUAS")) 

family_Punggol <- rbind.data.frame(family_Punggol_MAIN,family_Punggol_SUB)
family <- rbind.data.frame(family_Punggol,family_Tuas)

family_servicepoint <- full_join(family,service_point,by=c("id_service_point"="id")) %>%
                       dplyr::select_("id_service_point","pub_cust_id","service_point_sn")
family_servicepoint_meter <- inner_join(family_servicepoint,meter,by=c("service_point_sn"="id_real_estate")) %>%
                             dplyr::select_("id_service_point","pub_cust_id","service_point_sn","meter_sn")

family_servicepoint_meter_consumption <- inner_join(family_servicepoint_meter,consumption_DB,by=c("id_service_point","service_point_sn")) %>%
                                         dplyr::arrange(service_point_sn,date_consumption)
family_servicepoint_meter_consumption[1] <- NULL

consumption_DB <- family_servicepoint_meter_consumption %>% as.data.frame() %>%
                  dplyr::select_("service_point_sn","interpolated_consumption","index_value","meter_sn","pub_cust_id","date_consumption")

colnames(consumption_DB) <- c("service_point_sn_DB","consumption_DB","index_DB","meter_sn_DB","pub_cust_id_DB","ReadingDate_DB")
consumption_DB$consumption_DB <- as.numeric(consumption_DB$consumption_DB)
consumption_DB$index_DB <- as.numeric(consumption_DB$index_DB)

ConsumptionRawDB <- left_join(consumption_Raw,consumption_DB,by=c("service_point_sn_Raw"="service_point_sn_DB",
                                                                  "ReadingDate_Raw"="ReadingDate_DB")) %>%
                    dplyr::rename(service_point_sn=service_point_sn_Raw,ReadingDate=ReadingDate_Raw) %>%
                    dplyr::mutate(ConsumptionEqual=(consumption_Raw==consumption_DB),
                                  IndexEqual=(index_Raw==index_DB),
                                  MeterSnEqual=(meter_sn_Raw==meter_sn_DB),
                                  PubCustIdEqual=(pub_cust_id_Raw==pub_cust_id_DB))

Consumption_Inconsistency <- ConsumptionRawDB %>%
                             dplyr::filter(ConsumptionEqual %in% c("NA","FALSE")) %>%
                             dplyr::select_("service_point_sn","consumption_Raw","consumption_DB") %>%
                             unique()

Index_Inconsistency <- ConsumptionRawDB %>%
                       dplyr::filter(IndexEqual %in% c("NA","FALSE")) %>%
                       dplyr::select_("service_point_sn","index_Raw","index_DB") %>%
                       unique()

MeterSn_Inconsistency <- ConsumptionRawDB %>%
                         dplyr::filter(MeterSnEqual %in% c("NA","FALSE")) %>%
                         dplyr::select_("service_point_sn","meter_sn_Raw","meter_sn_DB") %>%
                         unique()

PubCustId_Inconsistency <- ConsumptionRawDB %>%
                           dplyr::filter(PubCustIdEqual %in% c("NA","FALSE")) %>%
                           dplyr::select_("service_point_sn","pub_cust_id_Raw","pub_cust_id_DB") %>%
                           unique()

RawServicePoint <- consumption_Raw %>% 
                   dplyr::select_("service_point_sn_Raw") %>%
                   unique() 
ServicePoint_Inconsistency <- as.data.frame(RawServicePoint[which(!(RawServicePoint$service_point_sn_Raw %in% service_point$service_point_sn)),1])
colnames(ServicePoint_Inconsistency) <- "ServicePointSn"

Updated_DateTime_OndeoAWS <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(Consumption_Inconsistency,Index_Inconsistency,
     MeterSn_Inconsistency,PubCustId_Inconsistency,ServicePoint_Inconsistency,
     Updated_DateTime_OndeoAWS,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DataInconsistencyChecks.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_OndeoAWS successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)