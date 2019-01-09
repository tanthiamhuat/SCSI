rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

#date_extract <- as.Date("2017-03-15")  ## different rows btw Raw and DB

load("/srv/shiny-server/DataAnalyticsPortal/data/ConsumptionRaw.RData")  # from FTP Server
consumption_Raw <- RawConsumption %>%
                   dplyr::mutate(date=date(ReadingDate)) %>%
                   #dplyr::filter(date==date_extract) %>%
                   select(ExternalMeteringPointReference,ExternalCustomerReference,MeterSerialNumber,Consumption,Index,ReadingDate)
colnames(consumption_Raw) <- c("service_point_sn_Raw","pub_cust_id_Raw","meter_sn_Raw","consumption_Raw","index_Raw","ReadingDate_Raw")
consumption_Raw <- consumption_Raw %>%
                   dplyr::arrange(service_point_sn_Raw,ReadingDate_Raw)
consumption_Raw$consumption_Raw <- as.numeric(consumption_Raw$consumption_Raw)
consumption_Raw$index_Raw <- as.numeric(consumption_Raw$index_Raw)

load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")  # from DB
consumption_DB <- DB_Consumption %>% 
                  dplyr::mutate(date=date(date_consumption)) %>%
                  #dplyr::filter(date==date_extract) %>%
                  as.data.frame()

service_point <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
consumption_DB <- inner_join(consumption_DB,service_point,by=c("id_service_point"="id")) %>%
                  dplyr::select_("id_service_point","service_point_sn","interpolated_consumption","index_value","date_consumption") 

family <- as.data.frame(tbl(con,"family"))
meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")
family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE")) %>%
          group_by(id_service_point) %>%
          dplyr::filter(move_in_date==max(move_in_date))
family_servicepoint <- full_join(family,service_point,by=c("id_service_point"="id")) %>%
                       dplyr::select_("id_service_point","pub_cust_id","service_point_sn")
family_servicepoint_meter <- inner_join(family_servicepoint,meter,by=c("service_point_sn"="id_real_estate")) %>%
                             dplyr::select_("id_service_point","pub_cust_id","service_point_sn","meter_sn")

family_servicepoint_meter_consumption <- inner_join(family_servicepoint_meter,consumption_DB,by=c("id_service_point","service_point_sn")) %>%
                                         dplyr::arrange(service_point_sn,date_consumption)
family_servicepoint_meter_consumption[1] <- NULL

consumption_DB <- family_servicepoint_meter_consumption %>% as.data.frame() %>%
                  dplyr::select_("service_point_sn","pub_cust_id","meter_sn","interpolated_consumption","index_value","date_consumption")

colnames(consumption_DB) <- c("service_point_sn_DB","pub_cust_id_DB","meter_sn_DB","consumption_DB","index_DB","ReadingDate_DB")

ConsumptionRawDB <- left_join(consumption_Raw,consumption_DB,by=c("service_point_sn_Raw"="service_point_sn_DB",
                                                                  "ReadingDate_Raw"="ReadingDate_DB")) %>%
                    dplyr::rename(service_point_sn=service_point_sn_Raw,ReadingDate=ReadingDate_Raw) %>%
                    dplyr::mutate(ConsumptionNotEqual=ifelse(consumption_Raw!=consumption_DB,TRUE,FALSE),
                                  IndexNotEqual=ifelse(index_Raw!=index_DB,TRUE,FALSE),
                                  MeterSnNotEqual=ifelse(meter_sn_Raw!=meter_sn_DB,TRUE,FALSE),
                                  PubCustIdNotEqual=ifelse(pub_cust_id_Raw!=pub_cust_id_DB,TRUE,FALSE))

Consumption_Extracted <- ConsumptionRawDB %>%
                         dplyr::filter(ConsumptionNotEqual=="TRUE" | is.na(ConsumptionNotEqual)) %>%
                         dplyr::select_("service_point_sn","ReadingDate","consumption_Raw","consumption_DB")

Index_Extracted <- ConsumptionRawDB %>%
                   dplyr::filter(IndexNotEqual=="TRUE" | is.na(IndexNotEqual)) %>%
                   dplyr::select_("service_point_sn","ReadingDate","index_Raw","index_DB") 

MeterSn_Extracted <- ConsumptionRawDB %>%
                     dplyr::filter(MeterSnNotEqual=="TRUE" | is.na(MeterSnNotEqual)) %>%
                     dplyr::select_("service_point_sn","ReadingDate","meter_sn_Raw","meter_sn_DB") 

PubCustId_Extracted <- ConsumptionRawDB %>%
                       dplyr::filter(PubCustIdNotEqual=="TRUE" | is.na(PubCustIdNotEqual)) %>%
                       dplyr::select_("service_point_sn","ReadingDate","pub_cust_id_Raw","pub_cust_id_DB") 

DataInconsistencySummary = data.table(ItemDescription=c("ConsumptionNotEqual","IndexNotEqual",
                                                     "MeterSnNotEqual","PubCustIdNotEqual"),
                                   TotalServicePointSnAffected=c(length(unique(Consumption_Extracted$service_point_sn)),
                                                                 length(unique(Index_Extracted$service_point_sn)),
                                                                 length(unique(MeterSn_Extracted$service_point_sn)),
                                                                 length(unique(PubCustId_Extracted$service_point_sn))))

Consumption_Inconsistency <- as.data.frame(unique(Consumption_Extracted$service_point_sn))
colnames(Consumption_Inconsistency) <- "ServicePointSn"
Index_Inconsistency <- as.data.frame(unique(Index_Extracted$service_point_sn))
colnames(Index_Inconsistency) <- "ServiePointSn"
MeterSn_Inconsistency <- as.data.frame(unique(MeterSn_Extracted$service_point_sn))
colnames(MeterSn_Inconsistency) <- "ServicePointSn"
PubCustId_Inconsistency <- as.data.frame(unique(PubCustId_Extracted$service_point_sn))
colnames(PubCustId_Inconsistency) <- "ServicePointSn"

save(Consumption_Inconsistency,Index_Inconsistency,
     MeterSn_Inconsistency,PubCustId_Inconsistency,
     DataInconsistencySummary,file="/srv/shiny-server/DataAnalyticsPortal/data/DataInconsistencyChecks.RData")

time_taken <- proc.time() - ptm
ans <- paste("DataInconsistency successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
