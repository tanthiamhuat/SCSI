rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,stringr,ISOweek)

date_past3months <- today()-days(90)
begin_date <- date_past3months - mday(date_past3months) + 1

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Index.RData")

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
servicepoint <- as.data.frame(tbl(con,"service_point")) %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")
family_servicepoint_meter <- inner_join(family_servicepoint,meter,by=c("service_point_sn"="id_real_estate"))

DB_Consumption_family_servicepoint_meter <- inner_join(DB_Consumption,family_servicepoint_meter,by="id_service_point")
DB_Index_family_servicepoint_meter <- inner_join(DB_Index,family_servicepoint_meter,by="id_service_point")

ConsumptionDownload <- DB_Consumption_family_servicepoint_meter  %>%
                       dplyr::mutate(date=date(date_consumption),time=substr(date_consumption,12,19)) %>%
                       dplyr::filter(!(site=="Whampoa") & date >= begin_date & !(is.na(interpolated_consumption))) %>%
                       dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn",
                                      "date","time","adjusted_consumption","index_value")

IndexDownload <- DB_Index_family_servicepoint_meter  %>%
                 dplyr::mutate(date=date(current_index_date),time=substr(current_index_date,12,19)) %>%
                 dplyr::filter(!(site=="Whampoa") & date >= begin_date) %>%
                 dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn",
                                "date","time","index")

colnames(ConsumptionDownload)[ncol(ConsumptionDownload)] <-"interpolated_index"

ConsumptionDownload$week <- gsub("-W","_",str_sub(date2ISOweek(ConsumptionDownload$date),end = -3)) # convert date to week
IndexDownload$week <- gsub("-W","_",str_sub(date2ISOweek(IndexDownload$date),end = -3)) # convert date to week

Updated_DateTime_ConsumptionIndexDownload <- paste("Last Updated on",now())

save(ConsumptionDownload,IndexDownload,begin_date,Updated_DateTime_ConsumptionIndexDownload,file="/srv/shiny-server/DataAnalyticsPortal/data/ConsumptionIndexDownload.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ConsumptionIndexDownload successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)