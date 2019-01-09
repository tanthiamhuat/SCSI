rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,stringr,ISOweek,fst)

date_past3months <- today()-90
begin_date <- date_past3months - mday(date_past3months) + 1

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

consumption <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_last6months.fst")
consumption$date_consumption <- as.POSIXct(consumption$date_consumption, origin="1970-01-01")
consumption$adjusted_date <- as.POSIXct(consumption$adjusted_date, origin="1970-01-01")

consumption_download <- consumption %>% filter(date(date_consumption)>=begin_date) 

index <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/DB_Index_last6months.fst")
index$current_index_date <- as.POSIXct(index$current_index_date, origin="1970-01-01")

index_download <- index %>% filter(date(current_index_date)>=begin_date) 

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
family_servicepoint_meter <- inner_join(family_servicepoint,meter,by=c("service_point_sn"="id_real_estate","meter_type"))

consumption_download_family_servicepoint_meter <- inner_join(consumption_download,family_servicepoint_meter,by="id_service_point")
index_download_family_servicepoint_meter <- inner_join(index_download,family_servicepoint_meter,by="id_service_point")

ConsumptionDownload <- consumption_download_family_servicepoint_meter   %>%
                       dplyr::mutate(date=date(date_consumption),time=substr(date_consumption,12,19)) %>%
                       dplyr::filter(!(site=="Whampoa") & !(is.na(interpolated_consumption))) %>%
                       dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type",
                                      "date","time","adjusted_consumption","index_value")

IndexDownload <- index_download_family_servicepoint_meter  %>%
                 dplyr::mutate(date=date(current_index_date),time=substr(current_index_date,12,19)) %>%
                 dplyr::filter(!(site=="Whampoa")) %>%
                 dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type",
                                "date","time","index")

colnames(ConsumptionDownload)[ncol(ConsumptionDownload)] <-"interpolated_index"

ConsumptionDownload$week <- gsub("-W","_",str_sub(date2ISOweek(ConsumptionDownload$date),end = -3)) # convert date to week
IndexDownload$week <- gsub("-W","_",str_sub(date2ISOweek(IndexDownload$date),end = -3)) # convert date to week

Updated_DateTime_ConsumptionIndexDownload <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

fstwrite(ConsumptionDownload,"/srv/shiny-server/DataAnalyticsPortal/data/ConsumptionDownload.fst",100)
fstwrite(IndexDownload,"/srv/shiny-server/DataAnalyticsPortal/data/IndexDownload.fst",100)

save(begin_date,Updated_DateTime_ConsumptionIndexDownload,file="/srv/shiny-server/DataAnalyticsPortal/data/Updated_DateTime_ConsumptionIndexDownload.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ConsumptionIndexDownload successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)