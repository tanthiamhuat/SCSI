rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,stringr,ISOweek)

date_past3months <- today()-months(3)
begin_date <- date_past3months - mday(date_past3months) + 1

# Establish connection
con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")

load("/srv/shiny-server/DataAnalyticsPortal/data/consumption.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/index.RData")

family <- as.data.frame(tbl(con,"family")) %>% dplyr::filter(!(status=="INACTIVE"))
servicepoint <- as.data.frame(tbl(con,"service_point")) %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")
family_servicepoint_meter <- inner_join(family_servicepoint,meter,by=c("service_point_sn"="id_real_estate"))

consumption_family_servicepoint_meter <- inner_join(consumption,family_servicepoint_meter,by="id_service_point")
index_family_servicepoint_meter <- inner_join(index,family_servicepoint_meter,by="id_service_point")

ConsumptionDownload <- consumption_family_servicepoint_meter  %>%
                       dplyr::mutate(date=date(date_consumption),time=substr(date_consumption,12,19)) %>%
                       dplyr::filter(!(site=="Whampoa") & date >= begin_date) %>%
                       dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn",
                                      "date","time","adjusted_consumption","index_value")

IndexDownload <- index_family_servicepoint_meter  %>%
                 dplyr::mutate(date=date(current_index_date),time=substr(current_index_date,12,19)) %>%
                 dplyr::filter(!(site=="Whampoa") & date >= begin_date) %>%
                 dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn",
                                "date","time","index")

colnames(ConsumptionDownload)[ncol(ConsumptionDownload)] <-"interpolated_index"

ConsumptionDownload$week <- gsub("-W","_",str_sub(date2ISOweek(ConsumptionDownload$date),end = -3)) # convert date to week
IndexDownload$week <- gsub("-W","_",str_sub(date2ISOweek(IndexDownload$date),end = -3)) # convert date to week

save(ConsumptionDownload,IndexDownload,begin_date,file="/srv/shiny-server/DataAnalyticsPortal/data/ConsumptionIndexDownload.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ConsumptionRawIndex successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)