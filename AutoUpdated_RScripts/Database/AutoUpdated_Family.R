# -	In the field high_flow : a value TRUE or FALSE based on the max flow 5’ 
#  (TRUE IF > 750l/h? based on the average over the last 30 days). 
#   To be updated every week of possible. (It is not based on the challenges.)
# -	In the field high_shower usage : a value = TRUE if max flow 5’ > 600l/h AND max flow 15’>320 l/h 
#   in average over the last 30 days, otherwise FALSE.

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,stringr,ISOweek)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

last30days <- today()-30

# sql_update <- paste0("UPDATE family SET high_flow = NULL, high_shower_usage = NULL")
# 
# sapply(sql_update, function(x){dbSendQuery(mydb, x)})

family <- tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))) %>% 
          as.data.frame()
servicepoint <- tbl(con,"service_point") %>% 
                dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B") %>%
                as.data.frame()
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id")) %>% as.data.frame()

flow <- tbl(con,"flow") %>% as.data.frame()
family_servicepoint_flow <- inner_join(family_servicepoint,flow,by="id_service_point") %>%
                            dplyr::filter(date(flow_date)>last30days) %>%
                            group_by(id.x,id_service_point) %>%
                            dplyr::summarise(AverageMax5Flow=mean(max_5_flow),
                                             AverageMax15Flow=mean(max_15_flow)) %>%
                            dplyr::mutate(high_flow=AverageMax5Flow>750,
                                          high_usage=(AverageMax5Flow>600 & AverageMax15Flow >320)) %>%
                            as.data.frame()

sql_update <- paste0("UPDATE family SET high_flow = '",family_servicepoint_flow$high_flow,"', high_shower_usage = '",family_servicepoint_flow$high_usage,"' WHERE id = '",family_servicepoint_flow$id.x ,"' AND id_service_point = '",family_servicepoint_flow$id_service_point,"'")

sapply(sql_update, function(x){dbSendQuery(mydb, x)})
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_Family successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)