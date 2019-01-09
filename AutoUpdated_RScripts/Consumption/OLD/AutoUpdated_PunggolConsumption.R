rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

# Establish connection
con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")

consumption_Start <- as.data.table(tbl(con,"consumption") %>% filter(date(date_consumption) < "2016-11-01"))
save(consumption_Start,file="/srv/shiny-server/DataAnalyticsPortal/data/consumption_Start.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/consumption_Start.RData")
consumption_End <- as.data.table(tbl(con,"consumption") %>% filter(date(date_consumption) >= "2016-11-01"))
consumption_list <- list(consumption_Start,consumption_End)
consumption <- rbindlist(consumption_list)

load("/srv/shiny-server/DataAnalyticsPortal/data/consumption_Start.RData")
max.time <- max(consumption_Start$date_consumption)
consumption_End <- as.data.table(tbl(con,"consumption") %>% filter(date(date_consumption) > max.time))

l <- list(consumption_Start,consumption_End)
consumption <- rbindlist(l)

load("/srv/shiny-server/DataAnalyticsPortal/data/index_Start.RData")
index_End <- as.data.table(tbl(con,"index") %>% filter(date(current_index_date) >= "2016-11-01"))
index_list <- list(index_Start,index_End)
index <- rbindlist(index_list) %>% dplyr::select_("id_service_point","current_index_date","index")

servicepoint <- as.data.frame(tbl(con,"service_point"))
consumption_servicepoint <- inner_join(consumption,servicepoint,by=c("id_service_point"="id"))

PunggolConsumption_MAIN <- consumption_servicepoint %>%
                           dplyr::filter(site == "Punggol" & (meter_type=="MAIN" |meter_type=="BYPASS")) %>%
                           dplyr::mutate(day=day(date(date_consumption)),month=month(date_consumption)) %>%                  
                           select(service_point_sn,meter_type,block,interpolated_consumption,date_consumption,day,month) %>%
                           arrange(date_consumption)

PunggolConsumption_MAINSUB <- consumption_servicepoint %>%
                              dplyr::filter(site %in% c("Punggol","Whampoa")) %>%
                              dplyr::mutate(day=day(date(date_consumption)),month=month(date_consumption)) %>%                  
                              select(service_point_sn,adjusted_consumption,date_consumption,day,month,
                                     block,floor,room_type,meter_type) %>%
                              arrange(date_consumption)

PunggolConsumption_SUB <- consumption_servicepoint %>%
                      dplyr::filter(site %in% c("Punggol","Whampoa") & !(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
                      dplyr::mutate(day=day(date(date_consumption)),month=month(date_consumption)) %>%                  
                      select(service_point_sn,block,room_type,floor,adjusted_consumption,date_consumption,day,month) %>%
                      arrange(date_consumption)

save(PunggolConsumption_MAIN,PunggolConsumption_MAINSUB,PunggolConsumption_SUB,
     file="/srv/shiny-server/DataAnalyticsPortal/data/PunggolConsumption.RData")

save(consumption,file="/srv/shiny-server/DataAnalyticsPortal/data/consumption.RData")
save(index,file="/srv/shiny-server/DataAnalyticsPortal/data/index.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_PunggolConsumption successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)