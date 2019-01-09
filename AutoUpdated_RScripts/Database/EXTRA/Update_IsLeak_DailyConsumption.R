rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

daily_consumption <- as.data.frame(tbl(con,"daily_consumption"))
daily_consumption$is_leak <- FALSE
leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))

leak_alarm$start_date <- as.Date(leak_alarm$start_date) 
leak_alarm$end_date <- as.Date(leak_alarm$end_date)  

leak_alarm <- leak_alarm %>% filter(service_point_sn %in% daily_consumption$service_point_sn)

daily_consumption_leak_alarm <- left_join(daily_consumption,leak_alarm,by=c("service_point_sn")) %>%
  dplyr::filter(date_consumption==start_date+3) %>% # is_leak =TRUE if date_consumption=start_date+3
  dplyr::select_("service_point_sn","nett_consumption","overconsumption","date_consumption","is_leak") %>%
  dplyr::mutate(is_leak='TRUE')

daily_consumption_leak_alarm$service_point_sn <- paste0("'",daily_consumption_leak_alarm$service_point_sn,"'")
daily_consumption_leak_alarm$nett_consumption <- paste0("'",daily_consumption_leak_alarm$nett_consumption,"'")
daily_consumption_leak_alarm$overconsumption <- paste0("'",daily_consumption_leak_alarm$overconsumption,"'")

sql_update<- paste("UPDATE daily_consumption SET is_leak = ",daily_consumption_leak_alarm$is_leak," 
                    WHERE service_point_sn = ",daily_consumption_leak_alarm$service_point_sn," AND 
                    nett_consumption = ",daily_consumption_leak_alarm$nett_consumption," AND 
                    overconsumption = ",daily_consumption_leak_alarm$overconsumption,";")
sapply(sql_update, function(x){dbSendQuery(mydb, x)})