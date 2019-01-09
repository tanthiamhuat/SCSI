# Block Forecast : chart to be created in the DAP, similar to “Consumption per block” 
# but with both Forecasted consumption and actual consumption.

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,xts,RPostgreSQL,lubridate)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

weekly_consumption <- as.data.frame(tbl(con,"weekly_consumption"))

servicepoint <- as.data.frame(tbl(con,"service_point")) %>% filter(service_point_sn !="3101127564") # exclude ChildCare

block_forecast <- inner_join(weekly_consumption,servicepoint,by=c("service_point_sn")) %>%
                  dplyr::select_("block","week_number",
                                 "actual_consumption","forecasted_consumption")

block_forecast_result <- block_forecast %>% group_by(week_number,block) %>%
                         dplyr::summarise(TotalActualConsumption=sum(actual_consumption),
                                          TotalForecastedConsumption=sum(forecasted_consumption))
forecast_week <- block_forecast %>% group_by(week_number) %>%
                 dplyr::summarise(TotalActualConsumption=sum(actual_consumption),
                                  TotalForecastedConsumption=sum(forecasted_consumption))
Forecast_week <- inner_join(forecast_week,Week.date,by=c("week_number"="week")) %>%
                 dplyr::select_("end","TotalActualConsumption","TotalForecastedConsumption")

## extract last 16 rows (last 4 months data if nrow >16)
if (nrow(Forecast_week)>16) 
{
  Forecast_week <- Forecast_week[(nrow(Forecast_week)-16):nrow(Forecast_week),]
}
colnames(Forecast_week)[1] <- "Date"
TotalActualConsumption_xts <- xts(Forecast_week$TotalActualConsumption/1000,Forecast_week$Date)
TotalForecastedConsumption_xts <- xts(Forecast_week$TotalForecastedConsumption/1000,Forecast_week$Date)
TotalActualForecastedConsumption_xts=cbind(TotalActualConsumption_xts,TotalForecastedConsumption_xts)
colnames(TotalActualForecastedConsumption_xts)[1] <- "Actual"
colnames(TotalActualForecastedConsumption_xts)[2] <- "Forecasted"

Updated_DateTime_BlockForecast <- paste("Last Updated on ",now(),"."," Next Update on ",now()+7*24*60*60,".",sep="")

save(block_forecast_result,TotalActualForecastedConsumption_xts,Updated_DateTime_BlockForecast,
     file="/srv/shiny-server/DataAnalyticsPortal/data/block_forecast.RData")
write.csv(block_forecast_result,"/srv/shiny-server/DataAnalyticsPortal/data/block_forecast.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_Block_Forecast successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)