rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

# a) Insert Time difference between the difference in consumption and insert date in AWS
# b) Insert Time difference between the difference in consumption and insert date in GDC

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst,xts)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

## from AWS
consumption_AWS <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_last6months.fst")  

## from GDC
con_prod <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="proddb")
consumption_GDC <- as.data.table(tbl(con_prod,"consumption")) %>% filter(date(date_consumption)>="2017-06-01")

InsertTimeDifference_AWS <- consumption_AWS %>% 
                            dplyr::mutate(TimeDiff=difftime(date_consumption,insert_date,units = "hours"),Date=date(adjusted_date)) %>%
                            dplyr::group_by(Date) %>%
                            dplyr::arrange(Date) %>%
                            dplyr::summarise(AvgTimeDifference=abs(round(mean(TimeDiff),2)))
InsertTimeDifference_AWS_xts <- xts(InsertTimeDifference_AWS$AvgTimeDifference,order.by = InsertTimeDifference_AWS$Date)

InsertTimeDifference_GDC <- consumption_GDC %>% 
                            dplyr::mutate(TimeDiff=difftime(date_consumption,insert_date,units = "hours"),Date=date(date_consumption)) %>%
                            dplyr::group_by(Date) %>%
                            dplyr::arrange(Date) %>%
                            dplyr::summarise(AvgTimeDifference=abs(round(mean(TimeDiff),2)))
InsertTimeDifference_GDC_xts <- xts(InsertTimeDifference_GDC$AvgTimeDifference,order.by = InsertTimeDifference_GDC$Date)

InsertTimeDifference_PerServer_xts <- cbind(InsertTimeDifference_AWS_xts,InsertTimeDifference_GDC_xts)
colnames(InsertTimeDifference_PerServer_xts) <- c("InsertTimeDifference_AWS","InsertTimeDifference_GDC")

Updated_DateTime_InsertTimeLag <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(InsertTimeDifference_PerServer_xts,Updated_DateTime_InsertTimeLag,
     file="/srv/shiny-server/DataAnalyticsPortal/data/InsertTimeLag.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_InsertTimeLag successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)  

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)