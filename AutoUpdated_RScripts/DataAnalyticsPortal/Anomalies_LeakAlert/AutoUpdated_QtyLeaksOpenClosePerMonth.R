## a chart to show the QTY of leaks opened and closed every month.
## using WaterFall or Stock chart.

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

LeakAlarmOpen <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/leak_alarm_PunggolYuhua.csv") %>%
                 dplyr::mutate(Year=year(start_date),Month=month(start_date)) %>%
                 dplyr::select_("family_id","start_date","end_date","Year","Month","status") %>%
                 dplyr::group_by(Year,Month) %>%
                 dplyr::summarise(OpenCount=n()) %>% as.data.frame()

LeakAlarmClose <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/leak_alarm_PunggolYuhua.csv") %>%
                  dplyr::mutate(Year=year(end_date),Month=month(end_date)) %>%
                  dplyr::select_("family_id","start_date","end_date","Year","Month","status") %>%
                  dplyr::filter(status=="Close") %>%
                  dplyr::group_by(Year,Month) %>%
                  dplyr::summarise(CloseCount=n()) %>% as.data.frame()

LeakAlarmOpenCloseCount <- dplyr::left_join(LeakAlarmOpen,LeakAlarmClose,by=c("Year","Month"))

#LeakAlarmOpenCloseCount <- merge(LeakAlarmOpen, LeakAlarmClose, by=c("Year","Month"), all = TRUE)

LeakAlarmOpenCloseCount[is.na(LeakAlarmOpenCloseCount)] <- 0

LeakAlarmOpenCloseCount$YearMonth <- paste(LeakAlarmOpenCloseCount$Year,"-",LeakAlarmOpenCloseCount$Month,sep="")
library(zoo)
LeakAlarmOpenCloseCount$YearMonth <- as.yearmon(LeakAlarmOpenCloseCount$YearMonth)
LeakAlarmOpenCloseCount$MonthYear <- paste(substr(LeakAlarmOpenCloseCount$YearMonth,1,3),"'",substr(LeakAlarmOpenCloseCount$YearMonth,7,8),sep="")

LeakAlarmOpenCloseCount$OpenCount <- as.numeric(LeakAlarmOpenCloseCount$OpenCount)
LeakAlarmOpenCloseCount$CloseCount <- as.numeric(LeakAlarmOpenCloseCount$CloseCount)
LeakAlarmOpenCloseCount$CloseCount <- -LeakAlarmOpenCloseCount$CloseCount 

LeakAlarmOpenCloseCount <- LeakAlarmOpenCloseCount %>% dplyr::select_("MonthYear","OpenCount","CloseCount")

LeakAlarmOpenCloseCount <- as.data.frame(LeakAlarmOpenCloseCount)
LeakAlarmOpenCloseCount$Order <- seq(1:nrow(LeakAlarmOpenCloseCount))
require(reshape2)
LeakAlarmOpenCloseCount_long <- melt(LeakAlarmOpenCloseCount, id = c("MonthYear","Order"))
LeakAlarmOpenCloseCount_long <- LeakAlarmOpenCloseCount_long[order(LeakAlarmOpenCloseCount_long$Order),]
LeakAlarmOpenCloseCount_long$MonthYear <- as.character(LeakAlarmOpenCloseCount_long$MonthYear)
LeakAlarmOpenCloseCount_long["Order"] <- NULL

LeakAlarmWaterFall <- LeakAlarmOpenCloseCount_long %>% dplyr::select_("MonthYear","value")
LeakAlarmWaterFall$leak <- rep(c("O","C"),nrow(LeakAlarmWaterFall)/2)

write.csv(LeakAlarmWaterFall,file="/srv/shiny-server/DataAnalyticsPortal/data/LeakAlarmWaterFall.csv")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_QtyLeaksOpenClosePerMonth successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)
