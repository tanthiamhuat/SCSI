rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

# Phase_1 – pre leak alarm and app (from 2016-03-18 to 2016-04-19)
# Phase_2 – leak alarm only (from 2016-04-20" to 2017-06-10)
# Phase_3 – leak alarm + app (> 2017-06-10)
# LPCD Approach (preferred method)
# Actual Water savings due to Leak Alarm = LPCD (Phase_1) – LPCD (Phase_2)    ---------------  (A)
# Actual Water savings due to Gamification = LPCD (Phase_2) – LPCD (Phase_3)  ---------------  (B)

# Leakage Rate Approach
# Potential water savings due to leak alarm =  { Leak rate (Phase_1) – Leak rate (Phase_2) } x no of days in Phase_2 + 
#                                              { Leak rate (Phase_2) – Leak rate (Phase_3) } x no of days in Phase_3

# We can use the total leak volume to “back-check” the robustness of the figures under the LPCD approach since 
# the actual volume of water saved under the LPCD approach should be the same as that computed under the leak rate method.

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.RData")

## multiple linear regression
## output: daily_LPCD, 
## inputs: daily mean temperature, weekends, holiday, price hike (1 July 2017 onwards), 
##         GamificationApp (10 June 2017 onwards), LeakAlarm (10 April 2016 onwards)

load("/srv/shiny-server/DataAnalyticsPortal/data/Weather.RData")
TempAvg <- Weather %>% dplyr::select_("Date","Tavg") %>% filter(Date>="2016-03-14" & Date <= "2017-11-30")
LPCD_Daily <- DailyLPCD %>% dplyr::select_("date","DailyLPCD") %>% filter(date>="2016-03-14" & date <= "2017-11-30")

library(chron)
data_updated <- cbind(LPCD_Daily,TempAvg[,2])
colnames(data_updated)[3] <- "temp"
data_updated$weekend <- as.integer(is.weekend(data_updated$date))

holiday <- read.table("/srv/shiny-server/DataAnalyticsPortal/data/Holidays_2014_2017.csv",sep=";",dec=".",header=FALSE,as.is=TRUE,quote="\"");
holiday_dates <- as.Date(paste(substr(holiday$V2,7,10),"-",substr(holiday$V2,4,5),"-",substr(holiday$V2,1,2),sep=""))

data_updated <- data_updated %>% dplyr::mutate(holiday=ifelse(date %in% holiday_dates,1,0),
                                               price.hike=ifelse(date >= "2017-07-01",1,0),
                                               app=ifelse(date >= "2017-06-10",1,0),
                                               leak.alarm=ifelse(date >= "2016-04-20",1,0))


multi.fit = lm(DailyLPCD~temp+weekend+holiday+price.hike+app+leak.alarm, data=data_updated)
summary(multi.fit)
coefficients <- summary(multi.fit)$coefficients[6:7, 1]
coefficient_app <- coefficients[1]
coefficient_leakalarm <- coefficients[2]

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_BlockWaterSavings successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)