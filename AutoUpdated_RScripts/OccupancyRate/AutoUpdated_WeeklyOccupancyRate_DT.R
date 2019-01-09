rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,fst,xts)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

weekly_occupancy_DB <- as.data.frame(tbl(con,"weekly_occupancy"))

consumption_last30days_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last30days_servicepoint.fst",as.data.table=TRUE)

PunggolYuhua_SUB <- consumption_last30days_servicepoint[!(room_type %in% c("NIL","HDBCD","OTHER","Nil")) & !(is.na(room_type)) & 
                                                        meter_type =="SUB" & site %in% c("Punggol","Yuhua","Whampoa") &
                                                        !is.na(adjusted_consumption)]

PunggolYuhua_SUB <- PunggolYuhua_SUB[,c("week_number","wd_numeric"):= 
                                     list(paste(year(date(date_consumption)),"_",strftime(date(date_consumption),format="%W"),sep=""),
                                          wday(date(date_consumption))-1)]

# replace wd_numeric 0 with 7
PunggolYuhua_SUB$wd_numeric <- gsub(0,7,PunggolYuhua_SUB$wd_numeric)

WeeklyOccupancyRate <- PunggolYuhua_SUB %>% 
                       group_by(service_point_sn,week_number,wd_numeric) %>%
                       dplyr::summarise(daily_consumption=sum(adjusted_consumption)) %>% 
                       dplyr::mutate(
                                zero_daily_consumption = ifelse(daily_consumption==0,1,0),
                                Accumulated_zero_daily_consumption = cumsum(zero_daily_consumption),
                                occupancy_rate = round((1-Accumulated_zero_daily_consumption/7)*100,1)) %>%
                       filter(wd_numeric==7) %>%
                       select(service_point_sn,occupancy_rate,week_number)

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

WeeklyOccupancyRate <- inner_join(WeeklyOccupancyRate,Week.date,by=c("week_number"="week")) %>% 
                            dplyr::mutate(lastupdated=beg+7) %>%
                            dplyr::select_("service_point_sn","week_number","occupancy_rate","lastupdated")

WeeklyOccupancyRate <- data.frame(WeeklyOccupancyRate)  ## must be data.frame to be able to write to database

lastweek <- Week.date[nrow(Week.date)-1,1]

WeeklyOccupancyRate_lastweek <- WeeklyOccupancyRate %>% dplyr::filter(week_number==lastweek)

total_rows <- nrow(WeeklyOccupancyRate_lastweek)
if (total_rows>0){
  WeeklyOccupancyRate_lastweek$id <- as.integer(seq(max(weekly_occupancy_DB$id)+1,
                                                    max(weekly_occupancy_DB$id)+total_rows,1))
  
  WeeklyOccupancyRate_lastweek <- WeeklyOccupancyRate_lastweek[,c(ncol(WeeklyOccupancyRate_lastweek),
                                                                  c(1:ncol(WeeklyOccupancyRate_lastweek)-1))]
  
  ## daily appended table
  dbWriteTable(mydb, "weekly_occupancy", WeeklyOccupancyRate_lastweek, append=TRUE, row.names=F, overwrite=FALSE) # append table
}

weekly_occupancy_DB <- as.data.frame(tbl(con,"weekly_occupancy"))

weekly_occupancy_xts <- weekly_occupancy_DB %>% 
                        dplyr::group_by(week_number,lastupdated) %>%
                        dplyr::summarise(WeeklyOccupancy=round(mean(occupancy_rate),2))
weekly_occupancy_xts$lastupdated <- weekly_occupancy_xts$lastupdated-1
Weekly_Occupancy_xts <- xts(weekly_occupancy_xts$WeeklyOccupancy,weekly_occupancy_xts$lastupdated)
colnames(Weekly_Occupancy_xts) <- "WeeklyOccupancy"

save(Weekly_Occupancy_xts,file="/srv/shiny-server/DataAnalyticsPortal/data/Weekly_Occupancy_xts.RData")

dbDisconnect(mydb)
time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_WeeklyOccupancyRate_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)