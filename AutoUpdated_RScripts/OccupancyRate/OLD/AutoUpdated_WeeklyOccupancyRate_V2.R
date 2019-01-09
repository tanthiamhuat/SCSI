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

Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last30days.fst")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(month=month(adjusted_date),day=day(adjusted_date)) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,date_consumption,month,day) %>%
  arrange(date_consumption)

WeeklyOccupancyRate <- PunggolConsumption_SUB %>% 
                       dplyr::mutate(week_number=paste(year(date(date_consumption)),"_",strftime(date(date_consumption),format="%W"),sep=""),
                                     date=date(date_consumption),
                                     wd_numeric=wday(date(date_consumption))-1,
                                     wd_numeric=ifelse(wd_numeric==0,7,wd_numeric))%>% # replace 0 with 7
                       group_by(service_point_sn,week_number,wd_numeric) %>%
                       filter(!is.na(adjusted_consumption)) %>%
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
ans <- paste("AutoUpdated_WeeklyOccupancyRate_V2 successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)