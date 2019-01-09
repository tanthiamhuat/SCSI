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

daily_occupancy_DB <- as.data.frame(tbl(con,"daily_occupancy"))

consumption_last30days_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last30days_servicepoint.fst",as.data.table=TRUE)

PunggolYuhua_SUB <- consumption_last30days_servicepoint[!(room_type %in% c("NIL","HDBCD","OTHER","Nil")) & !(is.na(room_type)) & 
                                                          meter_type =="SUB" & site %in% c("Punggol","Yuhua","Whampoa") &
                                                          !is.na(adjusted_consumption)]

PunggolYuhua_SUB <- PunggolYuhua_SUB[,c("Date"):= list(date(date_consumption))]

DailyOccupancyRate <- PunggolYuhua_SUB %>% 
                      group_by(service_point_sn,Date) %>%
                      dplyr::mutate(
                            zero_hourly_consumption = ifelse(adjusted_consumption==0, 1, 0),
                            Accumulated_zero_hourly_consumption = cumsum(zero_hourly_consumption),
                            TotalHours = n(),
                            occupancy_rate = round((1-Accumulated_zero_hourly_consumption/TotalHours)*100,1)
                            ) %>%
                      filter(date_consumption==max(date_consumption)) %>%
                      select(service_point_sn,date_consumption,occupancy_rate) %>%
                      arrange(date_consumption)

DailyOccupancyRate$Date <- NULL

DailyOccupancyRate <- DailyOccupancyRate[,c("service_point_sn","occupancy_rate","date_consumption")]

DailyOccupancyRate <- data.frame(DailyOccupancyRate)  ## must be data.frame to be able to write to database

DailyOccupancyRate <- DailyOccupancyRate[!duplicated(DailyOccupancyRate),] # remove duplicates 

DailyOccupancyRate_yesterday <- DailyOccupancyRate %>% dplyr::filter(date(date_consumption)==today()-1)
## need to append to daily_occupancy table

total_rows <- nrow(DailyOccupancyRate_yesterday)
if (total_rows>0){
  DailyOccupancyRate_yesterday$id <- as.integer(seq(max(daily_occupancy_DB$id)+1,
                                              max(daily_occupancy_DB$id)+total_rows,1))
  
  DailyOccupancyRate_yesterday <- DailyOccupancyRate_yesterday[,c(ncol(DailyOccupancyRate_yesterday),
                                                    c(1:ncol(DailyOccupancyRate_yesterday)-1))]
  
  ## daily appended table
  dbWriteTable(mydb, "daily_occupancy", DailyOccupancyRate_yesterday, append=TRUE, row.names=F, overwrite=FALSE) # append table
}

daily_occupancy_DB <- as.data.frame(tbl(con,"daily_occupancy"))

daily_occupancy_xts <- daily_occupancy_DB %>% 
  dplyr::group_by(date(date_consumption)) %>%
  dplyr::summarise(DailyOccupancy=round(mean(occupancy_rate),2))
colnames(daily_occupancy_xts)[1] <- "date"
Daily_Occupancy_xts <- xts(daily_occupancy_xts$DailyOccupancy,daily_occupancy_xts$date)
colnames(Daily_Occupancy_xts) <- "DailyOccupancy"

save(Daily_Occupancy_xts,file="/srv/shiny-server/DataAnalyticsPortal/data/Daily_Occupancy_xts.RData")

dbDisconnect(mydb)
time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_DailyOccupancyRate_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)