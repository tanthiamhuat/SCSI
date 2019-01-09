rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

monthly_occupancy_DB <- as.data.frame(tbl(con,"monthly_occupancy"))

Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(year=year(adjusted_date),month=month(adjusted_date),day=day(adjusted_date)) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,year,month,day) %>%
  arrange(adjusted_date)

MonthlyOccupancyRate <- PunggolConsumption_SUB %>% group_by(service_point_sn,year,month,day) %>%
                        dplyr::filter(!is.na(adjusted_consumption)) %>%
                        dplyr::summarise(daily_consumption=sum(adjusted_consumption),   
                                  lastupdated =max(adjusted_date)) %>% 
                        dplyr::mutate(
                              zero_daily_consumption = ifelse(daily_consumption==0,1,0),
                              Accumulated_zero_daily_consumption = cumsum(zero_daily_consumption),
                              TotalDays = n(),
                              occupancy_rate = round((1-Accumulated_zero_daily_consumption/TotalDays)*100,1),
                              occupancy_days = round((occupancy_rate*TotalDays)/100,0)
                              ) %>%
                        group_by(service_point_sn,year,month) %>%
                        filter(day==max(day)) %>%
                        select(service_point_sn,occupancy_rate,occupancy_days,lastupdated)

MonthlyOccupancyRate <- MonthlyOccupancyRate[,c("service_point_sn","occupancy_rate","occupancy_days","lastupdated")]

MonthlyOccupancyRate$occupancy_days <- as.integer(MonthlyOccupancyRate$occupancy_days)

MonthlyOccupancyRate <- data.frame(MonthlyOccupancyRate)  ## must be data.frame to be able to write to database

MonthlyOccupancyRate_lastmonth <- MonthlyOccupancyRate %>% dplyr::filter(year(lastupdated)==year(today()-1) & 
                                                                         month(lastupdated)==month(today()-1))

total_rows <- nrow(MonthlyOccupancyRate_lastmonth)
if (total_rows>0){
  MonthlyOccupancyRate_lastmonth$id <- as.integer(seq(max(monthly_occupancy_DB$id)+1,
                                                    max(monthly_occupancy_DB$id)+total_rows,1))
  
  MonthlyOccupancyRate_lastmonth <- MonthlyOccupancyRate_lastmonth[,c(ncol(MonthlyOccupancyRate_lastmonth),
                                                                  c(1:ncol(MonthlyOccupancyRate_lastmonth)-1))]
  
  ## daily appended table
  dbWriteTable(mydb, "monthly_occupancy", MonthlyOccupancyRate_lastmonth, append=TRUE, row.names=F, overwrite=FALSE) # append table
  dbDisconnect(mydb)
}

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyOccupancyRate_V2 successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)