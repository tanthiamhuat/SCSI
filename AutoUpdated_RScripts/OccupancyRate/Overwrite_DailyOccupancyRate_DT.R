rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,RPushbullet,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

consumption_2016 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2016.fst",as.data.table=TRUE)
consumption_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2017.fst",as.data.table=TRUE)
consumption_previousyears <- rbind(consumption_2016,consumption_2017)

servicepoint <- as.data.table(tbl(con,"service_point"))

# set the ON clause as keys of the tables:
setkey(consumption_previousyears,id_service_point)
setkey(servicepoint,id)

consumption_previousyears_servicepoint <- consumption_previousyears[servicepoint, nomatch=0]
consumption_previousyears_servicepoint <- consumption_previousyears_servicepoint[, .(service_point_sn,block,floor,unit,room_type,site,meter_type,
                                                                                     interpolated_consumption,adjusted_consumption,date_consumption)]

consumption_thisyear_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_thisyear_servicepoint.fst",as.data.table=TRUE)

consumption_servicepoint <- rbind(consumption_previousyears_servicepoint,consumption_thisyear_servicepoint)

PunggolYuhua_SUB <- consumption_servicepoint[!(room_type %in% c("NIL","HDBCD","OTHER","Nil")) & !(is.na(room_type)) &
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
DailyOccupancyRate$id <- rownames(DailyOccupancyRate)
DailyOccupancyRate <- DailyOccupancyRate[,c("id","service_point_sn","occupancy_rate","date_consumption")]

DailyOccupancyRate <- DailyOccupancyRate %>% dplyr::filter(date(date_consumption)<today())

DailyOccupancyRate$id <- as.integer(DailyOccupancyRate$id)

DailyOccupancyRate <- data.frame(DailyOccupancyRate)  ## must be data.frame to be able to write to database

dbSendQuery(mydb, "delete from daily_occupancy")

dbWriteTable(mydb, "daily_occupancy", DailyOccupancyRate, append=FALSE, row.names=F, overwrite=TRUE)
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("Overwrite_DailyOccupancyRate_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)