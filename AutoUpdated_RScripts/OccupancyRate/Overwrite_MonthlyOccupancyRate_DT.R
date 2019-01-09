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

PunggolYuhua_SUB <- PunggolYuhua_SUB[,c("Year","Month","Day"):= list
                                     (year(date_consumption),month(date_consumption),day(date_consumption))]
                                           
MonthlyOccupancyRate <- PunggolYuhua_SUB %>% group_by(service_point_sn,Year,Month,Day) %>%
                        dplyr::summarise(daily_consumption=sum(adjusted_consumption),   
                                  lastupdated =max(date_consumption)) %>% 
                        dplyr::mutate(
                              zero_daily_consumption = ifelse(daily_consumption==0,1,0),
                              Accumulated_zero_daily_consumption = cumsum(zero_daily_consumption),
                              TotalDays = n(),
                              occupancy_rate = round((1-Accumulated_zero_daily_consumption/TotalDays)*100,1),
                              occupancy_days = round((occupancy_rate*TotalDays)/100,0)
                              ) %>%
                        group_by(service_point_sn,Year,Month) %>%
                        filter(Day==max(Day)) %>%
                        select(service_point_sn,occupancy_rate,occupancy_days,lastupdated)

## Remove rows of year/month of today
MonthlyOccupancyRate$YearMonth <- paste(MonthlyOccupancyRate$Year,MonthlyOccupancyRate$Month,sep="_")
MonthlyOccupancyRate <- MonthlyOccupancyRate %>% dplyr::filter(YearMonth!= paste(year(today()),month(today()),sep="_"))

MonthlyOccupancyRate[,c(1,2,ncol(MonthlyOccupancyRate))] <- NULL # remove Year,Month,YearMonth columns

MonthlyOccupancyRate$id <- rownames(MonthlyOccupancyRate)
MonthlyOccupancyRate <- MonthlyOccupancyRate[,c("id","service_point_sn","occupancy_rate","occupancy_days","lastupdated")]

MonthlyOccupancyRate$id <- as.integer(MonthlyOccupancyRate$id)
MonthlyOccupancyRate$occupancy_days <- as.integer(MonthlyOccupancyRate$occupancy_days)

MonthlyOccupancyRate <- data.frame(MonthlyOccupancyRate)  ## must be data.frame to be able to write to database

dbSendQuery(mydb, "delete from monthly_occupancy")

dbWriteTable(mydb, "monthly_occupancy", MonthlyOccupancyRate, append=FALSE, row.names=F, overwrite=TRUE)
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("Overwrite_MonthlyOccupancyRate_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)