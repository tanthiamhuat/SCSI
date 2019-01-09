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

PunggolYuhua_SUB <- PunggolYuhua_SUB[,c("week_number","wd_numeric"):= 
                                     list(paste(year(date(date_consumption)),"_",strftime(date(date_consumption),format="%W"),sep=""),
                                          wday(date(date_consumption))-1)]

# replace wd_numeric 0 with 7
PunggolYuhua_SUB$wd_numeric <- gsub(0,7,PunggolYuhua_SUB$wd_numeric)

WeeklyOccupancyRate <- PunggolYuhua_SUB %>% 
                       dplyr::group_by(service_point_sn,week_number,wd_numeric) %>%
                       dplyr::summarise(daily_consumption=sum(adjusted_consumption)) %>% 
                       dplyr::mutate(
                                zero_daily_consumption = ifelse(daily_consumption==0,1,0),
                                Accumulated_zero_daily_consumption = cumsum(zero_daily_consumption),
                                occupancy_rate = round((1-Accumulated_zero_daily_consumption/7)*100,1)) %>%
                       dplyr::filter(wd_numeric==7) %>%  ## Sunday
                       select(service_point_sn,occupancy_rate,week_number)

todayweek <- paste(year(date(today())),"_",strftime(date(today()),format="%W"),sep="")
WeeklyOccupancyRate <- WeeklyOccupancyRate %>% dplyr::filter(week_number!=todayweek)  
## remove today's week, in case it is not last day of week (Sunday)

## replace 2017_00 with 2016_52
WeeklyOccupancyRate$week_number <- gsub("2017_00","2016_52",WeeklyOccupancyRate$week_number)

WeeklyOccupancyRate$id <- rownames(WeeklyOccupancyRate)
WeeklyOccupancyRate$id <- as.integer(WeeklyOccupancyRate$id)

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

WeeklyOccupancyRate <- inner_join(WeeklyOccupancyRate,Week.date,by=c("week_number"="week")) %>% 
                            dplyr::mutate(lastupdated=beg+7) %>%
                            dplyr::select_("id","service_point_sn","week_number","occupancy_rate","lastupdated")

WeeklyOccupancyRate <- data.frame(WeeklyOccupancyRate)  ## must be data.frame to be able to write to database

dbSendQuery(mydb, "delete from weekly_occupancy")

dbWriteTable(mydb, "weekly_occupancy", WeeklyOccupancyRate, append=FALSE, row.names=F, overwrite=TRUE)
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("Overwrite_WeeklyOccupancyRate_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)