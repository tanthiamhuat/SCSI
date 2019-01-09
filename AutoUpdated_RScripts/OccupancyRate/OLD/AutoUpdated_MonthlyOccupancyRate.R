# rm(list=ls())  # remove all variables
# cat("\014")    # clear Console
# if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,RPushbullet,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M,year=year(adjusted_date)) %>%                  
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
MonthlyOccupancyRate[,1] <- NULL # remove month column

MonthlyOccupancyRate$id <- rownames(MonthlyOccupancyRate)
MonthlyOccupancyRate <- MonthlyOccupancyRate[,c("id","service_point_sn","occupancy_rate","occupancy_days","lastupdated")]

MonthlyOccupancyRate$id <- as.integer(MonthlyOccupancyRate$id)
MonthlyOccupancyRate$occupancy_days <- as.integer(MonthlyOccupancyRate$occupancy_days)

MonthlyOccupancyRate <- data.frame(MonthlyOccupancyRate)  ## must be data.frame to be able to write to database

dbSendQuery(mydb, "delete from monthly_occupancy")

dbWriteTable_output <- try(
  dbWriteTable(mydb, "monthly_occupancy", MonthlyOccupancyRate, append=FALSE, row.names=F, overwrite=TRUE)
)

# if (class(dbWriteTable_output)=='try-error'){
#   send.mail(from = "shd-snfr-autoreport@suez.com",
#             to = c("thiamhuat.tan@suez.com"),
#             subject = "monthly_occupancy table Not Updated Properly",
#             body = 'The monthly_occupancy table is not updated properly.',
#             html = TRUE,
#             inline = TRUE,
#             smtp = list(host.name = "smtp.office365.com",
#                         port = 587,
#                         user.name = "thiamhuat.tan@suezenvironnement.com",
#                         passwd = "Joy03052007###",
#                         tls = TRUE),
#             authenticate = TRUE,
#             send = TRUE)
#  
#   pbPost("note", title="The monthly_occupancy table is not updated properly.",
#          apikey = 'o.3hyhI3lAxvjlB8p1I2Pu2sowkE7YNNqF') 
# }

dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyOccupancyRate_V2 successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)