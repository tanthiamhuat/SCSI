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

Punggol_2016 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2016.fst")[,1:12]
Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst")[,1:12]
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")

Punggol_All <- rbind(rbind(Punggol_2016,Punggol_2017),Punggol_thisyear)

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(Date.Time=round_date(ymd_hms(date_consumption),"hour"),Date=date(Date.Time)) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,date_consumption,Date) %>%
  arrange(date_consumption)

DailyOccupancyRate <- PunggolConsumption_SUB %>% 
                      group_by(service_point_sn,Date) %>%
                      filter(!is.na(adjusted_consumption)) %>%
                      dplyr::mutate(
                            zero_hourly_consumption = ifelse(adjusted_consumption==0, 1, 0),
                            Accumulated_zero_hourly_consumption = cumsum(zero_hourly_consumption),
                            TotalHours = n(),
                            occupancy_rate = round((1-Accumulated_zero_hourly_consumption/TotalHours)*100,1)
                            ) %>%
                      filter(date_consumption==max(date_consumption)) %>%
                      select(service_point_sn,date_consumption,occupancy_rate) %>%
                      arrange(date_consumption)

DailyOccupancyRate$day <- NULL
DailyOccupancyRate$month <- NULL
DailyOccupancyRate$id <- rownames(DailyOccupancyRate)
DailyOccupancyRate <- DailyOccupancyRate[,c("id","service_point_sn","occupancy_rate","date_consumption")]

DailyOccupancyRate <- DailyOccupancyRate %>% dplyr::filter(date(date_consumption)<today())

DailyOccupancyRate$id <- as.integer(DailyOccupancyRate$id)

DailyOccupancyRate <- data.frame(DailyOccupancyRate)  ## must be data.frame to be able to write to database

dbSendQuery(mydb, "delete from daily_occupancy")

dbWriteTable_output <- try(
  dbWriteTable(mydb, "daily_occupancy", DailyOccupancyRate, append=FALSE, row.names=F, overwrite=TRUE)
)

if (class(dbWriteTable_output)=='try-error'){
  send.mail(from = "shd-snfr-autoreport@suez.com",
            to = c("thiamhuat.tan@suez.com"),
            subject = "daily_occupancy table Not Updated Properly",
            body = 'The daily_occupancy table is not updated properly.',
            html = TRUE,
            inline = TRUE,
            smtp = list(host.name = "smtp.office365.com",
                        port = 587,
                        user.name = "thiamhuat.tan@suezenvironnement.com",
                        passwd = "Joy03052007#",
                        tls = TRUE),
            authenticate = TRUE,
            send = TRUE)
  
  pbPost("note", title="The daily_occupancy table is not updated properly.",
         apikey = 'o.3hyhI3lAxvjlB8p1I2Pu2sowkE7YNNqF') 
}

dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_DailyOccupancyRate successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)