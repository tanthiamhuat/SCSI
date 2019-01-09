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

Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst",old_format = TRUE)
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,date_consumption,day,month) %>%
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
dbWriteTable_output <- try(
  dbWriteTable(mydb, "weekly_occupancy", WeeklyOccupancyRate, append=FALSE, row.names=F, overwrite=TRUE)
)
## this table is maintained to be over-written, as on the new week of the Monday, not all data in the 
## previous week may have arrived. Those data arrived late would then be updated in the following week.

if (class(dbWriteTable_output)=='try-error'){
  send.mail(from = "shd-snfr-autoreport@suez.com",
            to = c("thiamhuat.tan@suez.com"),
            subject = "weekly_occupancy table Not Updated Properly",
            body = 'The weekly_occupancy table is not updated properly.',
            html = TRUE,
            inline = TRUE,
            smtp = list(host.name = "smtp.office365.com",
                        port = 587,
                        user.name = "thiamhuat.tan@suezenvironnement.com",
                        passwd = "Joy03052007#",
                        tls = TRUE),
            authenticate = TRUE,
            send = TRUE)
  
  pbPost("note", title="The weekly_occupancy table is not updated properly.",
         apikey = 'o.3hyhI3lAxvjlB8p1I2Pu2sowkE7YNNqF') 
}

dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_WeeklyOccupancyRate_V2 successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)