rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,dplyr,lubridate)

load("/srv/shiny-server/DataAnalyticsPortal/data/DT/RawConsumption.RData")

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

today <- today()
Non_24_Consumption <- RawConsumption %>% dplyr::filter(date(ReadingDate)>="2017-01-01" & date(ReadingDate) <= today-1) %>%
                      dplyr::mutate(Date=date(ReadingDate)) %>%
                      dplyr::group_by(Date,ExternalMeteringPointReference) %>%
                      dplyr::summarise(Count=n()) %>%
                      dplyr::filter(Count!=24) %>%
                      dplyr::arrange(Date) %>%
                      dplyr::summarise(Non24Count=n())

Full_24_Consumption <- RawConsumption %>% dplyr::filter(date(ReadingDate)>="2017-01-01" & date(ReadingDate) <= today-1 & !is.na(Consumption)) %>%
                       dplyr::mutate(Date=date(ReadingDate)) %>%
                       dplyr::group_by(Date,ExternalMeteringPointReference) %>%
                       dplyr::summarise(Count=n()) %>%
                       dplyr::filter(Count==24) %>%
                       dplyr::arrange(Date) %>%
                       dplyr::summarise(Full24Count=n())

Full_24_Consumption_NA <- RawConsumption %>% dplyr::filter(date(ReadingDate)>="2017-01-01" & date(ReadingDate) <= today-1) %>%
                          dplyr::mutate(Date=date(ReadingDate)) %>%
                          dplyr::group_by(Date,ExternalMeteringPointReference) %>%
                          dplyr::summarise(Count=n()) %>%
                          dplyr::filter(Count==24) %>%
                          dplyr::arrange(Date) %>%
                          dplyr::summarise(Full24CountNA=n())

MeterCountFull_24ContainsNA <- inner_join(Full_24_Consumption,Full_24_Consumption_NA,by="Date") %>%
                               dplyr::mutate(Full24ContainsNA_MeterCount=Full24CountNA-Full24Count) %>%
                               dplyr::select_("Date","Full24ContainsNA_MeterCount")

Updated_DateTime_NAConsumption <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(MeterCountFull_24ContainsNA,Updated_DateTime_NAConsumption,
     file = "/srv/shiny-server/DataAnalyticsPortal/data/MeterCountFull_24ContainsNA.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NAConsumption successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)