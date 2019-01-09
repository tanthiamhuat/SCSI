rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

# SUM of hourly consumptions (adjusted_consumption) of the month
# = SUM of hourly consumptions (interpolated_consumption) of the month
# = SUM of daily consumptions of the month (nett_consumption + overconsumption)

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

today <- today()

# load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")  # from DB
# DB_Consumption <- as.data.frame(DB_Consumption)

# DB_Consumption_lastyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_lastyear.fst")
# DB_Consumption_lastyear$adjusted_date <- DB_Consumption_lastyear$date_consumption-lubridate::hours(1)
# DB_Consumption_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_thisyear.fst")
# DB_Consumption <- rbind(DB_Consumption_lastyear,DB_Consumption_thisyear)

DB_Consumption <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_thisyear.fst")

### adjusted_date column to be added
### based on date_consumption minus one hour
# DB_Consumption$adjusted_date <- DB_Consumption$date_consumption-lubridate::hours(1)

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
DB_Consumption_servicepoint  <- inner_join(DB_Consumption,servicepoint,by=c("id_service_point"="id")) 

DB_Consumption_family_servicepoint  <- inner_join(DB_Consumption,family_servicepoint,by=c("id_service_point")) 

daily_consumption <- as.data.frame(tbl(con,"daily_consumption")) %>% dplyr::filter(service_point_sn!="3100660792")
user_trends <- as.data.frame(tbl(con,"user_trends"))  
user_trends_date_created <- unique(user_trends$date_created)
daily_consumption_extracted <- daily_consumption %>% dplyr::filter(date(date_consumption)==user_trends_date_created-1)
user_trends_daily_consumption <- inner_join(user_trends,daily_consumption_extracted,by="service_point_sn") %>%
                                 dplyr::select_("service_point_sn","yesterday_consumption","nett_consumption","overconsumption") %>%
                                 dplyr::mutate(daily_consumption=nett_consumption+overconsumption, Difference=yesterday_consumption-daily_consumption)
  
user_trends_daily_consumption_Inconsistencies <- user_trends_daily_consumption[which(user_trends_daily_consumption$Difference !=0),]

daily_consumption_Monthly <- as.data.frame(tbl(con,"daily_consumption")) %>% dplyr::filter(service_point_sn !="3100660792" & service_point_sn !="3101127564") %>%
                             dplyr::mutate(year=year(date_consumption),month=month(date_consumption),
                                           daily_consumption=nett_consumption+overconsumption) %>%
                             dplyr::select_("service_point_sn","year","month","daily_consumption") %>%
                             group_by(year,month) %>%
                             dplyr::summarise(daily_consumption_accumulated=sum(daily_consumption,na.rm=TRUE)) 

daily_consumption$date_consumption <- as.Date(daily_consumption$date_consumption)+1

daily_consumption_servicepoint <- inner_join(daily_consumption,servicepoint,by="service_point_sn") %>%
                                  dplyr::select_("id.y","service_point_sn","nett_consumption","overconsumption","date_consumption") %>%
                                  dplyr::rename(id_service_point=id.y)

Consumption_Daily <- DB_Consumption_servicepoint %>% 
                     dplyr::mutate(year=year(date_consumption),date_consumption=date(adjusted_date)) %>%
                     dplyr::filter(date_consumption < today & service_point_sn !="3100660792" & service_point_sn !="3101127564") %>%
                     dplyr::select_("id_service_point","adjusted_consumption","date_consumption") %>%
                     group_by(id_service_point,date_consumption) %>%
                     dplyr::summarise(adjusted_consumption_accumulated=sum(adjusted_consumption,na.rm=TRUE))

daily_consumption_Consumption_Inconsistencies <- inner_join(daily_consumption_servicepoint,Consumption_Daily,by=c("id_service_point","date_consumption")) %>%
                                 dplyr::mutate(daily_consumption_accumulated=nett_consumption+overconsumption,
                                               year=year(date_consumption),month=month(date_consumption)) %>%
                                 group_by(year,month) %>%
                                 dplyr::summarise(daily_consumption_Total=sum(daily_consumption_accumulated,na.rm=TRUE),
                                                  adjusted_consumption_Total=sum(adjusted_consumption_accumulated,na.rm=TRUE)) %>%
                                 dplyr::mutate(Difference=adjusted_consumption_Total-daily_consumption_Total,
                                               DiscrepancyRate=round(Difference/adjusted_consumption_Total*100,2))

daily_consumption_Consumption_Inconsistencies <- daily_consumption_Consumption_Inconsistencies[nrow(daily_consumption_Consumption_Inconsistencies),]

Updated_DateTime_AWSTables <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(user_trends_daily_consumption_Inconsistencies,
     daily_consumption_Consumption_Inconsistencies,Updated_DateTime_AWSTables,
     file = "/srv/shiny-server/DataAnalyticsPortal/data/DiscrepancyRate_Consumption.RData")
  
time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_AWSTables successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
