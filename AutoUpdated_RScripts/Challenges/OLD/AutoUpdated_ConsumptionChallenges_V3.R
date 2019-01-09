## Monthly update of consumption_challenges table (over-write) on Monday at 11:56am

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,stringr,ISOweek,data.table)

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))) %>%
          group_by(id_service_point) %>%
          dplyr::filter(move_in_date==max(move_in_date))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id")) 

load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(is.na(adjusted_consumption)) & !(room_type %in% c("NIL")) & !(is.na(room_type)) & service_point_sn %in% family_servicepoint$service_point_sn) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

PunggolConsumption_SUB$week <- gsub("-W","_",str_sub(date2ISOweek(date(PunggolConsumption_SUB$adjusted_date)),end = -3)) # convert date to week

last3month <- month(floor_date(Sys.Date() - months(c(1,2,3)), "month"))
lastmonth <- month(floor_date(Sys.Date() - months(1), "month"))

#### ----to compute highest_day based on 7 days of Monday, Tuesday, Wednesday, Thursday, Friday, Saturday and Sunday------ #######
DailyPunggolConsumption_last3months <- PunggolConsumption_SUB %>% 
                                       dplyr::mutate(date=date(adjusted_date),highest_day=weekdays(adjusted_date)) %>%
                                       group_by(service_point_sn,date,month,day,week,highest_day) %>%
                                       dplyr::filter(month %in% last3month) %>%
                                       dplyr::summarise(dailyconsumption=sum(adjusted_consumption)) %>%
                                       dplyr::filter(service_point_sn !="3101127564") %>% # exclude ChildCare 
                                       dplyr::mutate(weekdayend=ifelse(highest_day=="Saturday" |highest_day=="Sunday","Weekends","Weekdays")) 

highest_day <- DailyPunggolConsumption_last3months %>%
               group_by(service_point_sn,highest_day) %>%
               dplyr::summarise(averagedailyconsumption_last3months=round(mean(dailyconsumption),2)) %>%
               dplyr::filter(averagedailyconsumption_last3months==max(averagedailyconsumption_last3months)) %>% 
               as.data.frame()

DailyPunggolConsumption_lastmonth <- PunggolConsumption_SUB %>%
  dplyr::mutate(date=date(adjusted_date),highest_day=weekdays(adjusted_date)) %>%
  group_by(service_point_sn,date,month,day,highest_day) %>%
  dplyr::filter(month==lastmonth) %>%
  dplyr::summarise(dailyconsumption_lastmonth=sum(adjusted_consumption)) %>%
  dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 

highest_day_result <- right_join(DailyPunggolConsumption_lastmonth,highest_day,by=c("service_point_sn","highest_day")) %>%
                      group_by(service_point_sn,highest_day,averagedailyconsumption_last3months) %>%
                      dplyr::summarise(averagehighestday_consumption=mean(dailyconsumption_lastmonth)) %>%
                      dplyr::mutate(highest_day_trend=round((averagehighestday_consumption-averagedailyconsumption_last3months)/averagedailyconsumption_last3months*100),
                                    highest_day_result=ifelse((highest_day_trend < 0 & highest_day_trend >-100),"TRUE","FALSE")) %>%
                      dplyr::filter(!(is.na(highest_day_trend))) %>%
                      as.data.frame() 

#### ----to compute highest_weekperiod based on Weekdays and Weekends ---------- #######
average_weekdays_last3months <- DailyPunggolConsumption_last3months %>%
                                group_by(service_point_sn) %>%
                                dplyr::filter(weekdayend=="Weekdays") %>%
                                dplyr::summarise(averageweekdaysconsumption_last3months=mean(dailyconsumption)) 

average_weekends_last3months <- DailyPunggolConsumption_last3months %>%
                                group_by(service_point_sn) %>%
                                dplyr::filter(weekdayend=="Weekends") %>%
                                dplyr::summarise(averageweekendsconsumption_last3months=mean(dailyconsumption)) 

average_weekdayends_last3months <- inner_join(average_weekdays_last3months,average_weekends_last3months,by="service_point_sn") %>%
                                   dplyr::mutate(highest_week=ifelse(averageweekdaysconsumption_last3months > averageweekendsconsumption_last3months,
                                                 "Weekdays","Weekends"))

DailyPunggolConsumption_lastmonth <- PunggolConsumption_SUB %>%
                                    dplyr::mutate(wd=weekdays(adjusted_date), 
                                                  weekdayend=ifelse(wd=="Saturday" |wd=="Sunday","Weekends","Weekdays")) %>%
                                    group_by(service_point_sn,month,day,wd,week,weekdayend) %>%
                                    dplyr::filter(month==lastmonth) %>%
                                    dplyr::summarise(dailyconsumption_lastmonth=sum(adjusted_consumption)) %>%
                                    dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 

AverageConsumption_last_weekdays <- DailyPunggolConsumption_lastmonth %>%
                                    group_by(service_point_sn) %>%
                                    dplyr::filter(weekdayend=="Weekdays") %>%
                                    dplyr::summarise(averageweekdayconsumption_lastmonth=mean(dailyconsumption_lastmonth)) %>%
                                    dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 
AverageConsumption_last_weekends <- DailyPunggolConsumption_lastmonth %>%
                                    group_by(service_point_sn) %>%
                                    dplyr::filter(weekdayend=="Weekends") %>%
                                    dplyr::summarise(averageweekendconsumption_lastmonth=mean(dailyconsumption_lastmonth)) %>%
                                    dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 

weekdays_trend <- inner_join(average_weekdays_last3months,AverageConsumption_last_weekdays,by="service_point_sn") %>%
                  dplyr::mutate(weekdays_trend=round((averageweekdayconsumption_lastmonth-averageweekdaysconsumption_last3months)/averageweekdaysconsumption_last3months*100),
                                weekdays_trend_result=ifelse((weekdays_trend < 0 & weekdays_trend >-100),"TRUE","FALSE"))
weekends_trend <- inner_join(average_weekends_last3months,AverageConsumption_last_weekends,by="service_point_sn") %>%
                  dplyr::mutate(weekends_trend=round((averageweekendconsumption_lastmonth-averageweekendsconsumption_last3months)/averageweekendsconsumption_last3months*100),
                                weekends_trend_result=ifelse((weekends_trend < 0 & weekends_trend >-100),"TRUE","FALSE"))

week_trend <- inner_join(weekdays_trend,weekends_trend,by="service_point_sn")
week_trend <- inner_join(week_trend,average_weekdayends_last3months,by=c("service_point_sn","averageweekdaysconsumption_last3months","averageweekendsconsumption_last3months")) %>%
              dplyr::mutate(week_trend=ifelse(highest_week=="Weekdays",weekdays_trend,weekends_trend),
                            week_trend_result=ifelse(highest_week=="Weekdays",weekdays_trend_result,weekends_trend_result)) %>%
              dplyr::select_("service_point_sn","highest_week","week_trend","week_trend_result")

#### ----to compute highest_period based on 4 durations of Morning, Afternoon, Evening and Night ---------- #######
HourlyPunggolConsumption_last3months <- PunggolConsumption_SUB %>% 
                                        dplyr::mutate(time=strftime(adjusted_date,format="%H:%M:%S"),H=as.numeric(substr(time,1,2))) %>%
                                        group_by(service_point_sn,day,month,week,H) %>%
                                        dplyr::filter(month %in% last3month & service_point_sn !="3101127564") %>% # exclude ChildCare 
                                        dplyr::summarise(hourlyconsumption_last3months=sum(adjusted_consumption))

highest_period <- HourlyPunggolConsumption_last3months %>%
                  dplyr::mutate(highest_period=ifelse((H>=6 & H<=11),"Morning",
                                               ifelse((H>=12 & H<=17),"Afternoon",
                                               ifelse((H>=18 & H<=23),"Evening",
                                               ifelse((H>=0 & H<=5),"Night",NA))))) %>%
                  group_by(service_point_sn,highest_period) %>%
                  dplyr::summarise(averagehourlyconsumption_last3months=round(mean(hourlyconsumption_last3months),2)) %>%
                  dplyr::filter(averagehourlyconsumption_last3months==max(averagehourlyconsumption_last3months) & 
                                averagehourlyconsumption_last3months !=0) 

HourlyPunggolConsumption_lastmonth <- PunggolConsumption_SUB %>%
                                     dplyr::mutate(time=strftime(adjusted_date,format="%H:%M:%S"),H=as.numeric(substr(time,1,2))) %>%
                                     dplyr::mutate(highest_period=ifelse((H>=6 & H<=11),"Morning",
                                                                  ifelse((H>=12 & H<=17),"Afternoon",
                                                                  ifelse((H>=18 & H<=23),"Evening",
                                                                  ifelse((H>=0 & H<=5),"Night",NA))))) %>%                                   
                                     group_by(service_point_sn,highest_period) %>%
                                     dplyr::filter(month==lastmonth & service_point_sn !="3101127564") %>% # exclude ChildCare
                                     dplyr::summarise(averagehourlyconsumption_lastmonth=round(mean(adjusted_consumption),2)) 

highest_period_result <- right_join(HourlyPunggolConsumption_lastmonth,highest_period,by=c("service_point_sn","highest_period")) %>%
                         dplyr::mutate(highest_period_trend=round((averagehourlyconsumption_lastmonth-averagehourlyconsumption_last3months)/averagehourlyconsumption_last3months*100),
                                       highest_period_result=ifelse((highest_period_trend < 0 & highest_period_trend >-100),"TRUE","FALSE"))

consumption_challenges_day <- highest_day_result %>% dplyr::select_("service_point_sn","highest_day","highest_day_result") %>% as.data.frame()
consumption_challenges_week <- week_trend %>% dplyr::select_("service_point_sn","highest_week","week_trend_result") %>% as.data.frame()
consumption_challenges_period <- highest_period_result %>% dplyr::select_("service_point_sn","highest_period","highest_period_result") %>% as.data.frame()

consumption_challenges <- inner_join(inner_join(consumption_challenges_day,consumption_challenges_week,by="service_point_sn"),
                                     consumption_challenges_period,by="service_point_sn")

match <- match(consumption_challenges$service_point_sn,family_servicepoint$service_point_sn,nomatch=NA)
match <- match[!is.na(match)]
consumption_challenges$family_id <- family_servicepoint$id[match]
#consumption_challenges$family_id <- family_servicepoint$id_service_point[match(consumption_challenges$service_point_sn,family_servicepoint$service_point_sn)]

consumption_challenges$id <- rownames(consumption_challenges)
consumption_challenges$id <- as.integer(consumption_challenges$id)
consumption_challenges$date_created <- today()
consumption_challenges <- consumption_challenges %>% dplyr::select_("id","family_id","highest_day","highest_day_result",
                                                                    "highest_week","week_trend_result","highest_period","highest_period_result",
                                                                    "date_created")
dbSendQuery(mydb, "delete from consumption_challenges")
dbWriteTable(mydb, "consumption_challenges", consumption_challenges, append=FALSE, row.names=F, overwrite=TRUE) # over-write table
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ConsumptionChallenges_V2 successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)