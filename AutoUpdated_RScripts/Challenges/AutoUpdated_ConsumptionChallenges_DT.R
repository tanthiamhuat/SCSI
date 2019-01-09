## Monthly update of consumption_challenges table (over-write) on the first day of the month at 11:56am

## mean (for daily, remove zero, but not for hourly), i.e
## mean(dailyconsumption_lastmonth[dailyconsumption_lastmonth!=0],na.rm = TRUE)
## mean(hourlyconsumption_last3months,na.rm = TRUE)

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,stringr,ISOweek,data.table,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

last3month <- month(floor_date(Sys.Date() - months(c(1,2,3)), "month"))
lastmonth <- month(floor_date(Sys.Date() - months(1), "month"))

family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))) %>%
          group_by(id_service_point) %>%
          dplyr::filter(move_in_date==max(move_in_date))

servicepoint <- as.data.frame(tbl(con,"service_point")) %>% dplyr::filter(!service_point_sn %in% c("3100507837M","3100507837B"))
## "3100660792" AHL
                              
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id")) 

consumption_last6months_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last6months_servicepoint.fst",as.data.table=TRUE)
PunggolYuhua_SUB <- consumption_last6months_servicepoint[!(room_type %in% c("NIL","HDBCD","OTHER","Nil")) & !(is.na(room_type)) & 
                                                          meter_type =="SUB" & site %in% c("Punggol","Yuhua","Whampoa") &
                                                          !is.na(adjusted_consumption) &
                                                          service_point_sn %in% family_servicepoint$service_point_sn]

PunggolYuhua_SUB$week <- gsub("-W","_",str_sub(date2ISOweek(date(PunggolYuhua_SUB$date_consumption)),end = -3)) # convert date to week

#### ----to compute highest_day based on 7 days of Monday, Tuesday, Wednesday, Thursday, Friday, Saturday and Sunday------ #######
DailyPunggolYuhuaConsumption_last3months <- PunggolYuhua_SUB %>% 
                                       dplyr::mutate(date=date(date_consumption),month=month(date_consumption),day=day(date_consumption),
                                                     highest_day=weekdays(date_consumption)) %>%
                                       group_by(service_point_sn,date,month,day,week,highest_day) %>%
                                       dplyr::filter(month %in% last3month) %>%
                                       dplyr::summarise(dailyconsumption=sum(adjusted_consumption)) %>%
                                       dplyr::filter(service_point_sn !="3101127564") %>% # exclude ChildCare 
                                       dplyr::mutate(weekdayend=ifelse(highest_day=="Saturday" |highest_day=="Sunday","Weekends","Weekdays")) 

highest_day_tmp <- DailyPunggolYuhuaConsumption_last3months 
highest_day_tmp$dailyconsumption[which(highest_day_tmp$dailyconsumption==0)] <- NA
highest_day_tmp1 <- highest_day_tmp %>%
                    dplyr::group_by(service_point_sn,highest_day) %>%
                    dplyr::summarise(averagedailyconsumption_last3months=round(mean(dailyconsumption,na.rm = TRUE),2)) 

highest_day_tmp2 <- highest_day_tmp1 %>% dplyr::group_by(service_point_sn) %>% 
                    dplyr::summarise(averagedailyconsumption_last3months=max(averagedailyconsumption_last3months,na.rm = TRUE)) %>%
                    dplyr::filter(!is.nan(averagedailyconsumption_last3months))
highest_day <-inner_join(highest_day_tmp1,highest_day_tmp2) 

DailyPunggolYuhuaConsumption_lastmonth <- PunggolYuhua_SUB %>%
  dplyr::mutate(date=date(date_consumption),month=month(date_consumption),day=day(date_consumption),highest_day=weekdays(date_consumption)) %>%
  group_by(service_point_sn,date,month,day,highest_day) %>%
  dplyr::filter(month==lastmonth) %>%
  dplyr::summarise(dailyconsumption_lastmonth=sum(adjusted_consumption)) %>%
  dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 

highest_day_result <- right_join(DailyPunggolYuhuaConsumption_lastmonth,highest_day,by=c("service_point_sn","highest_day")) %>%
                      group_by(service_point_sn,highest_day,averagedailyconsumption_last3months) %>%
                      dplyr::summarise(averagehighestday_consumption=mean(dailyconsumption_lastmonth[dailyconsumption_lastmonth!=0],na.rm = TRUE)) %>%
                      dplyr::mutate(highest_day_trend=round((averagehighestday_consumption-averagedailyconsumption_last3months)/averagedailyconsumption_last3months*100),
                                    highest_day_result=ifelse((highest_day_trend < 0 & highest_day_trend >-100),"TRUE","FALSE")) %>%
                      dplyr::filter(!(is.na(highest_day_trend))) %>%
                      as.data.frame() 

#### ----to compute highest_weekperiod based on Weekdays and Weekends ---------- #######
average_weekdays_last3months <- DailyPunggolYuhuaConsumption_last3months %>%
                                group_by(service_point_sn) %>%
                                dplyr::filter(weekdayend=="Weekdays") %>%
                                dplyr::summarise(averageweekdaysconsumption_last3months=mean(dailyconsumption[dailyconsumption!=0],na.rm = TRUE)) 

average_weekends_last3months <- DailyPunggolYuhuaConsumption_last3months %>%
                                group_by(service_point_sn) %>%
                                dplyr::filter(weekdayend=="Weekends") %>%
                                dplyr::summarise(averageweekendsconsumption_last3months=mean(dailyconsumption[dailyconsumption!=0],na.rm = TRUE)) 

average_weekdayends_last3months <- inner_join(average_weekdays_last3months,average_weekends_last3months,by="service_point_sn") %>%
                                   dplyr::mutate(highest_week=ifelse(averageweekdaysconsumption_last3months > averageweekendsconsumption_last3months,
                                                 "Weekdays","Weekends"))

DailyPunggolYuhuaConsumption_lastmonth <- PunggolYuhua_SUB %>%
                                    dplyr::mutate(month=month(date_consumption),day=day(date_consumption),wd=weekdays(date_consumption), 
                                                  weekdayend=ifelse(wd=="Saturday" |wd=="Sunday","Weekends","Weekdays")) %>%
                                    group_by(service_point_sn,month,day,wd,week,weekdayend) %>%
                                    dplyr::filter(month==lastmonth) %>%
                                    dplyr::summarise(dailyconsumption_lastmonth=sum(adjusted_consumption)) %>%
                                    dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 

AverageConsumption_last_weekdays <- DailyPunggolYuhuaConsumption_lastmonth %>%
                                    group_by(service_point_sn) %>%
                                    dplyr::filter(weekdayend=="Weekdays") %>%
                                    dplyr::summarise(averageweekdayconsumption_lastmonth=mean(dailyconsumption_lastmonth[dailyconsumption_lastmonth!=0],na.rm = TRUE)) %>%
                                    dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 
AverageConsumption_last_weekends <- DailyPunggolYuhuaConsumption_lastmonth %>%
                                    group_by(service_point_sn) %>%
                                    dplyr::filter(weekdayend=="Weekends") %>%
                                    dplyr::summarise(averageweekendconsumption_lastmonth=mean(dailyconsumption_lastmonth[dailyconsumption_lastmonth!=0],na.rm = TRUE)) %>%
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
HourlyPunggolYuhuaConsumption_last3months <- PunggolYuhua_SUB %>% 
                                        dplyr::mutate(day=day(date_consumption),month=month(date_consumption),
                                                      time=strftime(date_consumption,format="%H:%M:%S"),H=as.numeric(substr(time,1,2))) %>%
                                        group_by(service_point_sn,day,month,week,H) %>%
                                        dplyr::filter(month %in% last3month & service_point_sn !="3101127564") %>% # exclude ChildCare 
                                        dplyr::summarise(hourlyconsumption_last3months=sum(adjusted_consumption))

highest_period <- HourlyPunggolYuhuaConsumption_last3months %>%
                  dplyr::mutate(highest_period=ifelse((H>=6 & H<=11),"Morning",
                                               ifelse((H>=12 & H<=17),"Afternoon",
                                               ifelse((H>=18 & H<=23),"Evening",
                                               ifelse((H>=0 & H<=5),"Night",NA))))) %>%
                  group_by(service_point_sn,highest_period) %>%
                  dplyr::summarise(averagehourlyconsumption_last3months=round(mean(hourlyconsumption_last3months,na.rm = TRUE),2)) %>%
                  dplyr::filter(averagehourlyconsumption_last3months==max(averagehourlyconsumption_last3months) & 
                                averagehourlyconsumption_last3months !=0) 

HourlyPunggolYuhuaConsumption_lastmonth <- PunggolYuhua_SUB %>%
                                     dplyr::mutate(month=month(date_consumption),
                                                   time=strftime(date_consumption,format="%H:%M:%S"),H=as.numeric(substr(time,1,2))) %>%
                                     dplyr::mutate(highest_period=ifelse((H>=6 & H<=11),"Morning",
                                                                  ifelse((H>=12 & H<=17),"Afternoon",
                                                                  ifelse((H>=18 & H<=23),"Evening",
                                                                  ifelse((H>=0 & H<=5),"Night",NA))))) %>%                                   
                                     group_by(service_point_sn,highest_period) %>%
                                     dplyr::filter(month==lastmonth & service_point_sn !="3101127564") %>% # exclude ChildCare
                                     dplyr::summarise(averagehourlyconsumption_lastmonth=round(mean(adjusted_consumption,na.rm = TRUE),2)) 

highest_period_result <- right_join(HourlyPunggolYuhuaConsumption_lastmonth,highest_period,by=c("service_point_sn","highest_period")) %>%
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

consumption_challenges$id <- rownames(consumption_challenges)
consumption_challenges$id <- as.integer(consumption_challenges$id)
consumption_challenges$date_created <- today()
consumption_challenges <- consumption_challenges %>% dplyr::select_("id","family_id","highest_day","highest_day_result",
                                                                    "highest_week","week_trend_result","highest_period","highest_period_result",
                                                                    "date_created")
dbSendQuery(mydb, "delete from consumption_challenges")
dbWriteTable(mydb, "consumption_challenges", consumption_challenges, append=FALSE, row.names=F, overwrite=TRUE) # over-write table

dbSendQuery(proddb, "delete from consumption_challenges")
dbWriteTable(proddb, "consumption_challenges", consumption_challenges, append=FALSE, row.names=F, overwrite=TRUE) # over-write table

dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ConsumptionChallenges_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)
