rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,tidyr,lubridate,RPostgreSQL,data.table,fst)

# run on 1st day of month

today <- today()-1  # get the previous month
#today <- as.Date("2017-10-31")
lastmonth <- month(today)
if (lastmonth==1) {
  last2month=12
} else {
  last2month=lastmonth-1
}
thisyear <- year(today)
lastyear <- thisyear
if (last2month==12){
  lastyear <- thisyear-1
}

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

monthly_report_DB <- as.data.frame(tbl(con,"monthly_report"))

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id")) 

Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type)) & service_point_sn %in% family_servicepoint$service_point_sn) %>%
  dplyr::mutate(year=year(adjusted_date),month=month(adjusted_date),day=day(adjusted_date)) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,year,month,day) %>%
  arrange(adjusted_date)

monthlyoccupancy <- as.data.frame(tbl(con,"monthly_occupancy")) %>% 
  dplyr::mutate(Year=year(date(lastupdated)),
                Month=month(date(lastupdated))) %>%
  dplyr::filter(service_point_sn !="3101127564" & # exclude child-care
                (date(lastupdated) >(today() %m-% months(2))))

dailyoccupancy <- as.data.frame(tbl(con,"daily_occupancy")) %>%
  dplyr::mutate(Date=date(date_consumption),Month=month(date(date_consumption)),
                Year=year(date_consumption),wd=weekdays(date(date_consumption))) %>%
  dplyr::filter(service_point_sn !="3101127564" & # exclude child-care
                (date(date_consumption) >= (today() %m-% months(2)))) 

PunggolConsumption_data <- PunggolConsumption_SUB %>%
  dplyr::filter(month(adjusted_date) %in% c(lastmonth,last2month) & year(adjusted_date) %in% c(lastyear,thisyear)) %>%  
  dplyr::mutate(wd=weekdays(date(adjusted_date)),hour=hour(adjusted_date)) %>%
  select(service_point_sn,adjusted_consumption,adjusted_date,wd,hour,day,month,year) 

MonthlyConsumption <- PunggolConsumption_data %>% 
                      group_by(service_point_sn,year,month) %>%
                      dplyr::summarise(MonthlyConsumption=sum(adjusted_consumption,na.rm = TRUE))

MonthlyConsumption_occupiedDays <- inner_join(MonthlyConsumption,monthlyoccupancy,by=c("service_point_sn"="service_point_sn",
                                                                                       "year"="Year","month"="Month"))

average_dailyconsumption_occupiedDays <- MonthlyConsumption_occupiedDays %>%
                                         dplyr::mutate(average_dailyconsumption=ifelse(occupancy_days==0,0,
                                                         round(MonthlyConsumption/occupancy_days))) %>%
                                         dplyr::select_("service_point_sn","average_dailyconsumption","month") %>%
                                         as.data.frame()
average_dailyconsumption_occupiedDays[1] <- NULL
monthly_report1 <- as.data.frame(spread(average_dailyconsumption_occupiedDays,month,average_dailyconsumption))
colnames(monthly_report1)[2] <- "last_2month_average"
colnames(monthly_report1)[3] <- "last_month_average"
monthly_report1$last_month <- rep(month.abb[lastmonth],nrow(monthly_report1))
monthly_report1$last_2month <- rep(month.abb[last2month],nrow(monthly_report1))
monthly_report1 <- monthly_report1 %>% dplyr::mutate(is_highusage_alarm=ifelse(last_month_average/last_2month_average>=1.5,"TRUE","FALSE"))

DailyConsumption <- PunggolConsumption_data %>% 
                    dplyr::mutate(Year=year(adjusted_date),Date=date(adjusted_date),wd=weekdays(date(adjusted_date))) %>%
                    group_by(service_point_sn,Date,Year,month,wd) %>%
                    dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) %>%
                    dplyr::mutate(NonZeroConsumption=ifelse(DailyConsumption!=0,1,0))

DailyConsumption_occupiedDays <- inner_join(DailyConsumption,dailyoccupancy,by=c("service_point_sn","Date"))

HourlyConsumption <- PunggolConsumption_data %>% 
                     dplyr::mutate(Date=date(adjusted_date),
                                   part_day=ifelse(hour %in% c(6:11),"morning",
                                            ifelse(hour %in% c(12:17),"afternoon",
                                            ifelse(hour %in% c(18:23),"evening",
                                            ifelse(hour %in% c(0:5),"night",0))))) %>%
                     dplyr::filter(year==thisyear & month==lastmonth) %>%
                     group_by(service_point_sn,Date,hour,part_day) 

days_week <- c("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday")
parts_day <- c("morning","afternoon","evening","night")

average_DaysWeek_consumption <- list()
for (i in 1:7){
  average_DaysWeek_consumption[[i]] <- DailyConsumption %>%
    group_by(service_point_sn,Date,Year,month,wd) %>%
    dplyr::filter(Year==thisyear & month==lastmonth & wd==days_week[i]) 
}

AverageDaysWeekConsumption <- as.data.frame(rbindlist(average_DaysWeek_consumption))
AverageWeekday_Consumption <- AverageDaysWeekConsumption %>%
  group_by(service_point_sn,wd) %>%
  dplyr::summarise(AverageWeekday_Consumption=round(sum(DailyConsumption)/sum(NonZeroConsumption))) %>%
  dplyr::mutate(AverageWeekday_Consumption=ifelse(is.na(AverageWeekday_Consumption),0,AverageWeekday_Consumption))

AverageWeekday_Consumption_wide <- spread(AverageWeekday_Consumption, wd, AverageWeekday_Consumption)

monthly_report2 <- as.data.frame(AverageWeekday_Consumption_wide) 
monthly_report2$weekend_average <- round(rowMeans(subset(monthly_report2,select=c(Saturday,Sunday)),na.rm=TRUE))
monthly_report2$weekday_average <- round(rowMeans(subset(monthly_report2,select=c(Monday,Tuesday,Wednesday,
                                                                                  Thursday,Friday)),na.rm=TRUE))
monthly_report2 <- monthly_report2 %>% dplyr::mutate(highest_week=ifelse(weekend_average>weekday_average,"Weekend","Weekday"))

# http://stackoverflow.com/questions/10290801/getting-column-name-which-holds-a-max-value-within-a-row-of-a-matrix-holding-a-s
mxCol=function(df, colIni, colFim){ #201609
  if(missing(colIni)) colIni=1
  if(missing(colFim)) colFim=ncol(df)
  if(colIni>=colFim) { print('colIni>=ColFim'); return(NULL)}
  dfm=cbind(mxCol=apply(df[colIni:colFim], 1, function(x) colnames(df)[which.max(x)+(colIni-1)])
            ,df)
  dfm=cbind(mxVal=as.numeric(apply(dfm,1,function(x) x[x[1]]))
            ,dfm)
  return(dfm)
}

monthly_report2=mxCol(monthly_report2,2)
monthly_report2[1] <- NULL
colnames(monthly_report2)[1] <-"highest_day"
setnames(monthly_report2, old = c("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"), 
                         new = c("mon_average","tue_average","wed_average","thu_average","fri_average","sat_average","sun_average"))

average_PartsDay_consumption <- list()
for (i in 1:4){
  average_PartsDay_consumption[[i]] <- HourlyConsumption %>%
                                       group_by(service_point_sn,part_day) %>%
                                       dplyr::filter(part_day==parts_day[i]) %>%
        dplyr::summarise(partsday_consumption=round(mean(adjusted_consumption,na.rm = TRUE)))
}
AveragePartDayConsumption <- as.data.frame(rbindlist(average_PartsDay_consumption))

monthly_report3 <- spread(AveragePartDayConsumption, part_day, partsday_consumption)
monthly_report3=mxCol(monthly_report3,2)
monthly_report3[1] <- NULL
colnames(monthly_report3)[1] <-"highest_period"

setnames(monthly_report3, old = c("morning","afternoon","evening","night"), 
         new = c("morning_average","afternoon_average","evening_average","night_average"))

min_row <- min(nrow(monthly_report1),nrow(monthly_report2),nrow(monthly_report3))
monthly_report <- cbind(monthly_report1[1:min_row,],monthly_report2[1:min_row,],
                        monthly_report3[1:min_row,]) %>%
                  subset(., select=which(!duplicated(names(.)))) %>%  # remove duplicated columns
                  dplyr::select_("service_point_sn","last_month_average","last_2month_average","last_month","last_2month","highest_day",
                                 "mon_average","tue_average","wed_average","thu_average","fri_average","sat_average","sun_average",
                                 "highest_period","morning_average","afternoon_average","evening_average","night_average","highest_week",
                                 "weekend_average","weekday_average","is_highusage_alarm")

monthly_report$morning_average <- monthly_report$morning_average*6
monthly_report$afternoon_average <- monthly_report$afternoon_average*6
monthly_report$evening_average <- monthly_report$evening_average*6
monthly_report$night_average <- monthly_report$night_average*6

number_customers <- length(monthly_report$service_point_sn) 

monthly_report$id <- as.integer(seq(max(monthly_report_DB$id)+1,
                                    max(monthly_report_DB$id)+1+number_customers-1,1))

monthly_report <- monthly_report[,c(ncol(monthly_report),c(1:ncol(monthly_report)-1))]

monthly_report$frame <- rep("NULL")
monthly_report$insert_date <- rep(Sys.Date())

## monthly appended table
dbWriteTable(mydb, "monthly_report", monthly_report, append=TRUE, row.names=F, overwrite=FALSE) # append table
dbDisconnect(mydb)

## above has issue on the insert_date which overwrite the previous records with the latest date.
# if(NROW(monthly_report)>0){
#   ### INSERT new challenges
#   sql_insert<- paste0("INSERT INTO monthly_report (
#                       id,service_point_sn,last_month_average,last_2month_average,last_month,last_2month,        
#                       highest_day,mon_average,tue_average,wed_average,thu_average,fri_average,        
#                       sat_average,sun_average,highest_period,morning_average,afternoon_average,evening_average,    
#                       night_average,highest_week,weekend_average,weekday_average,is_highusage_alarm,frame,insert_date     
#                       ) 
#                       VALUES (",apply(monthly_report,1,function(x){paste0(x,collapse = ',')}),");")
#   sapply(sql_insert, function(x){dbSendQuery(mydb, x)})
# }
# dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyReport successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)