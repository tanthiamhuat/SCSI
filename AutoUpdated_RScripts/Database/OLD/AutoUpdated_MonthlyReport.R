rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,tidyr,lubridate,RPostgreSQL,data.table)

# lastmonth <- month(today())
# if (lastmonth==1) {
#   last2month=12
# } else {
#   last2month=lastmonth-1
# }

today <- today()-months(1)
lastmonth <- month(today)
if (lastmonth==1) {
  last2month=12
} else {
  last2month=lastmonth-1
}


thisyear <- year(today())
today <- today()

# Establish connection
con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")
mydb <- dbConnect(PostgreSQL(), dbname="amrstaging",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")

monthly_report_DB <- as.data.frame(tbl(con,"monthly_report"))

load("/srv/shiny-server/DataAnalyticsPortal/data/PunggolConsumption.RData")
rm(PunggolConsumption_MAINSUB,PunggolConsumption_MAIN)

servicepoint <- as.data.frame(tbl(con,"service_point"))
monthlyoccupancy <- as.data.frame(tbl(con,"monthly_occupancy")) %>% 
                    dplyr::mutate(Month=month(lastupdated)) %>%
                    dplyr::filter(service_point_sn !="3101127564" & Month %in% c(lastmonth,last2month)) # exclude child-care

dailyoccupancy <- as.data.frame(tbl(con,"daily_occupancy")) %>%
                  dplyr::mutate(Date=date(date_consumption),Month=month(date(date_consumption)),wd=weekdays(date(date_consumption))) %>%
                  dplyr::filter(service_point_sn !="3101127564" & Month %in% c(lastmonth,last2month)) # exclude child-care

PunggolConsumption_servicepoint <- inner_join(PunggolConsumption_SUB,servicepoint,
                                              by=c("service_point_sn","block","room_type","floor"))

PunggolConsumption_data <- PunggolConsumption_servicepoint %>%
  dplyr::filter(site %in% c("Punggol","Whampoa") & !(room_type %in% c("NIL","HDBCD")) & !(is.na(room_type)) &
                # month(date_consumption) %in% c(lastmonth,last2month) & (year(date_consumption)==thisyear  
                ## above for same year, that is lastmonth=2 onwards
                ## below is for lastmonth=1 (Jan) and last2month=12 (Dec)
                ((year(date_consumption)==thisyear & month(date_consumption)==lastmonth) | 
                 (year(date_consumption)==thisyear-1 & month(date_consumption)==last2month))) %>%
  dplyr::mutate(wd=weekdays(date(date_consumption)),hour=hour(date_consumption)) %>%
  select(service_point_sn,adjusted_consumption,date_consumption,wd,hour) 

MonthlyConsumption <- PunggolConsumption_data %>% 
                      dplyr::mutate(Month=month(date_consumption)) %>%
                      group_by(service_point_sn,Month) %>%
                      dplyr::summarise(MonthlyConsumption=sum(adjusted_consumption,na.rm = TRUE))

MonthlyConsumption_occupiedDays <- inner_join(MonthlyConsumption,monthlyoccupancy,by=c("service_point_sn","Month"))

average_dailyconsumption_occupiedDays <- MonthlyConsumption_occupiedDays %>%
                                         dplyr::mutate(average_dailyconsumption=round(MonthlyConsumption/occupancy_days)) %>%
                                         dplyr::select_("service_point_sn","Month","average_dailyconsumption")
monthly_report1 <- as.data.frame(spread(average_dailyconsumption_occupiedDays,Month,average_dailyconsumption))
colnames(monthly_report1)[2] <- "last_month_average"
colnames(monthly_report1)[3] <- "last_2month_average"
monthly_report1$last_month <- rep(month.abb[lastmonth],nrow(monthly_report1))
monthly_report1$last_2month <- rep(month.abb[last2month],nrow(monthly_report1))
monthly_report1 <- monthly_report1 %>% dplyr::mutate(is_highusage_alarm=ifelse(last_month_average/last_2month_average>=1.5,"TRUE","FALSE"))

DailyConsumption <- PunggolConsumption_data %>% 
                    dplyr::mutate(Date=date(date_consumption)) %>%
                    group_by(service_point_sn,Date) %>%
                    dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE))

DailyConsumption_occupiedDays <- inner_join(DailyConsumption,dailyoccupancy,by=c("service_point_sn","Date"))

HourlyConsumption <- PunggolConsumption_data %>% 
                     dplyr::mutate(Date=date(date_consumption),Month=month(date_consumption),
                                   part_day=ifelse(hour %in% c(6:11),"morning",
                                            ifelse(hour %in% c(12:17),"afternoon",
                                            ifelse(hour %in% c(18:23),"evening",
                                            ifelse(hour %in% c(0:5),"night",0))))) %>%
                     dplyr::filter(Month==lastmonth) %>%
                     group_by(service_point_sn,Date,hour,part_day) 

days_week <- c("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday")
parts_day <- c("morning","afternoon","evening","night")

average_DaysWeek_consumption <- list()
for (i in 1:7){
  average_DaysWeek_consumption[[i]] <- DailyConsumption_occupiedDays %>%
    dplyr::mutate(Date=date(date_consumption)) %>%
    group_by(service_point_sn,Date,Month,wd) %>%
    dplyr::filter(wd==days_week[i]) %>%
    dplyr::filter(Month %in% c(lastmonth)) %>%
    dplyr::summarise(daysweek_consumption=round(DailyConsumption*occupancy_rate/100))
  #tmp <- paste(c("mon","tue","wed","thu","fri","sat","sun"),"average",sep = "_")[i]
  #assign(tmp, average_DaysWeek_consumption[[i]])
}

AverageDaysWeekConsumption <- as.data.frame(rbindlist(average_DaysWeek_consumption))
AverageWeekday_Consumption <- AverageDaysWeekConsumption %>%
                              group_by(service_point_sn,wd) %>%
                              dplyr::summarise(AverageWeekday_Consumption=round(mean(daysweek_consumption)))

AverageWeekday_Consumption_wide <- spread(AverageWeekday_Consumption, wd, AverageWeekday_Consumption)

monthly_report2 <- as.data.frame(AverageWeekday_Consumption_wide) 
monthly_report2$weekend_average <- round(rowMeans(subset(monthly_report2,select=c(Saturday,Sunday)),na.rm=TRUE))
monthly_report2$weekday_average <- round(rowMeans(subset(monthly_report2,select=c(Monday,Tuesday,Wednesday,
                                                                                  Thursday,Friday)),na.rm=TRUE))
monthly_report2 <- monthly_report2 %>% dplyr::mutate(highest_week=ifelse(weekend_average>weekday_average,"Weekend","Weeekday"))

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

monthly_report <- cbind(monthly_report1,monthly_report2,monthly_report3) %>%
                  subset(., select=which(!duplicated(names(.)))) %>%  # remove duplicated columns
                  dplyr::select_("service_point_sn","last_month_average","last_2month_average","last_month","last_2month","highest_day",
                                 "mon_average","tue_average","wed_average","thu_average","fri_average","sat_average","sun_average",
                                 "highest_period","morning_average","afternoon_average","evening_average","night_average","highest_week",
                                 "weekend_average","weekday_average","is_highusage_alarm")

number_customers <- length(monthly_report$service_point_sn) 

monthly_report$id <- as.integer(seq(nrow(monthly_report_DB)+1,
                                       nrow(monthly_report_DB)+number_customers,1))

monthly_report <- monthly_report[,c(ncol(monthly_report),c(1:ncol(monthly_report)-1))]

## monthly appended table
dbWriteTable(mydb, "monthly_report", monthly_report, append=TRUE, row.names=F, overwrite=FALSE) # append table
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyReport successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)