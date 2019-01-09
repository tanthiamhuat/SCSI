## Accumulative, Appended on Weekly basis for MonthlyConsumption,TotalHH_wide,LPCD_PerMonth and WeeklyLPCD

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,tidyr,RPostgreSQL,data.table,xts,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/MonthlyConsumption.RData")

family_ACTIVE <- as.data.frame(tbl(con,"family") %>% 
                 dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                               & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family_ACTIVE,servicepoint,by=c("id_service_point"="id","room_type")) 

Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last30days.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

yesterday <- today()-1
thisYear <- year(today())
thismonth <- month(today())
lastYear <- thisYear-1

## below is for Monthly Consumption
PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792") %>%  # remove AHL
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
  group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date)>=date(move_in_date) & (date(adjusted_date)<date(move_out_date) | is.na(move_out_date)))

PunggolConsumption <- unique(PunggolConsumption[c("service_point_sn","adjusted_consumption","adjusted_date","num_house_member","room_type","block")])

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

Monthly_Consumption <- DailyConsumption %>% group_by(service_point_sn,Year,Month,num_house_member,block) %>%
  dplyr::summarise(ConsumptionPerMonth=sum(DailyConsumption,na.rm = TRUE))

MonthlyConsumption_New <- Monthly_Consumption %>%
                          group_by(Year,Month) %>%
                          dplyr::summarise(MonthlyConsumption=sum(ConsumptionPerMonth))

MonthlyConsumption_New <- MonthlyConsumption_New[-1,] # remove first row of previous month
MonthlyConsumption_New$Month <- factor(month.abb[MonthlyConsumption_New$Month],levels = month.abb)
MonthlyConsumption_New$Year <- as.character(MonthlyConsumption_New$Year)
MonthlyConsumption_New <- as.data.frame(MonthlyConsumption_New)

MonthlyConsumption <- rbind(MonthlyConsumption,MonthlyConsumption_New)
MonthlyConsumption <- MonthlyConsumption[-c(nrow(MonthlyConsumption)-1),]

MonthlyConsumption_wide <- spread(MonthlyConsumption, Year, MonthlyConsumption)
colnames(MonthlyConsumption_wide)[2] <- paste("MonthlyConsumption_",lastYear,sep="")
colnames(MonthlyConsumption_wide)[3] <- paste("MonthlyConsumption_",thisYear,sep="")

MonthlyConsumption_wide$Month <- factor(month.abb[MonthlyConsumption_wide$Month],
                                        levels = month.abb)
save(MonthlyConsumption,
     file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyConsumption.RData")
write.csv(MonthlyConsumption_wide,file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyConsumption.csv",row.names = FALSE)

## to find number of household members at each month
monthStart <- function(x) {
  x <- as.POSIXlt(x)
  x$mday <- 1
  as.Date(x)
}
family_ACTIVEINACTIVE <- as.data.frame(tbl(con,"family") %>% 
                                         dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))

## retrieve those family which has more than 1 move_in_date
family_ACTIVEINACTIVE_Morethan1MoveInDate <- family_ACTIVEINACTIVE %>% dplyr::group_by(id_service_point) %>%
                                             dplyr::mutate(count=n()) %>%
                                             dplyr::filter(count>1)

family_ACTIVEINACTIVE_BeforeGamification <- family_ACTIVEINACTIVE %>% dplyr::group_by(id_service_point) %>%
  dplyr::filter(move_out_date < "2017-06-10")

familymembers <- family_ACTIVEINACTIVE %>% dplyr::select_("id_service_point","move_in_date","move_out_date","num_house_member") 

YearMonth <- seq(as.Date("2016-03-01"),as.Date(monthStart(today())),"month")
YearMonth_DF <- as.data.frame(YearMonth) %>% dplyr::mutate(Year=year(YearMonth),Month=month(YearMonth)) %>% dplyr::select_("Year","Month")

familymembers_YearMonth <- list()
for (i in 1:length(YearMonth))
{
  familymembers_YearMonth[[i]] <- familymembers %>% 
                                  dplyr::filter(move_in_date <= YearMonth[i] & (move_out_date > YearMonth[i] | is.na(move_out_date))) %>%
                                  dplyr::summarise(TotalHH=sum(num_house_member))
}
familymembers_YearMonth_DF <- as.data.frame(unlist(familymembers_YearMonth)) 
colnames(familymembers_YearMonth_DF) <- "TotalHH"
familymembers_YearMonth_DF$TotalHH <- round(familymembers_YearMonth_DF$TotalHH)

TotalHH <- cbind(YearMonth_DF,familymembers_YearMonth_DF)
TotalHH$Year <- as.character(TotalHH$Year)
TotalHH$Month <- factor(month.abb[TotalHH$Month],levels = month.abb)
TotalHH_wide <- spread(TotalHH,Month,TotalHH)

save(TotalHH_wide,file="/srv/shiny-server/DataAnalyticsPortal/data/TotalHH.RData")

## below is Average LPCD per month
days_PerMonth <- ts(diff(seq(as.Date(paste(lastYear,"-01-01",sep="")), as.Date(paste(thisYear+1,"-01-01",sep="")), by = "month")),
                    start = c(lastYear, 01), freq = 12) %>%
                 as.data.frame()

daysPerMonth <- data.frame(Year = c(rep(lastYear,12),rep(thisYear,12)),
                           month = c(rep(c(1:12),2)),
                           Days = days_PerMonth$x)
daysPerMonth$month <- as.numeric(daysPerMonth$month)
daysPerMonth$Days <- as.numeric(daysPerMonth$Days)

CountsPerDay <- length(unique(PunggolConsumption$service_point_sn))*24
CountsThisMonth <- PunggolConsumption %>% dplyr::mutate(Year=year(adjusted_date),Month=month(adjusted_date)) %>%
                   dplyr::filter(Year==thisYear & Month==thismonth) %>%
                   dplyr::summarise(TotalCount=n())

daysPast <- sum(CountsThisMonth$TotalCount)/CountsPerDay

daysPerMonth[which(daysPerMonth$Year==thisYear & daysPerMonth$month==thismonth),3] <- daysPast
daysPerMonth$month <- month.abb[daysPerMonth$month]

MonthConsumptionTotalHH <- inner_join(MonthlyConsumption,TotalHH,by=c("Year","Month"))
MonthConsumptionTotalHH$Month <- as.character(MonthConsumptionTotalHH$Month)
MonthConsumptionTotalHH$Year <- as.numeric(MonthConsumptionTotalHH$Year)
  
LPCD_PerMonth <- inner_join(MonthConsumptionTotalHH,daysPerMonth,by=c("Year","Month"="month")) %>%
                 dplyr::mutate(AverageLPCD=round(MonthlyConsumption/(TotalHH*Days))) %>%
                 dplyr::select_("Year","Month","AverageLPCD")

LPCD_PerMonth$Month <- match(LPCD_PerMonth$Month,month.abb)  # convert to numeric
LPCD_PerMonth$Month <- factor(month.abb[LPCD_PerMonth$Month],levels = month.abb)

LPCD_PerMonth$Year <- as.character(LPCD_PerMonth$Year)
LPCD_PerMonth <- as.data.frame(LPCD_PerMonth)

save(LPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/LPCD_PerMonth.RData")
write.csv(LPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/LPCD_PerMonth.csv",row.names = FALSE)

load("/srv/shiny-server/DataAnalyticsPortal/data/WeeklyLPCD.RData")

## to calculate weekly_LPCD, updated on every Monday, so DailyConsumption need to exclude Monday.
DailyConsumption <- DailyConsumption %>% dplyr::filter(date <= yesterday)

weeknumber <- as.numeric(strftime(DailyConsumption$date,format="%W")) %>% as.data.frame()
colnames(weeknumber) <- "week_number"
weeknumber <- weeknumber %>% dplyr::mutate(week_number=ifelse(week_number<10,paste(0,weeknumber$week_number,sep=""),week_number))
index00 <- which(weeknumber$week_number=="00")
weeknumber <- weeknumber %>% dplyr::mutate(week_number=replace(week_number, which(week_number=="00"),"52"))

yearnumber <- DailyConsumption$Year %>% as.data.frame()
colnames(yearnumber) <- "yearnumber"
yearnumber[c(index00),] <- lastYear
yearweek <- cbind(yearnumber,weeknumber) %>% dplyr::mutate(yearweek=paste(yearnumber,week_number,sep="_")) %>% dplyr::select_("yearweek")

DailyConsumption_yearweek <- cbind(as.data.frame(DailyConsumption),yearweek) 
WeeklyConsumption <- DailyConsumption_yearweek %>%
                     dplyr::group_by(service_point_sn,num_house_member,yearweek) %>%
                     dplyr::summarise(WeeklyConsumption=sum(DailyConsumption,na.rm=TRUE))

WeeklyLPCD_New <- WeeklyConsumption %>% 
  dplyr::mutate(Year=substr(yearweek,1,4),Week=substr(yearweek,6,8)) %>%
  dplyr::group_by(Year,Week,yearweek) %>%
  dplyr::summarise(TotalWeeklyConsumption=sum(WeeklyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(WeeklyLPCD=round(TotalWeeklyConsumption/(TotalHH*7),2)) %>% 
  dplyr::select_("Year","Week","WeeklyLPCD") %>% as.data.frame()
colnames(WeeklyLPCD_New) <- c("Year","Week",paste("WeeklyLPCD_",thisYear,sep=""))
WeeklyLPCD_New <- WeeklyLPCD_New[nrow(WeeklyLPCD_New),c(2,3)]

WeeklyLPCD_lastYear <- WeeklyLPCD[,c(1,2)]
WeeklyLPCD_thisYear <- WeeklyLPCD[,c(1,3)]

WeeklyLPCD_thisYear[WeeklyLPCD_New$Week,ncol(WeeklyLPCD_thisYear)] <- WeeklyLPCD_New[1,2]

WeeklyLPCD=full_join(WeeklyLPCD_lastYear,WeeklyLPCD_thisYear,by="Week") 
WeeklyLPCD["Year.x"] <- NULL
WeeklyLPCD["Year.y"] <- NULL
WeeklyLPCD <- as.data.frame(WeeklyLPCD) %>% dplyr::arrange_("Week")
colnames(WeeklyLPCD) <- c("Week",paste("WeeklyLPCD_",lastYear,sep=""),paste("WeeklyLPCD_",thisYear,sep=""))

save(WeeklyLPCD,file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyLPCD.RData")
write.csv(WeeklyLPCD,file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyLPCD.csv",row.names = FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyConsumptionLPCD_HH_WeeklyLPCD successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)