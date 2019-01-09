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

family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

today <- today()
thisYear <- year(today)
lastYear <- thisYear-1
thismonth <- month(today)
lastmonth <- month(today)-1

date_lastmonth <- today()-30
date_lastmonth_begin <- date_lastmonth - mday(date_lastmonth) + 1

## below is for the Monthly LPCD
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

## below is for Monthly Consumption
MonthlyConsumption <- Monthly_Consumption %>%
                      group_by(Year,Month) %>%
                      dplyr::summarise(MonthlyConsumption=sum(ConsumptionPerMonth))

MonthlyConsumption <- MonthlyConsumption[-1,] # remove first row
MonthlyConsumption$Month <- factor(month.abb[MonthlyConsumption$Month],levels = month.abb)
MonthlyConsumption$Year <- as.character(MonthlyConsumption$Year)
MonthlyConsumption <- as.data.frame(MonthlyConsumption)

MonthlyConsumption_wide <- spread(MonthlyConsumption, Year, MonthlyConsumption)
colnames(MonthlyConsumption_wide)[2] <-"MonthlyConsumption_2016"
colnames(MonthlyConsumption_wide)[3] <-"MonthlyConsumption_2017"

MonthlyConsumption_wide$Month <- factor(month.abb[MonthlyConsumption_wide$Month],
                                        levels = month.abb)

save(MonthlyConsumption,MonthlyConsumption_wide,
     file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyConsumption.RData")
write.csv(MonthlyConsumption_wide,file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyConsumption.csv",row.names = FALSE)

## below is Average LPCD per month
days_PerMonth <- ts(diff(seq(as.Date("2016-01-01"), as.Date("2019-01-01"), by = "month")), start = c(2016, 01), freq = 12) %>% 
                 as.data.frame()

daysPerMonth <- data.frame(Year = c(rep(2016,12),rep(2017,12),rep(2018,12)),
                           month = c(rep(c(1:12),3)),
                           Days = days_PerMonth$x)
daysPerMonth$month <- as.numeric(daysPerMonth$month)
daysPerMonth$Days <- as.integer(daysPerMonth$Days)

daysPast <- as.integer(day(today-1))

daysPerMonth[which(daysPerMonth$Year==thisYear & daysPerMonth$month==thismonth),3] <- daysPast

## to find number of household members at each month
family_ACTIVEINACTIVE <- as.data.frame(tbl(con,"family") %>% 
                                         dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))
familymembers <- family_ACTIVEINACTIVE %>% dplyr::select_("id_service_point","move_in_date","move_out_date","num_house_member") 

YearMonth <- seq(as.Date("2016-03-01"),as.Date(date_lastmonth_begin),"month")
YearMonth_DF <- as.data.frame(YearMonth) %>% dplyr::mutate(Year=year(YearMonth),Month=month(YearMonth)) %>% dplyr::select_("Year","Month")

familymembers_YearMonth <- list()
for (i in 1:length(YearMonth))
{
  familymembers_YearMonth[[i]] <- familymembers %>% dplyr::filter(move_in_date <= YearMonth[i] & (move_out_date > YearMonth[i] | is.na(move_out_date))) %>%
    dplyr::summarise(TotalHH=sum(num_house_member))
}
familymembers_YearMonth_DF <- as.data.frame(unlist(familymembers_YearMonth)) 
colnames(familymembers_YearMonth_DF) <- "TotalHH"
familymembers_YearMonth_DF$TotalHH <- round(familymembers_YearMonth_DF$TotalHH)

TotalHH <- cbind(YearMonth_DF,familymembers_YearMonth_DF)
TotalHH$Month <- factor(month.abb[TotalHH$Month],levels = month.abb)
TotalHH_wide <- spread(TotalHH,Month,TotalHH)

LPCD_PerMonth <- inner_join(Monthly_Consumption,daysPerMonth,by=c("Year","Month"="month")) %>%
  group_by(Year,Month) %>%
  dplyr::mutate(LPCD=ifelse(ConsumptionPerMonth==0,0,
                     ifelse(ConsumptionPerMonth!=0,
                            round(ConsumptionPerMonth/(num_house_member*Days)),0)),
                LPCD_Weighted=LPCD*num_house_member) %>%
  group_by(Year,Month) %>%
  dplyr::summarise(AverageLPCD=round(sum(LPCD_Weighted,na.rm = TRUE)/sum(num_house_member)))

LPCD_PerMonth <- LPCD_PerMonth[-1,] # remove first row

## to take care of the last row LPCD_PerMonth (which is not complete)
LPCD_PerMonth_lastrow <- inner_join(LPCD_PerMonth,daysPerMonth,by=c("Year","Month"="month"))

LPCD_PerMonth_lastrow[nrow(LPCD_PerMonth_lastrow),3] <- round(LPCD_PerMonth_lastrow[nrow(LPCD_PerMonth_lastrow),3]*LPCD_PerMonth_lastrow[nrow(LPCD_PerMonth_lastrow),4]/(daysPast+1))

LPCD_PerMonth <- LPCD_PerMonth_lastrow

LPCD_PerMonth$Month <- factor(month.abb[LPCD_PerMonth$Month],levels = month.abb)
LPCD_PerMonth$Year <- as.character(LPCD_PerMonth$Year)
LPCD_PerMonth <- as.data.frame(LPCD_PerMonth)

save(LPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/LPCD_PerMonth.RData")
write.csv(LPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/LPCD_PerMonth.csv",row.names = FALSE)



DailyLPCD <- DailyConsumption %>% dplyr::filter(date>="2016-03-14" & date!=today) %>%
             dplyr::group_by(date) %>%
             dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
             dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH)
DailyLPCD_CSV <- DailyLPCD %>% dplyr::select_("date","DailyLPCD")

DailyLPCD_xts <- xts(DailyLPCD$DailyLPCD,order.by=as.Date(DailyLPCD$date))
colnames(DailyLPCD_xts) <- "DailyLPCD"

DailyLPCD_Block <- DailyConsumption %>% dplyr::filter(date>="2016-03-14" & date!=today) %>%
  dplyr::group_by(date,block) %>%
  dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH)

familymembers_Block <- family_ACTIVEINACTIVE %>% dplyr::mutate(block=substr(address,1,5)) %>%  # be careful (for production it is block=substr(address,1,4))
  dplyr::group_by(block) %>%
  dplyr::summarise(TotalBlockMember=sum(num_house_member))

save(TotalHH,TotalHH_wide,DailyLPCD,DailyLPCD_xts,DailyLPCD_Block,familymembers_Block,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.RData")
write.csv(DailyLPCD_CSV,file="/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.csv",row.names = FALSE)

## to calculate weekly_LPCD
weekly <- apply.weekly(DailyLPCD_xts,sum)
WeeklyLPCD_xts <- weekly/7
colnames(WeeklyLPCD_xts) <- "WeeklyLPCD"
WeeklyLPCD_xts <- WeeklyLPCD_xts[1:(nrow(WeeklyLPCD_xts)-1),]

weeknumber <- as.numeric(strftime(DailyConsumption$date,format="%W")) %>% as.data.frame()
colnames(weeknumber) <- "week_number"
weeknumber <- weeknumber %>% dplyr::mutate(week_number=ifelse(week_number<10,paste(0,weeknumber$week_number,sep=""),week_number))
index00 <- which(weeknumber$week_number=="00")
weeknumber <- weeknumber %>% dplyr::mutate(week_number=replace(week_number, which(week_number=="00"),"52"))

yearnumber <- DailyConsumption$Year %>% as.data.frame()
colnames(yearnumber) <- "yearnumber"
yearnumber[c(index00),] <- lastYear
yearweek <- cbind(yearnumber,weeknumber) %>% dplyr::mutate(yearweek=paste(yearnumber,week_number,sep="_")) %>% dplyr::select_("yearweek")

DailyConsumption_yearweek <- cbind(as.data.frame(DailyConsumption),yearweek) %>% dplyr::filter(date>="2016-03-14")
WeeklyConsumption <- DailyConsumption_yearweek %>% 
                     dplyr::group_by(service_point_sn,num_house_member,yearweek) %>%
                     dplyr::summarise(WeeklyConsumption=sum(DailyConsumption,na.rm=TRUE))

WeeklyLPCD <- WeeklyConsumption %>% 
  dplyr::mutate(Year=substr(yearweek,1,4),Week=substr(yearweek,6,8)) %>%
  dplyr::group_by(Year,Week,yearweek) %>%
  dplyr::summarise(TotalWeeklyConsumption=sum(WeeklyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(WeeklyLPCD=round(TotalWeeklyConsumption/(TotalHH*7),2)) %>% 
  dplyr::select_("Year","Week","WeeklyLPCD")
WeeklyLPCD <- WeeklyLPCD[1:(nrow(WeeklyLPCD)-1),]

WeeklyLPCD_lastyear <- WeeklyLPCD %>% dplyr::filter(Year==lastYear) %>% dplyr::rename(WeeklyLPCD2016=WeeklyLPCD)
WeeklyLPCD_thisyear <- WeeklyLPCD %>% dplyr::filter(Year==thisYear) %>% dplyr::rename(WeeklyLPCD2017=WeeklyLPCD)

WeeklyLPCD=full_join(WeeklyLPCD_lastyear,WeeklyLPCD_thisyear,by="Week") 
WeeklyLPCD["Year.x"] <- NULL
WeeklyLPCD["Year.y"] <- NULL
WeeklyLPCD <- as.data.frame(WeeklyLPCD) %>% dplyr::arrange_("Week")

save(WeeklyLPCD,file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyLPCD.RData")
write.csv(WeeklyLPCD,file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyLPCD.csv",row.names = FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyConsumption_LPCD successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)