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

#Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last30days.fst")
Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")

yesterday <- today()-1
thisYear <- year(today())
thismonth <- month(today())
lastYear <- thisYear-1

## below is for Monthly Consumption
PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792") %>%  # remove AHL
  dplyr::mutate(day=day(adjusted_date),month=month(adjusted_date)) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
  dplyr::select_("service_point_sn","adjusted_consumption","adjusted_date","num_house_member","room_type","block")

DailyConsumption <- PunggolConsumption_SUB %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

Monthly_Consumption <- DailyConsumption %>% group_by(service_point_sn,Year,Month,block) %>%
  dplyr::summarise(ConsumptionPerMonth=sum(DailyConsumption,na.rm = TRUE))

MonthlyConsumption_New <- Monthly_Consumption %>%
                          group_by(Year,Month) %>%
                          dplyr::summarise(MonthlyConsumption=sum(ConsumptionPerMonth))

MonthlyConsumption_New <- MonthlyConsumption_New[-1,] # remove first row of previous month
MonthlyConsumption_New$Month <- factor(month.abb[MonthlyConsumption_New$Month],levels = month.abb)
MonthlyConsumption_New$Year <- as.character(MonthlyConsumption_New$Year)
MonthlyConsumption_New <- as.data.frame(MonthlyConsumption_New)

MonthlyConsumption <- unique(rbind(as.data.frame(MonthlyConsumption),MonthlyConsumption_New)) %>% 
                      dplyr::group_by(Year,Month) %>% 
                      dplyr::filter(MonthlyConsumption==max(MonthlyConsumption))

MonthlyConsumption_wide <- spread(MonthlyConsumption, Year, MonthlyConsumption)
colnames(MonthlyConsumption_wide)[2] <- paste("MonthlyConsumption_",thisYear-2,sep="")
colnames(MonthlyConsumption_wide)[3] <- paste("MonthlyConsumption_",thisYear-1,sep="")
colnames(MonthlyConsumption_wide)[4] <- paste("MonthlyConsumption_",thisYear,sep="")

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

familymembers <- family_ACTIVEINACTIVE %>% dplyr::filter(substr(address,1,2)=="PG") %>%
                 dplyr::select_("id_service_point","move_in_date","move_out_date","num_house_member") 

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
days_PerMonth <- ts(diff(seq(as.Date(paste(thisYear-2,"-01-01",sep="")), as.Date(paste(thisYear+1,"-01-01",sep="")), by = "month")),
                    start = c(thisYear-2, 01), freq = 12) %>%
                 as.data.frame()

daysPerMonth <- data.frame(Year = c(rep(thisYear-2,12),rep(thisYear-1,12),rep(thisYear,12)),
                           month = c(rep(c(1:12),3)),
                           Days = days_PerMonth$x)
daysPerMonth$month <- as.numeric(daysPerMonth$month)
daysPerMonth$Days <- as.numeric(daysPerMonth$Days)

CountsPerDay <- length(unique(PunggolConsumption_SUB$service_point_sn))*24
CountsThisMonth <- PunggolConsumption_SUB %>% dplyr::mutate(Year=year(adjusted_date),Month=month(adjusted_date)) %>%
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

LPCD_PerMonth[1,3] <- 142

save(LPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/LPCD_PerMonth.RData")
write.csv(LPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/LPCD_PerMonth.csv",row.names = FALSE)

## to calculate weekly_LPCD, updated on every Monday
load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/Weekly_LPCD.RData")
weeknumber <- as.numeric(strftime(today(),format="%W")) 
i <- weeknumber-1
if (i <10) {i <- paste(0,i,sep="")}
WeekNumber=paste(year(today()),"_",i,sep="") 

week.start <- Week.date$beg[which(Week.date$week==WeekNumber)]
week.end <- Week.date$end[which(Week.date$week==WeekNumber)]

Weekly_Consumption  <- PunggolConsumption_SUB %>% dplyr::group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date) >= week.start & date(adjusted_date) <= week.end) %>%
  dplyr::summarise(WeeklyConsumption=sum(adjusted_consumption,na.rm = TRUE))

WeeklyConsumption <- cbind(Date=as.data.frame(week.end),Weekly_Consumption)

PunggolConsumption_HHSize <- PunggolConsumption %>% dplyr::select_("service_point_sn","num_house_member") %>% unique()

WeeklyConsumption_NumHouseMember <- inner_join(WeeklyConsumption,PunggolConsumption_HHSize,by="service_point_sn") %>%
  dplyr::select_("week.end","service_point_sn","num_house_member","WeeklyConsumption")

Weekly_LPCD_new <- WeeklyConsumption_NumHouseMember %>% 
  dplyr::group_by(week.end) %>% 
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(WeeklyLPCD=round(WeeklyConsumption/(TotalHH*7),2)) %>% as.data.frame()

Weekly_LPCD_new_xts <- xts(Weekly_LPCD_new$WeeklyLPCD,order.by=as.Date(Weekly_LPCD_new$week.end))
colnames(Weekly_LPCD_new_xts) <- "WeeklyLPCD"

Weekly_LPCD <- rbind(as.data.frame(Weekly_LPCD),Weekly_LPCD_new)
Weekly_LPCD_xts <- rbind(Weekly_LPCD_xts,Weekly_LPCD_new_xts)

Updated_DateTime_WeeklyLPCD <- paste("Last Updated on ",now(),"."," Next Update on ",now()+7*24*60*60,".",sep="")

save(Weekly_LPCD,Weekly_LPCD_xts,Updated_DateTime_WeeklyLPCD,file="/srv/shiny-server/DataAnalyticsPortal/data/Weekly_LPCD.RData")
write.csv(Weekly_LPCD[,c(1,4)],file="/srv/shiny-server/DataAnalyticsPortal/data/Weekly_LPCD.csv",row.names = FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyConsumptionLPCD_HH_WeeklyLPCD successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
