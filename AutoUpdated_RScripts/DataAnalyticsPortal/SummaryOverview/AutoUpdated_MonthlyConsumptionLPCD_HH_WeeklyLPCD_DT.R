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

# load("/srv/shiny-server/DataAnalyticsPortal/data/DT/PunggolMonthlyConsumption.RData")
# load("/srv/shiny-server/DataAnalyticsPortal/data/DT/YuhuaMonthlyConsumption.RData")

load("/srv/shiny-server/DataAnalyticsPortal/data/DT/MonthlyConsumption.RData")

thisYear <- year(today())
thisMonth <- month(today())

consumption_last90days_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last90days_servicepoint.fst",as.data.table = TRUE)

Consumption_thisMonth <- function(area){
  consumption_thisMonth <- consumption_last90days_servicepoint[!(room_type %in% c("NIL","HDBCD","OTHER")) & service_point_sn !="3100660792"] %>%
    dplyr::filter(!is.na(adjusted_consumption) & site==area) %>%
    dplyr::mutate(Year=year(date_consumption),Month=month(date_consumption)) %>%
    group_by(Year,Month,site) %>%
    dplyr::summarise(ConsumptionPerMonth=sum(adjusted_consumption,na.rm = TRUE)) %>% as.data.frame()
  consumption_thisMonth$Month <- factor(month.abb[consumption_thisMonth$Month],levels = month.abb)
  return (consumption_thisMonth)
}

PunggolMonthlyConsumption <- as.data.frame(PunggolMonthlyConsumption)
PunggolConsumption_thisMonth <- Consumption_thisMonth("Punggol") %>% tail(1)
PunggolMonthlyConsumption <- as.data.frame(rbind(PunggolMonthlyConsumption[1:(nrow(PunggolMonthlyConsumption)-1),],PunggolConsumption_thisMonth[,c(1,2,4)]))
PunggolMonthlyConsumption$Year <- as.character(PunggolMonthlyConsumption$Year)

YuhuaConsumption_thisMonth <- Consumption_thisMonth("Yuhua") %>% tail(1)
YuhuaMonthlyConsumption <- as.data.frame(rbind(YuhuaMonthlyConsumption[1:(nrow(YuhuaMonthlyConsumption)-1),],YuhuaConsumption_thisMonth[,c(1,2,4)]))
YuhuaMonthlyConsumption$Year <- as.character(YuhuaMonthlyConsumption$Year)

PunggolMonthlyConsumption_wide <- spread(PunggolMonthlyConsumption,Year,ConsumptionPerMonth)
colnames(PunggolMonthlyConsumption_wide)[2] <- paste("MonthlyConsumption_",thisYear-2,sep="")
colnames(PunggolMonthlyConsumption_wide)[3] <- paste("MonthlyConsumption_",thisYear-1,sep="")
colnames(PunggolMonthlyConsumption_wide)[4] <- paste("MonthlyConsumption_",thisYear,sep="")

PunggolMonthlyConsumption_wide$Month <- factor(month.abb[PunggolMonthlyConsumption_wide$Month],
                                               levels = month.abb)

YuhuaMonthlyConsumption_wide <- spread(YuhuaMonthlyConsumption,Year,ConsumptionPerMonth)
colnames(YuhuaMonthlyConsumption_wide)[2] <- paste("MonthlyConsumption_",thisYear,sep="")

YuhuaMonthlyConsumption_wide$Month <- factor(month.abb[YuhuaMonthlyConsumption_wide$Month],
                                               levels = month.abb)

write.csv(PunggolMonthlyConsumption_wide,file="/srv/shiny-server/DataAnalyticsPortal/data/DT/PunggolMonthlyConsumption.csv",row.names = FALSE)
write.csv(YuhuaMonthlyConsumption_wide,file="/srv/shiny-server/DataAnalyticsPortal/data/DT/YuhuaMonthlyConsumption.csv",row.names = FALSE)

Updated_DateTime_MonthlyConsumption <- paste("Last Updated on ",now(),"."," Next Update on ",now()+7*24*60*60,".",sep="")

save(PunggolMonthlyConsumption,YuhuaMonthlyConsumption,Updated_DateTime_MonthlyConsumption,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DT/MonthlyConsumption.RData")

## to find number of household members at each month
monthStart <- function(x) {
  x <- as.POSIXlt(x)
  x$mday <- 1
  as.Date(x)
}
family_ACTIVEINACTIVE <- as.data.frame(tbl(con,"family")) %>% 
                         dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD","OTHER")) & id_service_point !='601')

familymembers_PG <- family_ACTIVEINACTIVE %>% dplyr::filter(substr(address,1,2)=="PG") %>%
                    dplyr::select_("id_service_point","move_in_date","move_out_date","num_house_member") 

familymembers_YH <- family_ACTIVEINACTIVE %>% dplyr::filter(substr(address,1,2)=="YH") %>%
                    dplyr::select_("id_service_point","move_in_date","move_out_date","num_house_member") 

YearMonth <- seq(as.Date("2016-03-01"),as.Date(monthStart(today())),"month")
YearMonth_DF <- as.data.frame(YearMonth) %>% dplyr::mutate(Year=year(YearMonth),Month=month(YearMonth)) %>% dplyr::select_("Year","Month")

familymembers_PG_YearMonth <- list()
for (i in 1:length(YearMonth))
{
  familymembers_PG_YearMonth[[i]] <- familymembers_PG %>% 
                                  dplyr::filter(move_in_date <= YearMonth[i] & (move_out_date > YearMonth[i] | is.na(move_out_date))) %>%
                                  dplyr::summarise(TotalHH_PG=sum(num_house_member,na.rm = TRUE))
}
familymembers_PG_YearMonth_DF <- as.data.frame(unlist(familymembers_PG_YearMonth)) 
colnames(familymembers_PG_YearMonth_DF) <- "TotalHH_PG"
familymembers_PG_YearMonth_DF$TotalHH_PG <- round(familymembers_PG_YearMonth_DF$TotalHH_PG)

PG_TotalHH <- cbind(YearMonth_DF,familymembers_PG_YearMonth_DF)
PG_TotalHH$Year <- as.character(PG_TotalHH$Year)
PG_TotalHH$Month <- factor(month.abb[PG_TotalHH$Month],levels = month.abb)
PG_TotalHH_wide <- spread(PG_TotalHH,Month,TotalHH_PG)

familymembers_YH_YearMonth <- list()
for (i in 1:length(YearMonth))
{
  familymembers_YH_YearMonth[[i]] <- familymembers_YH %>% 
    dplyr::filter(move_in_date <= YearMonth[i] & (move_out_date > YearMonth[i] | is.na(move_out_date))) %>%
    dplyr::summarise(TotalHH_YH=sum(num_house_member,na.rm = TRUE))
}
familymembers_YH_YearMonth_DF <- as.data.frame(unlist(familymembers_YH_YearMonth)) 
colnames(familymembers_YH_YearMonth_DF) <- "TotalHH_YH"
familymembers_YH_YearMonth_DF$TotalHH_YH <- round(familymembers_YH_YearMonth_DF$TotalHH_YH)

YH_TotalHH <- cbind(YearMonth_DF,familymembers_YH_YearMonth_DF)
YH_TotalHH$Year <- as.character(YH_TotalHH$Year)
YH_TotalHH$Month <- factor(month.abb[YH_TotalHH$Month],levels = month.abb)
YH_TotalHH_wide <- spread(YH_TotalHH,Month,TotalHH_YH)

save(PG_TotalHH_wide,YH_TotalHH_wide,file="/srv/shiny-server/DataAnalyticsPortal/data/DT/TotalHH.RData")

## below is Average LPCD per month
days_PerMonth <- ts(diff(seq(as.Date(paste(thisYear-2,"-01-01",sep="")), as.Date(paste(thisYear+1,"-01-01",sep="")), by = "month")),
                    start = c(thisYear-2, 01), freq = 12) %>%
                 as.data.frame()

daysPerMonth <- data.frame(Year = c(rep(thisYear-2,12),rep(thisYear-1,12),rep(thisYear,12)),
                           month = c(rep(c(1:12),3)),
                           Days = days_PerMonth$x)
daysPerMonth$month <- as.numeric(daysPerMonth$month)
daysPerMonth$Days <- as.numeric(daysPerMonth$Days)

PunggolCountsPerDay <- length(unique(consumption_last90days_servicepoint[site=="Punggol" & month(date_consumption)==thisMonth]$service_point_sn))*24
PunggolCountsThisMonth <- consumption_last90days_servicepoint[site=="Punggol" & year(date_consumption)==thisYear & month(date_consumption)==thisMonth] %>%
                          dplyr::summarise(TotalCount=n())

PunggolDaysPast <- sum(PunggolCountsThisMonth$TotalCount)/PunggolCountsPerDay

daysPerMonth[which(daysPerMonth$Year==thisYear & daysPerMonth$month==thisMonth),3] <- PunggolDaysPast
daysPerMonth$month <- month.abb[daysPerMonth$month]

PunggolMonthlyConsumption$Year <- as.character(PunggolMonthlyConsumption$Year)
PunggolMonthConsumptionTotalHH <- inner_join(PunggolMonthlyConsumption,PG_TotalHH,by=c("Year","Month"))
PunggolMonthConsumptionTotalHH$Month <- as.character(PunggolMonthConsumptionTotalHH$Month)
PunggolMonthConsumptionTotalHH$Year <- as.numeric(PunggolMonthConsumptionTotalHH$Year)
  
PunggolLPCD_PerMonth <- inner_join(PunggolMonthConsumptionTotalHH,daysPerMonth,by=c("Year","Month"="month")) %>%
                        dplyr::mutate(AverageLPCD=round(ConsumptionPerMonth/(TotalHH_PG*Days))) %>%
                        dplyr::select_("Year","Month","AverageLPCD")

PunggolLPCD_PerMonth$Month <- match(PunggolLPCD_PerMonth$Month,month.abb)  # convert to numeric
PunggolLPCD_PerMonth$Month <- factor(month.abb[PunggolLPCD_PerMonth$Month],levels = month.abb)

PunggolLPCD_PerMonth$Year <- as.character(PunggolLPCD_PerMonth$Year)
PunggolLPCD_PerMonth <- as.data.frame(PunggolLPCD_PerMonth)

PunggolLPCD_PerMonth[1,3] <- 142

write.csv(PunggolLPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/DT/PunggolLPCD_PerMonth.csv",row.names = FALSE)

## Yuhua LPCD Per Month
YuhuaCountsPerDay <- length(unique(consumption_last90days_servicepoint[site=="Yuhua" & month(date_consumption)==thisMonth]$service_point_sn))*24
YuhuaCountsThisMonth <- consumption_last90days_servicepoint[site=="Yuhua" & year(date_consumption)==thisYear & month(date_consumption)==thisMonth] %>%
  dplyr::summarise(TotalCount=n())

YuhuaDaysPast <- sum(YuhuaCountsThisMonth$TotalCount)/YuhuaCountsPerDay

daysPerMonth[which(daysPerMonth$Year==thisYear & daysPerMonth$month==month.abb[thisMonth]),3] <- YuhuaDaysPast

YuhuaMonthlyConsumption$Year <- as.character(YuhuaMonthlyConsumption$Year)
YuhuaMonthConsumptionTotalHH <- inner_join(YuhuaMonthlyConsumption,PG_TotalHH,by=c("Year","Month"))
YuhuaMonthConsumptionTotalHH$Month <- as.character(YuhuaMonthConsumptionTotalHH$Month)
YuhuaMonthConsumptionTotalHH$Year <- as.numeric(YuhuaMonthConsumptionTotalHH$Year)

YuhuaLPCD_PerMonth <- inner_join(YuhuaMonthConsumptionTotalHH,daysPerMonth,by=c("Year","Month"="month")) %>%
  dplyr::mutate(AverageLPCD=round(ConsumptionPerMonth/(TotalHH_PG*Days))) %>%
  dplyr::select_("Year","Month","AverageLPCD")

YuhuaLPCD_PerMonth$Month <- match(YuhuaLPCD_PerMonth$Month,month.abb)  # convert to numeric
YuhuaLPCD_PerMonth$Month <- factor(month.abb[YuhuaLPCD_PerMonth$Month],levels = month.abb)

YuhuaLPCD_PerMonth$Year <- as.character(YuhuaLPCD_PerMonth$Year)
YuhuaLPCD_PerMonth <- as.data.frame(YuhuaLPCD_PerMonth)

save(PunggolLPCD_PerMonth,YuhuaLPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/DT/LPCD_PerMonth.RData")
write.csv(YuhuaLPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/DT/YuhuaLPCD_PerMonth.csv",row.names = FALSE)

## to calculate weekly_LPCD, updated on every Monday
load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/DT/Punggol_WeeklyLPCD.RData")
weeknumber <- as.numeric(strftime(today(),format="%W")) 
i <- weeknumber-1
if (i <10) {i <- paste(0,i,sep="")}
WeekNumber=paste(year(today()),"_",i,sep="") 

week.start <- Week.date$beg[which(Week.date$week==WeekNumber)]
week.end <- Week.date$end[which(Week.date$week==WeekNumber)]

Punggol_WeeklyConsumption  <- consumption_last90days_servicepoint[site=="Punggol" & date(date_consumption) >= week.start & date(date_consumption) <= week.end] %>%
                              dplyr::group_by(service_point_sn) %>%
                              dplyr::summarise(WeeklyConsumption=sum(adjusted_consumption,na.rm = TRUE))

PunggolWeeklyConsumption <- cbind(Date=as.data.frame(week.end),Punggol_WeeklyConsumption)

PG_familymembers_Active <- familymembers_PG %>% 
                           dplyr::group_by(id_service_point) %>% 
                           dplyr::filter(move_in_date==max(move_in_date))

servicepoint <- as.data.frame(tbl(con,"service_point"))
PG_familymembers_Active_ServicePoint <- inner_join(PG_familymembers_Active,servicepoint,by=c("id_service_point"="id")) %>%
                                        dplyr::select_("service_point_sn","num_house_member")

PunggolWeeklyConsumption_NumHouseMember <- inner_join(PunggolWeeklyConsumption,PG_familymembers_Active_ServicePoint,by="service_point_sn") %>%
  dplyr::select_("week.end","service_point_sn","num_house_member","WeeklyConsumption")

Punggol_WeeklyLPCD_new <- PunggolWeeklyConsumption_NumHouseMember %>% 
                          dplyr::group_by(week.end) %>% 
                          dplyr::summarise(PunggolWeeklyConsumption=sum(WeeklyConsumption),TotalHH=sum(num_house_member)) %>%
                          dplyr::mutate(PunggolWeeklyLPCD=round(PunggolWeeklyConsumption/(TotalHH*7),2)) %>% as.data.frame()

Punggol_WeeklyLPCD_new_xts <- xts(Punggol_WeeklyLPCD_new$PunggolWeeklyLPCD,order.by=as.Date(Punggol_WeeklyLPCD_new$week.end))
colnames(Punggol_WeeklyLPCD_new_xts) <- "WeeklyLPCD"

colnames(Punggol_WeeklyLPCD) <- colnames(Punggol_WeeklyLPCD_new)
Punggol_WeeklyLPCD <- rbind(as.data.frame(Punggol_WeeklyLPCD),Punggol_WeeklyLPCD_new)
Punggol_WeeklyLPCD_xts <- rbind(Punggol_WeeklyLPCD_xts,Punggol_WeeklyLPCD_new_xts)

Updated_DateTime_WeeklyLPCD <- paste("Last Updated on ",now(),"."," Next Update on ",now()+7*24*60*60,".",sep="")

save(Punggol_WeeklyLPCD,Punggol_WeeklyLPCD_xts,Updated_DateTime_WeeklyLPCD,file="/srv/shiny-server/DataAnalyticsPortal/data/DT/Punggol_WeeklyLPCD.RData")
write.csv(Punggol_WeeklyLPCD[,c(1,4)],file="/srv/shiny-server/DataAnalyticsPortal/data/DT/Weekly_LPCD.csv",row.names = FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyConsumptionLPCD_HH_WeeklyLPCD_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)
