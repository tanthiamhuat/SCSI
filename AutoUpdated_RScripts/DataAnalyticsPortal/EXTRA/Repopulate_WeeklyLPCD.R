## for this year and last year

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,xts,data.table,tidyr,fst)

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family_ACTIVE <- as.data.frame(tbl(con,"family") %>% 
                                 dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                                               & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family_ACTIVE,servicepoint,by=c("id_service_point"="id","room_type")) 

Punggol_2016 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2016.fst")[,1:12]
Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_lastyear.fst")[,1:12]
Punggol_2018 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")

Punggol_All <- rbind(rbind(Punggol_2016,Punggol_2017),Punggol_2018)

yesterday <- today()-1
thisYear <- year(today())
thismonth <- month(today())
lastYear <- thisYear-1

## below is for Monthly Consumption
PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792") %>%  # remove AHL
  dplyr::mutate(day=date(date_consumption),month=month(date_consumption)) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,day,month) 

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

# http://stackoverflow.com/questions/29402528/append-data-frames-together-in-a-for-loop
## every Monday
#days <- seq(as.Date("2016-03-21"),as.Date("2016-12-26"),by=7)
days <- seq(as.Date("2016-03-21"),as.Date("2018-09-03"),by=7)
WeeklyConsumption <- list()

for (k in 1:length(days))
{
  weeknumber <- as.numeric(strftime(days[k],format="%W")) 
  i <- weeknumber-1
  if (i <10) {i <- paste(0,i,sep="")}
  WeekNumber=paste(year(days[k]),"_",i,sep="") 
  if (i =="00") {
    i <- 52
    WeekNumber=paste(year(today())-1,"_",i,sep="")
  }
  week.start <- Week.date$beg[which(Week.date$week==WeekNumber)]
  week.end <- Week.date$end[which(Week.date$week==WeekNumber)]
  
  Weekly_Consumption  <- PunggolConsumption %>% dplyr::group_by(service_point_sn) %>%
                         dplyr::filter(date(adjusted_date) >= week.start & date(adjusted_date) <= week.end) %>%
                         dplyr::summarise(WeeklyConsumption=sum(adjusted_consumption,na.rm = TRUE))
  
  WeeklyConsumption[[k]] <- cbind(Date=as.data.frame(week.end),Weekly_Consumption)
}

WeeklyConsumption_tmp <- as.data.frame(rbindlist(WeeklyConsumption)) 

PunggolConsumption_HHSize <- PunggolConsumption %>% dplyr::select_("service_point_sn","num_house_member") %>% unique()

WeeklyConsumption_NumHouseMember <- inner_join(WeeklyConsumption_tmp,PunggolConsumption_HHSize,by="service_point_sn") %>%
                                    dplyr::select_("week.end","service_point_sn","num_house_member","WeeklyConsumption")

Weekly_LPCD <- WeeklyConsumption_NumHouseMember %>% 
              dplyr::group_by(week.end) %>% 
              dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption),TotalHH=sum(num_house_member)) %>%
              dplyr::mutate(WeeklyLPCD=round(WeeklyConsumption/(TotalHH*7),2))

Weekly_LPCD_xts <- xts(Weekly_LPCD$WeeklyLPCD,order.by=as.Date(Weekly_LPCD$week.end))
colnames(Weekly_LPCD_xts) <- "WeeklyLPCD"
  
save(Weekly_LPCD,Weekly_LPCD_xts,file="/srv/shiny-server/DataAnalyticsPortal/data/Weekly_LPCD.RData")
write.csv(Weekly_LPCD[,c(1,4)],file="/srv/shiny-server/DataAnalyticsPortal/data/Weekly_LPCD.csv",row.names = FALSE)

time_taken <- proc.time() - ptm
ans <- paste("Repopulated_WeeklyLPCD successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

library(dygraphs)
graph <- dygraph(Weekly_LPCD_xts, main = "Weekly LPCD") %>%  
  dyRangeSelector() %>%
  dyAxis("y",label=HTML('Weekly LPCD'),valueRange = c(120, 145)) 
graph
