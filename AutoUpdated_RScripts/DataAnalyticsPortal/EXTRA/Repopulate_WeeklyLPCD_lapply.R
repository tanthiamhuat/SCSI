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

consumption_2016_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2016_servicepoint.fst",as.data.table = TRUE)
consumption_2017_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2017_servicepoint.fst",as.data.table = TRUE)
consumption_thisyear_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_thisyear_servicepoint.fst",as.data.table = TRUE)
consumption_all_servicepoint <- rbind(rbind(consumption_2016_servicepoint,consumption_2017_servicepoint),consumption_thisyear_servicepoint)

PunggolConsumption_SUB <- consumption_all_servicepoint[site=="Punggol" & meter_type=="SUB" &
                                                       !(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792"]

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
  group_by(service_point_sn) %>%
  dplyr::filter(date(date_consumption)>=date(move_in_date) & (date(date_consumption)<date(move_out_date) | is.na(move_out_date)))

PunggolConsumption <- unique(PunggolConsumption[c("service_point_sn","adjusted_consumption","date_consumption","num_house_member","room_type","block")])

# http://stackoverflow.com/questions/29402528/append-data-frames-together-in-a-for-loop
## every Monday
#days <- seq(as.Date("2016-03-21"),as.Date("2016-12-26"),by=7)
weeks_range <- seq(as.Date("2016-03-21"),as.Date("2018-09-03"),by=7)
weeks_range <- seq(as.Date("2016-03-21"),as.Date("2016-04-03"),by=7)
# WeeklyConsumption <- list()
# 
# for (k in 1:length(days))
# {
#   weeknumber <- as.numeric(strftime(days[k],format="%W")) 
#   i <- weeknumber-1
#   if (i <10) {i <- paste(0,i,sep="")}
#   WeekNumber=paste(year(days[k]),"_",i,sep="") 
#   if (i =="00") {
#     i <- 52
#     WeekNumber=paste(year(today())-1,"_",i,sep="")
#   }
#   week.start <- Week.date$beg[which(Week.date$week==WeekNumber)]
#   week.end <- Week.date$end[which(Week.date$week==WeekNumber)]
#   
#   Weekly_Consumption  <- PunggolConsumption %>% dplyr::group_by(service_point_sn) %>%
#                          dplyr::filter(date(date_consumption) >= week.start & date(date_consumption) <= week.end) %>%
#                          dplyr::summarise(WeeklyConsumption=sum(adjusted_consumption,na.rm = TRUE))
#   
#   WeeklyConsumption[[k]] <- cbind(Date=as.data.frame(week.end),Weekly_Consumption)
# }
# 
# WeeklyConsumption_tmp <- as.data.frame(rbindlist(WeeklyConsumption))


## use lapply to speed up
WeeklyConsumption_list <- lapply(weeks_range,function(week){
  weeknumber <- as.numeric(strftime(week,format="%W")) 
  i <- weeknumber-1
  if (i <10) {i <- paste(0,i,sep="")}
  WeekNumber=paste(year(week),"_",i,sep="") 
  if (i =="00") {
    i <- 52
    WeekNumber=paste(year(today())-1,"_",i,sep="")
  }
  week.start <- Week.date$beg[which(Week.date$week==WeekNumber)]
  week.end <- Week.date$end[which(Week.date$week==WeekNumber)]
  
  PunggolWeeklyConsumption <- PunggolConsumption %>% dplyr::group_by(service_point_sn) %>%
    dplyr::mutate(date=date(date_consumption)) %>%
    dplyr::filter(date >= week.start & date <= week.end) %>%
    dplyr::summarise(WeeklyConsumption=sum(adjusted_consumption,na.rm = TRUE))
  
  #return(cbind(Date=as.data.frame(week.end),PunggolWeeklyConsumption))
  return(PunggolWeeklyConsumption)
})

WeeklyConsumption <- rbindlist(WeeklyConsumption_list)

DailyLPCD_list <- lapply(days_range,function(day){
  return(DailyConsumption %>% 
           dplyr::filter(date==day) %>%
           dplyr::group_by(date) %>%
           dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
           dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH)
  )
})
DailyLPCD <- rbindlist(DailyLPCD_list)


PunggolConsumption_HHSize <- PunggolConsumption %>% dplyr::select_("service_point_sn","num_house_member") %>% unique()

WeeklyConsumption_NumHouseMember <- inner_join(WeeklyConsumption,PunggolConsumption_HHSize,by="service_point_sn") %>%
                                    dplyr::select_("week.end","service_point_sn","num_house_member","WeeklyConsumption")

Punggol_WeeklyLPCD <- WeeklyConsumption_NumHouseMember %>% 
                      dplyr::group_by(week.end) %>% 
                      dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption),TotalHH=sum(num_house_member)) %>%
                      dplyr::mutate(WeeklyLPCD=round(WeeklyConsumption/(TotalHH*7),2))

Punggol_WeeklyLPCD_xts <- xts(Punggol_WeeklyLPCD$WeeklyLPCD,order.by=as.Date(Punggol_WeeklyLPCD$week.end))
colnames(Punggol_WeeklyLPCD_xts) <- "WeeklyLPCD"
  
save(Punggol_WeeklyLPCD,Punggol_WeeklyLPCD_xts,file="/srv/shiny-server/DataAnalyticsPortal/data/DT/Punggol_WeeklyLPCD.RData")
write.csv(Punggol_WeeklyLPCD[,c(1,4)],file="/srv/shiny-server/DataAnalyticsPortal/data/DT/Punggol_WeeklyLPCD.csv",row.names = FALSE)

time_taken <- proc.time() - ptm
ans <- paste("Repopulated_WeeklyLPCD successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

library(dygraphs)
graph <- dygraph(Punggol_WeeklyLPCD_xts, main = "Weekly LPCD") %>%  
  dyRangeSelector() %>%
  dyAxis("y",label=HTML('Weekly LPCD'),valueRange = c(120, 145)) 
graph
