#######  UserTrends (Overwrite table)#################

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

consumption_last6months_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last6months_servicepoint.fst",as.data.table=TRUE)
Punggol_All <- consumption_last6months_servicepoint[site %in% c("Punggol","Whampoa")]

PunggolSUB <- Punggol_All[meter_type=="SUB"]

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

# length(unique(PunggolConsumption_SUB$service_point_sn))=533, excluding ChildCare, and including AHL.
PunggolConsumption <- inner_join(PunggolSUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

yesterday <- today()-1
date_past3months <- today()-90

consumption_yesterday_CountEqual24 <- PunggolConsumption %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(date(date_consumption)==yesterday) %>% 
  dplyr::mutate(Count=n()) %>%
  dplyr::filter(Count==24) %>%
  dplyr::summarise(yesterday_consumption=sum(adjusted_consumption,na.rm=TRUE))

consumption_yesterday_CountNotEqual24 <- PunggolConsumption %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(date(date_consumption)==yesterday) %>% 
  dplyr::mutate(Count=n()) %>%
  dplyr::filter(!(Count==24)) %>%
  dplyr::summarise(yesterday_consumption=NA)

consumption_yesterday <- full_join(consumption_yesterday_CountEqual24,consumption_yesterday_CountNotEqual24,
                                   by=c("service_point_sn","yesterday_consumption")) 

DailyConsumption <- PunggolConsumption %>%
  dplyr::mutate(date=date(date_consumption),wd=weekdays(date)) %>%
  group_by(service_point_sn,wd,date) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(service_point_sn !="3101127564" & n==24) # remove Child-Care, full count of 24 per day

# average of the same day (e.g if today is Wednesday, then all the same 12* Wednesday) of last 3 months (individual) 
# extract daily consumption, combine with weekdays.
DailyConsumption_past3months <- DailyConsumption %>%
  group_by(service_point_sn,wd) %>%
  dplyr::filter(wd==weekdays(yesterday) & date >= date_past3months) %>%
  dplyr::summarise(average_consumption_past3months=round(mean(DailyConsumption)))

user_trends <- inner_join(consumption_yesterday,DailyConsumption_past3months,by="service_point_sn") 
user_trends <- user_trends %>%
  select(service_point_sn,yesterday_consumption,average_consumption_past3months) %>%
  dplyr::mutate(percentage_change=round((yesterday_consumption-average_consumption_past3months)/average_consumption_past3months*100)) %>%
  dplyr::mutate(percentage_change=ifelse(yesterday_consumption==0,NA,percentage_change)) %>%
  dplyr::mutate(percentage_change=ifelse(is.na(yesterday_consumption),NA,percentage_change)) %>%
  dplyr::mutate(percentage_change=ifelse(average_consumption_past3months==0,NA,percentage_change)) 

user_trends$date_created <- today()
user_trends <- as.data.frame(user_trends)      
user_trends <- user_trends %>% select(service_point_sn,yesterday_consumption,average_consumption_past3months,percentage_change,date_created)
user_trends$id <- as.integer(rownames(user_trends))
user_trends <- user_trends %>% dplyr::select_("id","service_point_sn","yesterday_consumption","average_consumption_past3months",
                                              "percentage_change","date_created")

dbSendQuery(mydb, "delete from user_trends")
dbWriteTable(mydb, "user_trends", user_trends, append=TRUE, row.names=F, overwrite=FALSE)

#######  DailyConsumption (Appended table)#################
leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))

daily_consumption_DB <- as.data.frame(tbl(con,"daily_consumption"))
existing_data <- daily_consumption_DB %>% select(service_point_sn,date_consumption) %>% 
  dplyr::mutate(date_consumption = date(date_consumption)) %>%
  select(service_point_sn,date_consumption)

last7days <- c(Sys.Date()-c(1:7))

DailyPunggolConsumption_last7days <- PunggolConsumption %>%
  dplyr::filter(date(date_consumption) >= move_in_date) %>%  # take into consideration of move-in-date
  dplyr::mutate(date=date(date_consumption),wd=weekdays(date_consumption)) %>%
  group_by(service_point_sn,date,wd) %>%
  dplyr::filter(date %in% last7days) %>%
  dplyr::summarise(dailyconsumption_last7days=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(service_point_sn !="3101127564" & n==24) # exclude ChildCare, full count of 24 per day
## should have 527 unique service_point_sn, excluding ChildCare, and including AHL.

threshold=50
daily_consumption <- inner_join(DailyPunggolConsumption_last7days,DailyConsumption_past3months,
                                     by=c("service_point_sn","wd")) %>%
  group_by(service_point_sn,wd) %>%
  dplyr::mutate(overconsumption=dailyconsumption_last7days-(1+(threshold/100))*average_consumption_past3months) %>%
  dplyr::mutate(overconsumption=round(ifelse(overconsumption<0,0,overconsumption))) %>%
  dplyr::mutate(nett_consumption=round(dailyconsumption_last7days-overconsumption)) %>%
  dplyr::filter(date==today()-1) %>%  
  dplyr::select_("service_point_sn","nett_consumption","overconsumption","date") %>%
  dplyr::rename(date_consumption=date)

new_data <- daily_consumption %>% select(service_point_sn,date_consumption) %>% as.data.frame()
diff_data <- anti_join(new_data,existing_data) # check if new_data are currently in the daily_consumption DB

if(NROW(diff_data)>0){
  daily_consumption <- inner_join(daily_consumption,diff_data)

  leak_alarm_open <- leak_alarm %>% dplyr::filter(status=="Open")

  daily_consumption <- daily_consumption %>% 
                       dplyr::mutate(is_leak=ifelse(service_point_sn %in% leak_alarm_open$service_point_sn,TRUE,FALSE))
    
  number_customers <- length(daily_consumption$service_point_sn) 
  
  daily_consumption$id <- as.integer(seq(max(daily_consumption_DB$id)+1,
                                         max(daily_consumption_DB$id)+number_customers,1))
  
  daily_consumption <- daily_consumption[,c(ncol(daily_consumption),c(1:(ncol(daily_consumption)-1)))]
  
  daily_consumption <- as.data.frame(daily_consumption)
  
  colnames(daily_consumption)[which(colnames(daily_consumption)=="date_consumption")] <- "date_consumption"
  
  daily_consumption <- daily_consumption %>% dplyr::select_("id","service_point_sn","nett_consumption",
                                                            "overconsumption","date_consumption","is_leak")
  ## daily appended table
  dbWriteTable(mydb, "daily_consumption", daily_consumption, append=TRUE, row.names=F, overwrite=FALSE) # append table
  dbDisconnect(mydb)
}

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_UserTrends_DailyConsumption successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
