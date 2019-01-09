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

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

consumption_last30days_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last30days_servicepoint.fst",as.data.table=TRUE)

PunggolYuhua_SUB <- consumption_last30days_servicepoint[!(room_type %in% c("NIL","HDBCD","OTHER","Nil")) & !(is.na(room_type)) & 
                                                          meter_type =="SUB" & site %in% c("Punggol","Yuhua","Whampoa") &
                                                          !is.na(adjusted_consumption)]

PunggolYuhua_SUB <- PunggolYuhua_SUB[,c("Date","wd"):= list(date(date_consumption),weekdays(date(date_consumption)))]

family <- as.data.frame(tbl(con,"family")) %>%
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD","NIL","OTHER")))
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

PunggolYuhuaConsumption <- inner_join(PunggolYuhua_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type"))

yesterday <- today()-1
date_past3months <- today()-90

consumption_yesterday_CountEqual24 <- PunggolYuhua_SUB %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(Date==yesterday) %>% 
  dplyr::mutate(Count=n()) %>%
  dplyr::filter(Count==24) %>%
  dplyr::summarise(yesterday_consumption=sum(adjusted_consumption,na.rm=TRUE))

consumption_yesterday_CountNotEqual24 <- PunggolYuhua_SUB %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(Date==yesterday) %>% 
  dplyr::mutate(Count=n()) %>%
  dplyr::filter(!(Count==24)) %>%
  dplyr::summarise(yesterday_consumption=NA)

consumption_yesterday <- full_join(consumption_yesterday_CountEqual24,consumption_yesterday_CountNotEqual24,
                                   by=c("service_point_sn","yesterday_consumption")) 

DailyConsumption <- PunggolYuhua_SUB %>%
  group_by(service_point_sn,wd,Date) %>%
  dplyr::summarise(DailyConsumption=sum(interpolated_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(n==24) 

# average of the same day (e.g if today is Wednesday, then all the same 12* Wednesday) of last 3 months (individual) 
# extract daily consumption, combine with weekdays.
DailyConsumption_past3months <- DailyConsumption %>%
  group_by(service_point_sn,wd) %>%
  dplyr::filter(wd==weekdays(yesterday) & Date >= date_past3months) %>%
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

dbSendQuery(proddb, "delete from user_trends")
dbWriteTable(proddb, "user_trends", user_trends, append=TRUE, row.names=F, overwrite=FALSE)

user_trends_full <- user_trends %>% dplyr::filter(!is.na(yesterday_consumption))

user_trends_block <- inner_join(user_trends_full,servicepoint,by="service_point_sn") %>%
                     dplyr::select_("service_point_sn","block") %>%
                     dplyr::group_by(block) %>%
                     dplyr::summarise(Count=n())

#######  DailyConsumption (Appended table)#################
leak_alarm_YH <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/Leak_Yuhua.csv")
leak_alarm_YH_open <- leak_alarm_YH %>% filter(status=="Open")

leak_alarm_PG <- as.data.frame(tbl(con,"leak_alarm"))
leak_alarm_PG_open <- leak_alarm_PG %>% dplyr::filter(status=="Open" & site=="Punggol")

leak_alarm_open <- c(leak_alarm_PG_open$service_point_sn,leak_alarm_YH_open$service_point_sn)

daily_consumption_DB <- as.data.frame(tbl(proddb,"daily_consumption"))
existing_data <- daily_consumption_DB %>% select(service_point_sn,date_consumption) %>% filter(date_consumption==today()-1)

last7days <- c(Sys.Date()-c(1:7))

DailyPunggolYuhuaConsumption_last7days <- PunggolYuhuaConsumption %>%
  dplyr::filter(Date >= move_in_date) %>%  # take into consideration of move-in-date
  group_by(service_point_sn,Date,wd) %>%
  dplyr::filter(Date %in% last7days) %>%
  dplyr::summarise(dailyconsumption_last7days=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(n==24) 

threshold=50
daily_consumption <- inner_join(DailyPunggolYuhuaConsumption_last7days,DailyConsumption_past3months,
                                     by=c("service_point_sn","wd")) %>%
  group_by(service_point_sn,wd) %>%
  dplyr::mutate(overconsumption=dailyconsumption_last7days-(1+(threshold/100))*average_consumption_past3months) %>%
  dplyr::mutate(overconsumption=round(ifelse(overconsumption<0,0,overconsumption))) %>%
  dplyr::mutate(nett_consumption=round(dailyconsumption_last7days-overconsumption)) %>%
  dplyr::filter(Date==today()-1) %>%  
  dplyr::select_("service_point_sn","nett_consumption","overconsumption","Date") %>% as.data.frame()

new_data <- daily_consumption %>% dplyr::select_("service_point_sn","Date") %>% as.data.frame()
colnames(new_data)[2] <- "date_consumption"
existing_data$date_consumption <- as.Date(existing_data$date_consumption)+1  ## need to add one here.
diff_data <- anti_join(new_data,existing_data) # check if new_data are currently in the daily_consumption DB

if(NROW(diff_data)>0){
  daily_consumption <- inner_join(daily_consumption,diff_data,by=c("service_point_sn","Date"="date_consumption"))
  daily_consumption[1] <- NULL

  daily_consumption <- daily_consumption %>% 
                       dplyr::mutate(is_leak=ifelse(service_point_sn %in% leak_alarm_open,TRUE,FALSE))
    
  number_customers <- length(daily_consumption$service_point_sn) 
  
  daily_consumption$id <- as.integer(seq(max(daily_consumption_DB$id)+1,
                                         max(daily_consumption_DB$id)+number_customers,1))
  
  daily_consumption <- daily_consumption[,c(ncol(daily_consumption),c(1:(ncol(daily_consumption)-1)))]
  
  daily_consumption <- as.data.frame(daily_consumption)
  
  colnames(daily_consumption)[which(colnames(daily_consumption)=="Date")] <- "date_consumption"
  
  daily_consumption <- daily_consumption %>% dplyr::select_("id","service_point_sn","nett_consumption",
                                                            "overconsumption","date_consumption","is_leak")
  ## daily appended table
  #dbWriteTable(mydb, "daily_consumption", daily_consumption, append=TRUE, row.names=F, overwrite=FALSE) # append table
  dbWriteTable(proddb, "daily_consumption", daily_consumption, append=TRUE, row.names=F, overwrite=FALSE) # append table
  #dbDisconnect(mydb)
  dbDisconnect(proddb)
}

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_UserTrends_DailyConsumption_Hourly_PunggolYuhua successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)
