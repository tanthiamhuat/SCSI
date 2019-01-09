#######  UserTrends (Overwrite table)#################

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

# Establish connection
mydb <- dbConnect(PostgreSQL(), dbname="amrstaging",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")
con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

# length(unique(PunggolConsumption_SUB$service_point_sn))=533, excluding ChildCare, and including AHL.
PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

yesterday <- today()-1
date_past3months <- today()-days(90)

consumption_yesterday_CountEqual24 <- PunggolConsumption %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date)==yesterday) %>% 
  dplyr::mutate(Count=n()) %>%
  dplyr::filter(Count==24) %>%
  dplyr::summarise(yesterday_consumption=sum(adjusted_consumption,na.rm=TRUE))

consumption_yesterday_CountNotEqual24 <- PunggolConsumption %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date)==yesterday) %>% 
  dplyr::mutate(Count=n()) %>%
  dplyr::filter(!(Count==24)) %>%
  dplyr::summarise(yesterday_consumption=NA)

consumption_yesterday <- full_join(consumption_yesterday_CountEqual24,consumption_yesterday_CountNotEqual24,
                                   by=c("service_point_sn","yesterday_consumption")) 

DailyConsumption <- PunggolConsumption %>%
  dplyr::mutate(date=date(adjusted_date),wd=weekdays(date)) %>%
  group_by(service_point_sn,month,day,wd,date) %>%
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
  dplyr::mutate(adjusted_date = date(date_consumption)) %>%
  select(service_point_sn,adjusted_date)

last7days <- c(Sys.Date()-c(1:7))

DailyPunggolConsumption_last7days <- PunggolConsumption %>%
  dplyr::filter(date(adjusted_date) >= move_in_date) %>%  # take into consideration of move-in-date
  dplyr::mutate(date=date(adjusted_date),wd=weekdays(adjusted_date)) %>%
  group_by(service_point_sn,date,month,day,wd) %>%
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
  dplyr::rename(adjusted_date=date)

new_data <- daily_consumption %>% select(service_point_sn,adjusted_date) %>% as.data.frame()
diff_data <- anti_join(new_data,existing_data) # check if new_data are currently in the daily_consumption DB

if(NROW(diff_data)>0){
  daily_consumption <- inner_join(daily_consumption,diff_data)
  leak_alarm$start_date <- as.Date(leak_alarm$start_date) 
  leak_alarm$end_date <- as.Date(leak_alarm$end_date)  
  
  leak_alarm <- leak_alarm %>% filter(service_point_sn %in% daily_consumption$service_point_sn)
  
  daily_consumption_leak_alarm <- left_join(daily_consumption,leak_alarm,by=c("service_point_sn","adjusted_date"="start_date")) %>%
    dplyr::select_("service_point_sn","nett_consumption","overconsumption","adjusted_date","status") %>%
    dplyr::mutate(is_leak=ifelse(is.na(status),FALSE,TRUE))  ## to check....
 
  daily_consumption <- daily_consumption_leak_alarm %>%
    dplyr::select_("service_point_sn","nett_consumption","overconsumption","adjusted_date","is_leak")
  
  number_customers <- length(daily_consumption$service_point_sn) 
  
  daily_consumption$id <- as.integer(seq(max(daily_consumption_DB$id)+1,
                                         max(daily_consumption_DB$id)+number_customers,1))
  
  daily_consumption <- daily_consumption[,c(ncol(daily_consumption),c(1:ncol(daily_consumption)-1))]
  
  daily_consumption <- as.data.frame(daily_consumption)
  
  colnames(daily_consumption)[which(colnames(daily_consumption)=="adjusted_date")] <- "date_consumption"
  
  daily_consumption <- daily_consumption %>% dplyr::select_("id","service_point_sn","nett_consumption",
                                                            "overconsumption","date_consumption","is_leak")
  ## daily appended table
  dbWriteTable(mydb, "daily_consumption", daily_consumption, append=TRUE, row.names=F, overwrite=FALSE) # append table
  dbDisconnect(mydb)
}

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_UserTrends_DailyConsumption successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)