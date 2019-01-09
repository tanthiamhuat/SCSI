rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

# Establish connection
mydb <- dbConnect(PostgreSQL(), dbname="amrstaging",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")
con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))

# length(unique(PunggolConsumption_SUB$service_point_sn))=534, including ChildCare, and including AHL.
PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by="service_point_sn") 

yesterday <- today()-1
date_past3months <- today()-1-months(3)

consumption_yesterday_CountEqual24 <- PunggolConsumption %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date)==yesterday & service_point_sn !="3101127564") %>% # remove Child-Care
  dplyr::mutate(Count=n()) %>%
  dplyr::filter(Count==24) %>%
  dplyr::summarise(yesterday_consumption=sum(adjusted_consumption))

consumption_yesterday_CountNotEqual24 <- PunggolConsumption %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date)==yesterday & 
                  service_point_sn !="3101127564") %>% # remove Child-Care
  dplyr::mutate(Count=n()) %>%
  dplyr::filter(!(Count==24)) %>%
  dplyr::summarise(yesterday_consumption=NA)

consumption_yesterday <- full_join(consumption_yesterday_CountEqual24,consumption_yesterday_CountNotEqual24,
                                   by=c("service_point_sn","yesterday_consumption")) 

DailyConsumption <- PunggolConsumption %>% 
  dplyr::mutate(date=date(adjusted_date),wkday=weekdays(date)) %>%
  group_by(service_point_sn,date,month,day,wkday) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE)) %>%
  dplyr::filter(service_point_sn !="3101127564") # remove Child-Care

# average of the same day (e.g if today is Wednesday, then all the same 12* Wednesday) of last 3 months (individual) 
# extract daily consumption, combine with weekdays.
DailyConsumption_past3months <- DailyConsumption %>%
  group_by(service_point_sn) %>%
  dplyr::filter(wkday==weekdays(yesterday) & date >= date_past3months) %>%
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

#dbWriteTable(mydb, "user_trends", user_trends, append=FALSE, row.names=F, overwrite=TRUE)
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_UserTrends_V2_Hourly successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)