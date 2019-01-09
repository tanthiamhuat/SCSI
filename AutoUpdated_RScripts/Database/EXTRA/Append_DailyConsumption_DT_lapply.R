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

daily_consumption_DB <- as.data.frame(tbl(proddb,"daily_consumption"))

#consumption_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2017.fst",as.data.table=TRUE)

#servicepoint <- as.data.table(tbl(con,"service_point"))

# set the ON clause as keys of the tables:
#setkey(consumption_2017,id_service_point)
#setkey(servicepoint,id)

# consumption_2017_servicepoint <- consumption_2017[servicepoint, nomatch=0]
# consumption_2017_servicepoint <- consumption_2017_servicepoint[, .(service_point_sn,block,floor,unit,room_type,site,meter_type,
#                                                                    interpolated_consumption,adjusted_consumption,date_consumption)]

consumption_thisyear_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_thisyear_servicepoint.fst",as.data.table=TRUE)

#consumption_servicepoint <- rbind(consumption_2017_servicepoint,consumption_thisyear_servicepoint)

consumption_servicepoint <- consumption_thisyear_servicepoint
PunggolYuhua_SUB <- consumption_servicepoint[!(room_type %in% c("NIL","HDBCD","OTHER","Nil")) & !(is.na(room_type)) &
                                               meter_type =="SUB" & site %in% c("Punggol","Yuhua","Whampoa") &
                                               !is.na(adjusted_consumption)]

PunggolYuhua_SUB <- PunggolYuhua_SUB[,c("Date","wd"):= list(date(date_consumption),weekdays(date(date_consumption)))]

#days_range <- seq(as.Date("2017-01-02"),as.Date("2018-05-12"),by=1)  
## daily_consumption: date_consumption from 2017-01-01 to 2018-05-11

#days_range <- seq(as.Date("2018-01-02"),as.Date("2018-05-22"),by=1) ## testing
days_range <- seq(as.Date("2018-01-02"),as.Date("2018-01-03"),by=1) ## testing

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

PunggolYuhuaConsumption <- inner_join(PunggolYuhua_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

DailyConsumption <- PunggolYuhua_SUB %>%
  group_by(service_point_sn,wd,Date) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(n==24) 

daily_consumption_list <- lapply(days_range,function(day){
  date_past3months <- day-days(90)
  # average of the same day (e.g if today is Wednesday, then all the same 12* Wednesday) of last 3 months (individual) 
  # extract daily consumption, combine with weekdays.
  DailyConsumption_past3months <- DailyConsumption %>%
    group_by(service_point_sn,wd) %>%
    dplyr::filter(wd==weekdays(day-1) & Date >= date_past3months) %>%
    dplyr::summarise(average_consumption_past3months=round(mean(DailyConsumption)))
  
  last7days <- c(day-c(1:7))
  DailyPunggolYuhuaConsumption_last7days <- PunggolYuhuaConsumption %>%
                                            dplyr::filter(Date >= move_in_date) %>%  # take into consideration of move-in-date
                                            group_by(service_point_sn,Date,wd) %>%
                                            dplyr::filter(Date %in% last7days) %>%
                                            dplyr::summarise(dailyconsumption_last7days=sum(interpolated_consumption,na.rm=TRUE),n = n()) %>%
                                            dplyr::filter(n==24) 

  threshold=50
  return(inner_join(DailyPunggolYuhuaConsumption_last7days,DailyConsumption_past3months,
                                     by=c("service_point_sn","wd")) %>%
         group_by(service_point_sn,wd) %>%
         dplyr::mutate(overconsumption=dailyconsumption_last7days-(1+(threshold/100))*average_consumption_past3months) %>%
         dplyr::mutate(overconsumption=round(ifelse(overconsumption<0,0,overconsumption))) %>%
         dplyr::mutate(nett_consumption=round(dailyconsumption_last7days-overconsumption)) %>%
         dplyr::filter(Date==day-1) %>%  
         dplyr::select_("service_point_sn","nett_consumption","overconsumption","Date") 
  )
})

daily_consumption <- rbindlist(daily_consumption_list)

  leak_alarm_open <- as.data.frame(tbl(con,"leak_alarm")) %>% filter(status=="Open" & site %in% c("Punggol","Yuhua"))

  daily_consumption <- daily_consumption %>% 
                       dplyr::mutate(is_leak=ifelse(service_point_sn %in% leak_alarm_open$service_point_sn,TRUE,FALSE))
  ## above is not correct, because leak open dates do not tally with the date range !!
  
  daily_consumption["wd"] <- NULL
  colnames(daily_consumption)[4] <- "date_consumption"
 
  daily_consumption <- daily_consumption[!duplicated(daily_consumption),] # remove duplicates 
 
  #daily_consumption$id <- as.integer(rownames(daily_consumption))
  
  total_rows <- nrow(daily_consumption)
  daily_consumption$id <- as.integer(seq(max(daily_consumption_DB$id)+1,
                                     max(daily_consumption_DB$id)+total_rows,1))
  daily_consumption$id <- as.integer(seq(1,total_rows,1))
  
  daily_consumption_2018 <- daily_consumption %>% 
                       dplyr::select_("id","service_point_sn","nett_consumption","overconsumption","date_consumption","is_leak")
  
  write.csv(daily_consumption_2018,file="/srv/shiny-server/DataAnalyticsPortal/data/daily_consumption_2018.csv")
  dbWriteTable(proddb,"daily_consumption",daily_consumption_2018, append=TRUE, row.names=F, overwrite=FALSE) # append table
  dbDisconnect(proddb)
  
time_taken <- proc.time() - ptm
ans <- paste("Apend_DailyConsumption_DT_lapply successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)
  
cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)