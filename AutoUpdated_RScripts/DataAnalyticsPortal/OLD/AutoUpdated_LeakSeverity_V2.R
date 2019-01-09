## leak start date and end date is on first day when min_5_flow > and min_5_flow=0 respectively.
rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

flow <- as.data.frame(tbl(con,"flow"))
servicepoint <- as.data.frame(tbl(con,"service_point"))
meter <- as.data.frame(tbl(con,"meter"))

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate"))

flow_servicepoint_meter <- inner_join(servicepoint_meter,flow,by=c("id.x"="id_service_point"))

leaks_open <- flow_servicepoint_meter %>% 
  dplyr::filter(min_5_flow>0) %>%
  dplyr::mutate(date=as.Date(flow_date),Time=substr(flow_date,12,19)) %>%
  group_by(service_point_sn) %>%
  select(service_point_sn,meter_sn,min_5_flow,flow_date,date,Time) 

leaks_close <- flow_servicepoint_meter %>% 
  dplyr::filter(min_5_flow==0) %>%
  dplyr::mutate(date=as.Date(flow_date),Time=substr(flow_date,12,19)) %>%
  group_by(service_point_sn) %>%
  select(service_point_sn,meter_sn,min_5_flow,flow_date,date,Time) 

leaks_open <- as.data.frame(leaks_open)
leaks_close <- as.data.frame(leaks_close)

leaks_open <- leaks_open %>% group_by(service_point_sn,date) %>% filter(Time==max(Time))
leaks_close <- leaks_close %>% group_by(service_point_sn,date) %>% filter(Time==max(Time))
# above filter out duplicates and remove those with earlier time

ListOfOpenLeaks <- leaks_open %>% 
  group_by(service_point_sn) %>%
  arrange(date) %>%
  dplyr::mutate(consecutiveDay = c(NA,diff(date)==1)) %>%
  dplyr::filter(consecutiveDay!='NA') %>%
  dplyr::mutate(consecutiveDays = ave(consecutiveDay, cumsum(consecutiveDay == FALSE), FUN = cumsum)) %>%
  # cumulative sum that resets when FALSE is encountered
  dplyr::filter(consecutiveDays==2 & consecutiveDay==TRUE)

ListOfCloseLeaks <- leaks_close %>% 
  group_by(service_point_sn) %>%
  arrange(date) %>%
  dplyr::mutate(consecutiveDay = c(NA,diff(date)==1)) %>%
  dplyr::filter(consecutiveDay!='NA' & service_point_sn %in% unique(ListOfOpenLeaks$service_point_sn)) %>%
  dplyr::mutate(consecutiveDays = ave(consecutiveDay, cumsum(consecutiveDay == FALSE), FUN = cumsum)) %>%
  # cumulative sum that resets when FALSE is encountered
  dplyr::filter(consecutiveDays==2 & consecutiveDay==TRUE)

ListOfOpenCloseLeaks <- rbind(ListOfOpenLeaks,ListOfCloseLeaks) %>%
  arrange(service_point_sn,flow_date)

ListOfEffectiveOpenCloseLeaks <- ListOfOpenCloseLeaks %>%
  group_by(service_point_sn) %>%
  dplyr::mutate(Min5Flow_GreaterZero = c(min_5_flow>0)) %>%
  dplyr::mutate(consecutiveMin5Flow_GreaterZero = ave(Min5Flow_GreaterZero, cumsum(Min5Flow_GreaterZero == FALSE), FUN = cumsum)) %>%
  dplyr::filter(!(Min5Flow_GreaterZero==TRUE & consecutiveMin5Flow_GreaterZero !=1)) %>%
  dplyr::mutate(Min5Flow_EqualZero = c(min_5_flow==0)) %>%
  dplyr::mutate(consecutiveMin5Flow_EqualZero = ave(Min5Flow_EqualZero, cumsum(Min5Flow_EqualZero == FALSE), FUN = cumsum)) %>%
  dplyr::filter(!(Min5Flow_EqualZero==TRUE & consecutiveMin5Flow_EqualZero !=1)) %>%
  dplyr::mutate(status=ifelse(min_5_flow>0,"Open","Close")) %>%
  dplyr::mutate(row=row_number()) %>%
  # below to remove first close with no open
  dplyr::filter(!(row==1 & min_5_flow==0)) %>%
  dplyr::select_("service_point_sn","meter_sn","min_5_flow","date","status","row") %>%
  dplyr::mutate(duration=ifelse(status=="Open",0,c(NA,diff(date)+1))) %>%
  dplyr::mutate(lastopen=ifelse(row==max(row) & status=="Open",1,0)) %>%
  dplyr::mutate(duration1=ifelse(lastopen==1,today()-as.Date(date)+1,0)) %>%
  dplyr::mutate(duration=duration+duration1) %>%
  dplyr::select_("service_point_sn","meter_sn","min_5_flow","date","status","row","duration","lastopen") 

NotCloseLeaks <- ListOfEffectiveOpenCloseLeaks %>% filter(lastopen==1)
NotCloseLeaksAll <- leaks_open %>% filter(service_point_sn %in% NotCloseLeaks$service_point_sn) %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(date > max(date)-3) %>%  # last 3 days
  dplyr::summarise(min5flow_ave=round(mean(min_5_flow),2))

ListOfEffectiveOpenCloseLeaks <- ListOfEffectiveOpenCloseLeaks %>%
  group_by(lastopen) %>%
  dplyr::mutate(min5flow_ave=ifelse(lastopen==1,NotCloseLeaksAll$min5flow_ave,0))

CloseLeaks <- ListOfEffectiveOpenCloseLeaks %>% filter(status=="Close") %>% dplyr::select_("service_point_sn","meter_sn","date")
CloseLeaksAll <- leaks_open %>%
  filter(service_point_sn %in% CloseLeaks$service_point_sn) %>%
  group_by(service_point_sn)

CloseLeaksAll_Min5Flow_ave <- inner_join(CloseLeaks,CloseLeaksAll,by=c("service_point_sn","meter_sn")) %>% 
  filter(date.x > date.y) %>%
  group_by(service_point_sn,date.x) %>%
  dplyr::filter(row_number()==c(n()) | row_number()==c(n()-1) |
                  row_number()==c(n()-2)) %>% # last 3 days
  dplyr::summarise(min5flow_ave=round(mean(min_5_flow),2))

ListOfEffectiveOpenCloseLeaks <- ListOfEffectiveOpenCloseLeaks %>%
  group_by(status,lastopen) %>%
  dplyr::mutate(min5flow_ave1=ifelse(status=="Close",
                                     CloseLeaksAll_Min5Flow_ave$min5flow_ave,0)) %>%
  dplyr::mutate(min5flow_ave=min5flow_ave+min5flow_ave1) %>%
  dplyr::select_("service_point_sn","meter_sn","min_5_flow","date","status","duration","min5flow_ave") %>%
  dplyr::mutate(severity=ifelse((status=="Close" | (status=="Open" & duration !=0)) & duration < 5 & min5flow_ave < 10,"Moderate",
                         ifelse((status=="Close" | (status=="Open" & duration !=0)) & (duration < 5 & min5flow_ave >=10) |
                                         (duration >=5 & min5flow_ave < 10),"Severe",
                         ifelse((status=="Close" | (status=="Open" & duration !=0)) & duration >=5 & min5flow_ave >=10,"Critical",NA))))

ListOfEffectiveOpenCloseLeaks <- as.data.frame(ListOfEffectiveOpenCloseLeaks)
ListOfEffectiveOpenCloseLeaks_StartEndDates <- ListOfEffectiveOpenCloseLeaks %>% group_by(service_point_sn) %>%
  dplyr::mutate(start_date=ifelse(status=="Close",format(lag(date),"%Y-%m-%d"),NA)) %>%
  dplyr::mutate(end_date=ifelse(status=="Close",format(date,"%Y-%m-%d"),NA)) %>%
  dplyr::mutate(start_date1=ifelse(lastopen==1,format(date,"%Y-%m-%d"),NA)) %>%
  dplyr::mutate(start_date=ifelse(lastopen==1,start_date1,
                                  ifelse(lastopen==0,start_date,NA)))
ListOfEffectiveOpenCloseLeaks_StartEndDates <- as.data.table(ListOfEffectiveOpenCloseLeaks_StartEndDates)
ListOfEffectiveOpenCloseLeaks_StartEndDates <- ListOfEffectiveOpenCloseLeaks_StartEndDates[,`:=`(lastopen = NULL, min_5_flow = NULL, 
                                                                                                 date = NULL, start_date1 = NULL)]

ListOfEffectiveOpenCloseLeaks_StartEndDates <- ListOfEffectiveOpenCloseLeaks_StartEndDates[!is.na(ListOfEffectiveOpenCloseLeaks_StartEndDates$severity),]

LeakSeverity <- ListOfEffectiveOpenCloseLeaks_StartEndDates %>%
  dplyr::select_("service_point_sn","meter_sn","start_date","end_date","duration","min5flow_ave","severity","status")

## leak start date and end date is on first day when min_5_flow > and min_5_flow=0 respectively.
## start and end date to be adjusted to match with the min flow from EMIS (-2 days)
LeakSeverity$start_date <- as.Date(LeakSeverity$start_date)-2
LeakSeverity$end_date <- as.Date(LeakSeverity$end_date)-2

LeakSeverity$meter_type <- meter$meter_type[match(LeakSeverity$meter_sn,meter$meter_sn)]

LeakSeverity$site <- servicepoint$site[match(LeakSeverity$service_point_sn,servicepoint$service_point_sn)]

LeakSeverity$id <- as.integer(rownames(LeakSeverity))

LeakSeverity <- LeakSeverity %>% 
  dplyr::select_("id","service_point_sn","meter_sn","meter_type","site","start_date","end_date",
                 "duration","min5flow_ave","severity","status")

LeakSeverity$notification_status <- rep("new")

LeakSeverity <- LeakSeverity %>% dplyr::mutate(potential_savings=round(min5flow_ave*24*365*2.56/1000))

dbSendQuery(mydb, "delete from leak_alarm")
dbWriteTable(mydb, "leak_alarm", LeakSeverity, append=TRUE, row.names=F, overwrite=FALSE)
dbDisconnect(mydb)

LeakSeverity_Punggol <- LeakSeverity %>% filter(site!="Tuas") %>% arrange(desc(status)) 
LeakSeverity_Punggol["id"] <- NULL

colnames(LeakSeverity_Punggol)[7] <- "duration (days)"
colnames(LeakSeverity_Punggol)[8] <- "min5flow_ave (litres/hour)"

LeakSeverity_Punggol[(ncol(LeakSeverity_Punggol)-1):ncol(LeakSeverity_Punggol)] <- NULL

save(LeakSeverity_Punggol, file="/srv/shiny-server/DataAnalyticsPortal/data/LeakSeverity.RData")
write.csv(LeakSeverity_Punggol,"/srv/shiny-server/DataAnalyticsPortal/data/LeakSeverity_Punggol.csv",row.names=FALSE)

#################################################################################
##### Field “notification_status” in the leak alarm table. Value is “new”, ######
##### then updated to “old” by Carbon once is notification is sent, then,  ######
##### 8 days after the leak has started and IF the leak is still not fixed ######
##### (status =”open”), Suez to update the notification status to “new”    ######
##### again for Carbon to send a reminder on the leak alert.               ######
#################################################################################

leaks_test <- as.data.frame(tbl(con,"leak_alarm")) 
leaks_test[194,11] <- "Open"
leaks_test[194,12] <- "old"
leaks_test[194,6] <- today()-days(8)
leaks_test[194,7] <- NA

leaks_check <- leaks_test %>%
               dplyr::filter(notification_status=="old" & status=="Open") %>%
               dplyr::filter(start_date+days(8)==today()) %>%
               dplyr::mutate(notification_status="new")
if (nrow(leaks_check)>1){
sql_update <- paste("UPDATE leak_alarm SET notification_status = '",leaks_check$notification_status,"' 
                     WHERE id = ",leaks_check$id," AND 
                     service_point_sn = '",leaks_check$service_point_sn,"';",sep="")
sapply(sql_update, function(x){dbSendQuery(mydb, x)})
}

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_LeakSeverity V2 successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)