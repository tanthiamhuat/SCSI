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

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

flow <- as.data.frame(tbl(con,"flow")) %>% dplyr::mutate(min_5_flow_adjusted=min_5_flow)
servicepoint <- as.data.frame(tbl(con,"service_point"))
meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate","meter_type"))

flow_servicepoint_meter <- inner_join(servicepoint_meter,flow,by=c("id.x"="id_service_point"))

flow_servicepoint_meter_Tuas <- flow_servicepoint_meter %>% 
                                dplyr::filter(site=="Tuas") %>%
                                dplyr::select_("service_point_sn","meter_sn","block","floor","unit","room_type",
                                                "meter_type","min_5_flow","min_5_flow_adjusted","flow_date")

flow_servicepoint_meter_Punggol <- flow_servicepoint_meter %>% 
                                   dplyr::filter(site=="Punggol") %>%
                                   dplyr::select_("service_point_sn","meter_sn","block","floor","unit","room_type",
                                                  "meter_type","min_5_flow","min_5_flow_adjusted","flow_date")

all_blocks <- sort(unique(flow_servicepoint_meter_Punggol$block))
last_block <- all_blocks[length(all_blocks)]

flow_servicepoint_meter_Punggol_function <- function(Block){
  IndirectMainMeters <- servicepoint_meter  %>%
                        dplyr::filter(block==Block & grepl("M|B",service_point_sn) & meter_type!="SUB") %>%
                        dplyr::select_("service_point_sn")
  DirectMainMeters <- servicepoint_meter  %>%
                      dplyr::filter(block==Block & !grepl("M|B",service_point_sn) & meter_type!="SUB") %>%
                      dplyr::select_("service_point_sn")
  flow_servicepoint_meter_Indirect_MAINBYPASS <- flow_servicepoint_meter_Punggol %>% 
                                                 dplyr::filter(block==Block & service_point_sn %in% IndirectMainMeters$service_point_sn)
  
  if (Block==last_block){
      flow_servicepoint_meter_Indirect_SUB <- flow_servicepoint_meter_Punggol %>%  
                                              dplyr::filter(block==Block & meter_type=="SUB" &
                                                            !(floor %in% c("#01","#02","#03","#04","#05")))
  } else {
      flow_servicepoint_meter_Indirect_SUB <- flow_servicepoint_meter_Punggol %>%  
                                              dplyr::filter(block==Block & meter_type=="SUB" &
                                                            !(floor %in% c("#01","#02","#03","#04")))
  }
  ## --------------------------------------------------------------------------------------
  ## List of Leaks for MAIN and BYPASS, need to minus off from their respective SUB meters.
  ## --------------------------------------------------------------------------------------
  flow_servicepoint_meter_Indirect <-rbind(flow_servicepoint_meter_Indirect_MAINBYPASS,flow_servicepoint_meter_Indirect_SUB) %>%
                                     dplyr::arrange(flow_date) %>% 
                                     dplyr::group_by(substr(flow_date,1,10)) %>%
                                     dplyr::mutate(min_5_flow_adjusted=ifelse(meter_type=="MAIN" & min_5_flow > 0,
                                                   min_5_flow[meter_type=="MAIN"]-sum(min_5_flow[meter_type=="SUB"]),
                                                   min_5_flow_adjusted)) %>%
                                     dplyr::mutate(min_5_flow_adjusted=ifelse(meter_type=="BYPASS" & min_5_flow > 0,
                                                   min_5_flow[meter_type=="BYPASS"]-sum(min_5_flow[meter_type=="SUB"]),
                                                   min_5_flow_adjusted)) 
  flow_servicepoint_meter_Direct_MAIN <- flow_servicepoint_meter_Punggol %>% 
                                         dplyr::filter(block==Block & service_point_sn==DirectMainMeters$service_point_sn)
  
  if (Block==last_block){
      flow_servicepoint_meter_Direct_SUB <- flow_servicepoint_meter_Punggol %>% 
                                            dplyr::filter(block==Block & floor %in% c("#02","#03","#04","#05"))
  } else {
      flow_servicepoint_meter_Direct_SUB <- flow_servicepoint_meter_Punggol %>% 
                                            dplyr::filter(block==Block & floor %in% c("#02","#03","#04"))
  }
  
  flow_servicepoint_meter_Direct <- rbind(flow_servicepoint_meter_Direct_MAIN,flow_servicepoint_meter_Direct_SUB) %>%
                                    dplyr::arrange(flow_date) %>% 
                                    dplyr::group_by(substr(flow_date,1,10)) %>%
                                    dplyr::mutate(min_5_flow_adjusted=ifelse(meter_type=="MAIN" & min_5_flow > 0,
                                                  min_5_flow[meter_type=="MAIN"]-sum(min_5_flow[meter_type=="SUB"]),
                                                  min_5_flow_adjusted))
  flow_servicepoint_meter <- rbind(flow_servicepoint_meter_Indirect,flow_servicepoint_meter_Direct) %>%
                             dplyr::arrange(flow_date)
  
  return(flow_servicepoint_meter)
}

for (i in 1:length(all_blocks)){
  assign(paste("flow_servicepoint_meter_",all_blocks[i],sep=""), flow_servicepoint_meter_Punggol_function(all_blocks[i]))
}

flow_servicepoint_meter_Punggol <- rbind(flow_servicepoint_meter_PG_B1,flow_servicepoint_meter_PG_B2,flow_servicepoint_meter_PG_B3,
                                         flow_servicepoint_meter_PG_B4,flow_servicepoint_meter_PG_B5) %>% as.data.frame()
## above need to be modified to be more generic

flow_servicepoint_meter_Punggol[ncol(flow_servicepoint_meter_Punggol)] <- NULL

flow_servicepoint_meter <- rbind(flow_servicepoint_meter_Punggol,flow_servicepoint_meter_Tuas)

leaks_open <- flow_servicepoint_meter %>% 
  dplyr::filter(min_5_flow_adjusted>0) %>%
  dplyr::mutate(date=as.Date(flow_date),Time=substr(flow_date,12,19)) %>%
  group_by(service_point_sn) %>%
  select(service_point_sn,meter_sn,min_5_flow_adjusted,flow_date,date,Time) 

leaks_close <- flow_servicepoint_meter %>% 
  dplyr::filter(min_5_flow_adjusted==0) %>%
  dplyr::mutate(date=as.Date(flow_date),Time=substr(flow_date,12,19)) %>%
  group_by(service_point_sn) %>%
  select(service_point_sn,meter_sn,min_5_flow_adjusted,flow_date,date,Time) 

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
  dplyr::mutate(Min5Flow_GreaterZero = c(min_5_flow_adjusted>0)) %>%
  dplyr::mutate(consecutiveMin5Flow_GreaterZero = ave(Min5Flow_GreaterZero, cumsum(Min5Flow_GreaterZero == FALSE), FUN = cumsum)) %>%
  dplyr::filter(!(Min5Flow_GreaterZero==TRUE & consecutiveMin5Flow_GreaterZero !=1)) %>%
  dplyr::mutate(Min5Flow_EqualZero = c(min_5_flow_adjusted==0)) %>%
  dplyr::mutate(consecutiveMin5Flow_EqualZero = ave(Min5Flow_EqualZero, cumsum(Min5Flow_EqualZero == FALSE), FUN = cumsum)) %>%
  dplyr::filter(!(Min5Flow_EqualZero==TRUE & consecutiveMin5Flow_EqualZero !=1)) %>%
  dplyr::mutate(status=ifelse(min_5_flow_adjusted>0,"Open","Close")) %>%
  dplyr::mutate(row=row_number()) %>%
  # below to remove first close with no open
  dplyr::filter(!(row==1 & min_5_flow_adjusted==0)) %>%
  dplyr::select_("service_point_sn","meter_sn","min_5_flow_adjusted","date","status","row") %>%
  dplyr::mutate(duration=ifelse(status=="Open",0,c(NA,diff(date)+1))) %>%
  dplyr::mutate(lastopen=ifelse(row==max(row) & status=="Open",1,0)) %>%
  dplyr::mutate(duration1=ifelse(lastopen==1,today()-as.Date(date)+1,0)) %>%
  dplyr::mutate(duration=duration+duration1) %>%
  dplyr::select_("service_point_sn","meter_sn","min_5_flow_adjusted","date","status","row","duration","lastopen") 

NotCloseLeaks <- ListOfEffectiveOpenCloseLeaks %>% filter(lastopen==1)
NotCloseLeaksAll <- leaks_open %>% filter(service_point_sn %in% NotCloseLeaks$service_point_sn) %>% 
  group_by(service_point_sn) %>%
  dplyr::filter(date > max(date)-3) %>%  # last 3 days
  dplyr::summarise(min5flow_ave=round(mean(min_5_flow_adjusted),2))

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
  dplyr::filter(row_number()==c(n()) | row_number()==c(n()-1) | row_number()==c(n()-2)) %>% # last 3 days
  dplyr::summarise(min5flow_ave=round(mean(min_5_flow_adjusted),2))

ListOfEffectiveOpenCloseLeaks <- ListOfEffectiveOpenCloseLeaks %>%
  group_by(status,lastopen) %>%
  dplyr::mutate(min5flow_ave1=ifelse(status=="Close",
                                     CloseLeaksAll_Min5Flow_ave$min5flow_ave,0)) %>%
  dplyr::mutate(min5flow_ave=min5flow_ave+min5flow_ave1) %>%
  dplyr::select_("service_point_sn","meter_sn","min_5_flow_adjusted","date","status","duration","min5flow_ave") %>%
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
ListOfEffectiveOpenCloseLeaks_StartEndDates <- ListOfEffectiveOpenCloseLeaks_StartEndDates[,`:=`(lastopen = NULL, min_5_flow_adjusted = NULL, 
                                                                                                 date = NULL, start_date1 = NULL)]

ListOfEffectiveOpenCloseLeaks_StartEndDates <- ListOfEffectiveOpenCloseLeaks_StartEndDates[!is.na(ListOfEffectiveOpenCloseLeaks_StartEndDates$severity),]

LeakSeverity <- ListOfEffectiveOpenCloseLeaks_StartEndDates %>%
  dplyr::select_("service_point_sn","meter_sn","start_date","end_date","duration","min5flow_ave","severity","status")

## leak start date and end date is on first day when min_5_flow_adjusted > and min_5_flow_adjusted=0 respectively.
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

# for status="Open", plus one day to include start_date. status="Close" remains unchanged.
LeakSeverity <- LeakSeverity %>%
                dplyr::mutate(duration=ifelse(status=="Open",duration+1,duration))

save(LeakSeverity, file="/srv/shiny-server/DataAnalyticsPortal/data/LeakSeverity.RData")

## Daily update only those rows in leak_alarm table for status="Open", because values change everyday for status="Open"
LeakSeverity_Open <- LeakSeverity %>% filter(status=="Open")

sql_update1 <- paste("UPDATE leak_alarm SET duration = '",LeakSeverity_Open$duration,"',
                                           min5flow_ave = '",LeakSeverity_Open$min5flow_ave,"',
                                           severity = '",LeakSeverity_Open$severity,"',
                                           status = '",LeakSeverity_Open$status,"',
                                           potential_savings = '",LeakSeverity_Open$potential_savings,"'
                     WHERE start_date = '",LeakSeverity_Open$start_date,"' and
                     service_point_sn = '",LeakSeverity_Open$service_point_sn,"' and
                     meter_sn = '",LeakSeverity_Open$meter_sn,"' ",sep="")
if(nrow(LeakSeverity_Open)>0){
sapply(sql_update1, function(x){dbSendQuery(mydb, x)})
}

## Insert new_leak (append to existing) daily
leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))

leak_changes <- anti_join(LeakSeverity[,c("service_point_sn","meter_sn","meter_type","start_date","end_date","duration")],
                leak_alarm[,c("service_point_sn","meter_sn","meter_type","start_date","end_date","duration")]) 

## update rows which the status changes from Open to Close
leak_changes_Close <- leak_changes %>% dplyr::filter(!is.na(end_date)) # from Open to Close
leak_status_changes <- LeakSeverity %>% 
                       dplyr::filter(service_point_sn %in% leak_changes_Close$service_point_sn) %>%
                       dplyr::group_by(service_point_sn) %>%
                       dplyr::filter(id==max(id))

sql_update2 <- paste("UPDATE leak_alarm SET end_date = '",leak_status_changes$end_date,"',
                                           duration = '",leak_status_changes$duration,"',
                                           min5flow_ave = '",leak_status_changes$min5flow_ave,"',
                                           severity = '",leak_status_changes$severity,"',
                                           status = '",leak_status_changes$status,"',
                                           potential_savings = '",leak_status_changes$potential_savings,"'
                     WHERE start_date = '",leak_status_changes$start_date,"' and
                     service_point_sn = '",leak_status_changes$service_point_sn,"' and
                     meter_sn = '",leak_status_changes$meter_sn,"' ",sep="")
if (nrow(leak_status_changes)>0){
sapply(sql_update2, function(x){dbSendQuery(mydb, x)})
}

new_leak <- leak_changes %>% filter(is.na(end_date))

if (nrow(new_leak)>0) {
  new_leak <- LeakSeverity %>%
              dplyr::filter(service_point_sn %in% new_leak$service_point_sn) %>%
              dplyr::filter(id==max(id)) 
  new_leak$id <- NULL

  new_leak$id <- as.integer(seq(max(leak_alarm$id)+1,
                 max(leak_alarm$id)+nrow(new_leak),1))
  new_leak <- new_leak %>% dplyr::select_("id","service_point_sn","meter_sn","meter_type","site",             
                                          "start_date","end_date","duration","min5flow_ave","severity",         
                                          "status","notification_status","potential_savings")

  ## append with new_leak
  dbWriteTable(mydb, "leak_alarm", new_leak, append=TRUE, row.names=F, overwrite=FALSE)
}

#################################################################################
##### Field "notification_status" in the leak alarm table. Value is "new", ######
##### then updated to "sent" by Carbon once is notification is sent, then, ######
##### 8 days after the leak has started and IF the leak is still not fixed ######
##### (status ="Open"), Suez to update the notification status to "new"    ######
##### again for Carbon to send a reminder on the leak alert.               ######
#################################################################################

# sql_update_test <- paste("Update leak_alarm Set status='Open',notification_status='Sent',
#                                                 start_date='2017-03-01',end_date= NULL, duration='8' where id='206'")
# sapply(sql_update_test, function(x){dbSendQuery(mydb, x)})
# 
# sql_update_reset <- paste("Update leak_alarm set status='Close',notification_status='new', 
#                                                 start_date='2016-08-23',end_date='2016-09-01',duration='10' where id='206'")
# sapply(sql_update_reset, function(x){dbSendQuery(mydb, x)})

leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))

leaks_check <- leak_alarm %>%
  dplyr::filter(notification_status=="sent" & status=="Open") %>%
  dplyr::filter(duration==10) %>%
  dplyr::mutate(notification_status="new")
if (nrow(leaks_check)>0){
  sql_update <- paste("UPDATE leak_alarm SET notification_status = '",leaks_check$notification_status,"' 
                       WHERE id = ",leaks_check$id," AND 
                       service_point_sn = '",leaks_check$service_point_sn,"';",sep="")
  sapply(sql_update, function(x){dbSendQuery(mydb, x)})
}
dbDisconnect(mydb)

### =========== For Data Analytics Portal on List of Leak for site=Punggol only =============
leak_alarm_Punggol_new <- as.data.frame(tbl(con,"leak_alarm")) %>% 
  dplyr::filter(site=="Punggol")

leak_alarm_Punggol_new[c(1,(ncol(leak_alarm_Punggol_new)-1):ncol(leak_alarm_Punggol_new))] <- NULL

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE"))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))

# library(devtools);
# install_github(repo="skranz/dplyrExtras")
# library(dplyrExtras)
## need to manually put in a move-in-date for this service_point_sn 3090005152B
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id")) 
family_servicepoint <- dplyrExtras::mutate_if(family_servicepoint,service_point_sn=="3090005152B",move_in_date=as.Date(Sys.Date()))

family_servicepoint <- family_servicepoint %>%
                       group_by(service_point_sn) %>%
                       dplyr::filter(move_in_date==max(move_in_date))  # get the latest family with latest move_in_date

leak_alarm_Punggol_new <- inner_join(family_servicepoint,leak_alarm_Punggol_new,by=c("service_point_sn","meter_type","site")) %>%
  dplyr::select_("id","service_point_sn","meter_sn","meter_type","site","start_date","end_date",
                 "duration","min5flow_ave","severity","status.y") %>%
  dplyr::rename(family_id=id,status=status.y) %>% arrange(desc(status)) %>%
  dplyr::mutate(Comments="")

#leak_alarm_Punggol <- read.csv2("/srv/shiny-server/DataAnalyticsPortal/data/leak_alarm_Punggol.csv",header=TRUE,sep=",",fill = TRUE,stringsAsFactors=FALSE)
leak_alarm_Punggol <- fread("/srv/shiny-server/DataAnalyticsPortal/data/leak_alarm_Punggol.csv",showProgress = T)

leak_alarm_Punggol$min5flow_ave <- as.numeric(leak_alarm_Punggol$min5flow_ave)
leak_alarm_Punggol$start_date <- as.Date(leak_alarm_Punggol$start_date)
leak_alarm_Punggol$end_date <- as.Date(leak_alarm_Punggol$end_date)

tmp=full_join(leak_alarm_Punggol_new,leak_alarm_Punggol,by=c("family_id","service_point_sn","meter_sn","meter_type","site","start_date"))
tmp1=tmp[,c(1:11,ncol(tmp))]
colnames(tmp1)[7:12] <- c("end_date","duration","min5flow_ave","severity","status","Comments")
leak_alarm_Punggol <- tmp1
save(leak_alarm_Punggol, file="/srv/shiny-server/DataAnalyticsPortal/data/leak_alarm_Punggol.RData")
write.csv(leak_alarm_Punggol,"/srv/shiny-server/DataAnalyticsPortal/data/leak_alarm_Punggol.csv",row.names=FALSE,sep=",")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_Leak_Alarm successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
