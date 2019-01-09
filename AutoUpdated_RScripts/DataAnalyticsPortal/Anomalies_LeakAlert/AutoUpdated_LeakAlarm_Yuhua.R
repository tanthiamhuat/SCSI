rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,gdata,tidyr,fst)

source(paste0(path,'AutoUpdated_RScripts/ToDisconnect.R'))  
source(paste0(path,'AutoUpdated_RScripts/DB_Connections.R'))

leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))
meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

#consumption_thisyear_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_thisyear_servicepoint.fst",as.data.table = TRUE)
consumption_thisyear_servicepoint <- read.fst(paste0(path,'data/DT/consumption_thisyear_servicepoint.fst'),as.data.table = TRUE)
Yuhua_SUB <- consumption_thisyear_servicepoint[site=="Yuhua" & meter_type=="SUB" & adjusted_consumption != "NA" & date_consumption >="2018-04-01"] 
## reliable date for YH starts in April 2018

Yuhua_SUB_MinCon <- Yuhua_SUB %>% 
  dplyr::mutate(Date=date(date_consumption)) %>%
  dplyr::group_by(service_point_sn,Date,block) %>%
  dplyr::summarise(MinCons=min(adjusted_consumption,na.rm = TRUE))  

save(Yuhua_SUB_MinCon, file=paste0(path,'data/MinCons_60.RData'))

Yuhua_SUB_MinCon_Meter <- inner_join(Yuhua_SUB_MinCon,meter,by=c("service_point_sn"="id_real_estate")) %>%
                          dplyr::select_("service_point_sn","meter_sn","meter_type","Date","MinCons")

leaks_open <- Yuhua_SUB_MinCon_Meter %>% 
  dplyr::filter(MinCons >0) %>%
  group_by(service_point_sn) %>%
  select(service_point_sn,meter_sn,MinCons,Date) 
leaks_open$Date <- as.Date(leaks_open$Date)

leaks_close <- Yuhua_SUB_MinCon_Meter %>% 
  dplyr::filter(MinCons ==0) %>%
  group_by(service_point_sn) %>%
  select(service_point_sn,meter_sn,MinCons,Date) 
leaks_close$Date <- as.Date(leaks_close$Date)

leaks_open <- as.data.frame(leaks_open)
leaks_close <- as.data.frame(leaks_close)

ListOfOpenLeaks <- leaks_open %>% 
  group_by(service_point_sn) %>%
  arrange(Date) %>%
  dplyr::mutate(consecutiveDay = c(NA,diff(Date)==1)) %>%
  dplyr::filter(consecutiveDay!='NA') %>%
  dplyr::mutate(consecutiveDays = ave(consecutiveDay, cumsum(consecutiveDay == FALSE), FUN = cumsum)) %>%
  # cumulative sum that resets when FALSE is encountered
  dplyr::filter(consecutiveDays==2 & consecutiveDay==TRUE)

ListOfCloseLeaks <- leaks_close %>% 
  group_by(service_point_sn) %>%
  arrange(Date) %>%
  dplyr::mutate(consecutiveDay = c(NA,diff(Date)==1)) %>%
  dplyr::filter(consecutiveDay!='NA' & service_point_sn %in% unique(ListOfOpenLeaks$service_point_sn)) %>%
  dplyr::mutate(consecutiveDays = ave(consecutiveDay, cumsum(consecutiveDay == FALSE), FUN = cumsum)) %>%
  # cumulative sum that resets when FALSE is encountered
  dplyr::filter(consecutiveDays==2 & consecutiveDay==TRUE)

ListOfOpenCloseLeaks <- rbind(ListOfOpenLeaks,ListOfCloseLeaks) %>%
  arrange(service_point_sn,Date) %>%
  dplyr::mutate(status=ifelse(MinCons >0,1,0)) %>%
  dplyr::mutate(consecutiveStatus = c(TRUE,diff(status)==1)) %>%
  dplyr::mutate(OpenClose=ifelse(status==1 & consecutiveStatus==TRUE,"Open",
                          ifelse(status==0 & consecutiveStatus==FALSE,"Close",0))) %>%
  dplyr::filter(OpenClose !=0) %>%
  ## filter the first Close only after the Open
  dplyr::mutate(NonStatus=!status) %>%
  dplyr::mutate(consecutiveClose = ave(NonStatus, cumsum(NonStatus == FALSE), FUN = cumsum)) %>%
  dplyr::filter(consecutiveClose==0 | consecutiveClose==1)

ListOfLeaks <- ListOfOpenCloseLeaks %>%
                 dplyr::select_("service_point_sn","meter_sn","MinCons","Date","OpenClose")

## to remove the first close (without Open) for each service_point_sn
ListOfLeaks_Modified <- ListOfLeaks %>%
                        dplyr::group_by(service_point_sn) %>%
                        dplyr::mutate(temp=seq(1,n())) %>%
                        dplyr::filter(!(OpenClose=="Close" & temp==1)) %>% as.data.frame()
ListOfLeaks_Modified["temp"] <- NULL

ListOfLeaks_Modified$Date <- ListOfLeaks_Modified$Date -2 
## minus 2 days because to get the first day when MinConsumption is greater than zero

df <- arrange(ListOfLeaks_Modified,meter_sn,Date)
df$unique <- 0
distinct_meter <- distinct(df, meter_sn)

for (i in (1:nrow(distinct_meter)))
{
  df$unique[df$meter_sn == distinct_meter$meter_sn[i]] <- c(1:sum(df$meter_sn == distinct_meter$meter_sn[i])) 
}

df$unique[df$unique%%2 == 0] <- df$unique[df$unique%%2 == 0] - 1
df <- unite(df, unique_pair_id, c(meter_sn,unique), remove = FALSE)

df1 <- filter(df, OpenClose == "Open")
df2 <- filter(df, OpenClose == "Close")

df_final <- df1 %>%
            left_join(df2, by = c("unique_pair_id")) %>%
            dplyr::select_("service_point_sn.x","meter_sn.x","MinCons.x","Date.x","Date.y","OpenClose.y")

df_final <- rename.vars(df_final, from = c("service_point_sn.x","meter_sn.x","MinCons.x","Date.x","Date.y","OpenClose.y"),
                        to = c("service_point_sn","meter_sn","MinCons","start_date","end_date","status"))

df_final$status[is.na(df_final$status)] <- "Open"

write.csv(df_final,paste0(path,'data/Leak_Yuhua.csv'),row.names=FALSE)

YH_LeakSeverity <- df_final %>%
                   dplyr::mutate(duration = ifelse(status=="Open",as.integer(difftime(today()-1,start_date,units="days")),
                                                   as.integer(difftime(end_date,start_date,units="days")+1)),
                                 severity=ifelse(duration < 5 & MinCons < 10,"Moderate",
                                          ifelse((duration < 5 & MinCons >=10) | (duration >=5 & MinCons < 10),"Severe",
                                          ifelse(duration >=5 & MinCons >=10,"Critical",NA))),
                                 potential_savings=round(MinCons*24*365*2.56/1000))

YH_LeakSeverity$meter_type <- rep("SUB")
YH_LeakSeverity$site <- rep("Yuhua")
YH_LeakSeverity$notification_status <- rep("new")
colnames(YH_LeakSeverity)[3] <-"min5flow_ave"

YH_LeakSeverity$id <- seq(max(leak_alarm$id)+1,max(leak_alarm$id)+nrow(YH_LeakSeverity),1)

YH_LeakSeverity <- YH_LeakSeverity %>% 
  dplyr::select_("id","service_point_sn","meter_sn","meter_type","site","start_date","end_date",
                 "duration","min5flow_ave","severity","status","notification_status","potential_savings")

## write to leak_alarm table, once only.
# dbWriteTable(mydb, "leak_alarm",YH_LeakSeverity,append=TRUE, row.names=F, overwrite=FALSE) # append table

# for status="Open", plus one day to include start_date. status="Close" remains unchanged.
YH_LeakSeverity <- YH_LeakSeverity %>% dplyr::mutate(duration=ifelse(status=="Open",duration+1,duration))
save(YH_LeakSeverity, file=paste0(path,'data/YH_LeakSeverity.RData'))

## Daily update only those rows in leak_alarm table for status="Open", because values change everyday for status="Open"
YH_LeakSeverity_Open <- YH_LeakSeverity %>% filter(status=="Open")

sql_update1 <- paste("UPDATE leak_alarm SET duration = '",YH_LeakSeverity_Open$duration,"',
                                           min5flow_ave = '",YH_LeakSeverity_Open$min5flow_ave,"',
                                           severity = '",YH_LeakSeverity_Open$severity,"',
                                           status = '",YH_LeakSeverity_Open$status,"',
                                           potential_savings = '",YH_LeakSeverity_Open$potential_savings,"'
                     WHERE start_date = '",YH_LeakSeverity_Open$start_date,"' and
                     service_point_sn = '",YH_LeakSeverity_Open$service_point_sn,"' and
                     meter_sn = '",YH_LeakSeverity_Open$meter_sn,"' ",sep="")
if(nrow(YH_LeakSeverity_Open)>0){
  sapply(sql_update1, function(x){dbSendQuery(mydb, x)})
}

## update rows which the status changes from Open to Close
leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))

YH_leak_changes <- anti_join(YH_LeakSeverity[,c("service_point_sn","meter_sn","meter_type","start_date","end_date","duration")],
                             leak_alarm[,c("service_point_sn","meter_sn","meter_type","start_date","end_date","duration")]) 

YH_leak_changes_Close <- YH_leak_changes %>% dplyr::filter(!is.na(end_date)) # from Open to Close
YH_leak_status_changes <- YH_LeakSeverity %>% 
  dplyr::filter(service_point_sn %in% YH_leak_changes_Close$service_point_sn) %>%
  dplyr::group_by(service_point_sn) %>%
  #dplyr::filter(id==max(id)) %>% as.data.frame()
  dplyr::filter(end_date==max(end_date)) %>% as.data.frame()

sql_update2 <- paste("UPDATE leak_alarm SET end_date = '",YH_leak_status_changes$end_date,"',
                                            duration = '",YH_leak_status_changes$duration,"',
                                            min5flow_ave = '",YH_leak_status_changes$min5flow_ave,"',
                                            severity = '",YH_leak_status_changes$severity,"',
                                            status = '",YH_leak_status_changes$status,"',
                                            potential_savings = '",YH_leak_status_changes$potential_savings,"'
                     WHERE start_date = '",YH_leak_status_changes$start_date,"' and
                     service_point_sn = '",YH_leak_status_changes$service_point_sn,"' and
                     meter_sn = '",YH_leak_status_changes$meter_sn,"' ",sep="")
if (nrow(YH_leak_status_changes)>0){
sapply(sql_update2, function(x){dbSendQuery(mydb, x)})
}

## Insert new_leak (append to existing) daily
YH_new_leak <- YH_leak_changes %>% filter(is.na(end_date))

if (nrow(YH_new_leak)>0) {
  YH_new_leak <- YH_LeakSeverity %>%
              dplyr::filter(service_point_sn %in% YH_new_leak$service_point_sn) %>%
              dplyr::group_by(service_point_sn) %>%
              dplyr::filter(id==max(id) & duration>=3) 
  YH_new_leak$id <- NULL

  YH_new_leak$id <- as.integer(seq(max(leak_alarm$id)+1,
                                   max(leak_alarm$id)+nrow(YH_new_leak),1))
  YH_new_leak <- YH_new_leak %>% dplyr::select_("id","service_point_sn","meter_sn","meter_type","site",             
                                                "start_date","end_date","duration","min5flow_ave","severity",         
                                                "status","notification_status","potential_savings")
  ## append with YH_new_leak
  dbWriteTable(mydb, "leak_alarm", YH_new_leak, append=TRUE, row.names=F, overwrite=FALSE)
}

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_LeakAlarm_Yuhua successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file=paste0(path,'data/log_DT.txt'),append=TRUE)
