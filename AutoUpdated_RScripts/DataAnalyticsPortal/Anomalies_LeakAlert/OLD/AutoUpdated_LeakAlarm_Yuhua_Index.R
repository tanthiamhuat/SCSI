rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,gdata,tidyr)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

YH_flow <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/YH_flow.csv")
## above from /srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/Consumption/Yuhua_Index.R

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_YH <- servicepoint %>% filter(site=="Yuhua" & !room_type %in% c("OTHER","NIL") & meter_type=="SUB") 

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

servicepoint_meter_YH <- inner_join(servicepoint_YH,meter,by=c("service_point_sn"="id_real_estate","meter_type"))

YH_flow_servicepoint_meter <- inner_join(servicepoint_meter_YH,YH_flow,by=c("id.x"="id_service_point")) 

leaks_open <- YH_flow_servicepoint_meter %>% 
  dplyr::filter(MinCons_60 >0) %>%
  group_by(service_point_sn) %>%
  select(service_point_sn,meter_sn,MinCons_60,date) 
leaks_open$date <- as.Date(leaks_open$date)

leaks_close <- YH_flow_servicepoint_meter %>% 
  dplyr::filter(MinCons_60==0) %>%
  group_by(service_point_sn) %>%
  select(service_point_sn,meter_sn,MinCons_60,date) 
leaks_close$date <- as.Date(leaks_close$date)

leaks_open <- as.data.frame(leaks_open)
leaks_close <- as.data.frame(leaks_close)

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

ListOfOpenCloseLeaks60 <- rbind(ListOfOpenLeaks,ListOfCloseLeaks) %>%
  arrange(service_point_sn,date) %>%
  dplyr::mutate(status=ifelse(MinCons_60>0,1,0)) %>%
  dplyr::mutate(consecutiveStatus = c(TRUE,diff(status)==1)) %>%
  dplyr::mutate(OpenClose=ifelse(status==1 & consecutiveStatus==TRUE,"Open",
                          ifelse(status==0 & consecutiveStatus==FALSE,"Close",0))) %>%
  dplyr::filter(OpenClose !=0) %>%
  ## filter the first Close only after the Open
  dplyr::mutate(NonStatus=!status) %>%
  dplyr::mutate(consecutiveClose = ave(NonStatus, cumsum(NonStatus == FALSE), FUN = cumsum)) %>%
  dplyr::filter(consecutiveClose==0 | consecutiveClose==1)

ListOfLeaks60 <- ListOfOpenCloseLeaks60 %>%
                 dplyr::select_("service_point_sn","meter_sn","MinCons_60","date","OpenClose")

## to remove the first close (without Open) for each service_point_sn
ListOfLeaks60_Modified <- ListOfLeaks60 %>%
                          dplyr::group_by(service_point_sn) %>%
                          dplyr::mutate(temp=seq(1,n())) %>%
                          dplyr::filter(!(OpenClose=="Close" & temp==1)) %>% as.data.frame()
ListOfLeaks60_Modified["temp"] <- NULL

ListOfLeaks60_Modified$date <- ListOfLeaks60_Modified$date -2 ## to tally with EMIS

df <- arrange(ListOfLeaks60_Modified,meter_sn,date)
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
            dplyr::select_("service_point_sn.x","meter_sn.x","MinCons_60.x","date.x","date.y","OpenClose.y")

df_final <- rename.vars(df_final, from = c("service_point_sn.x","meter_sn.x","MinCons_60.x","date.x","date.y","OpenClose.y"),
                        to = c("service_point_sn","meter_sn","MinCons_60","start_date","end_date","status"))

df_final$status[is.na(df_final$status)] <- "Open"

write.csv(df_final,"/srv/shiny-server/DataAnalyticsPortal/data/Leak_Yuhua.csv",row.names=FALSE)

YH_Open_Severity <- df_final %>% dplyr::filter(status=="Open")
YH_Open_Severity$duration <- as.integer(difftime(today(),YH_Open_Severity$start_date,units="days"))
YH_Open_Severity <- YH_Open_Severity %>%
                    dplyr::mutate(severity=ifelse(duration < 5 & MinCons_60 < 10,"Moderate",
                                           ifelse((duration < 5 & MinCons_60 >=10) | (duration >=5 & MinCons_60 < 10),"Severe",
                                           ifelse(duration >=5 & MinCons_60 >=10,"Critical",NA))))

YH_Open_Severity$service_point_sn <- as.character(YH_Open_Severity$service_point_sn)
YH_Open_Severity_Block <- inner_join(YH_Open_Severity,servicepoint,by="service_point_sn") %>%
  dplyr::select_("service_point_sn","meter_sn","MinCons_60","start_date","end_date","status","duration","severity","block","floor","unit")

write.csv(YH_Open_Severity_Block,file="/srv/shiny-server/DataAnalyticsPortal/data/YH_Leak_Blocks.csv") 
