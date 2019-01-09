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

flow60 <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/Min60flow.csv")
## above from /srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/Consumption/NonInterpolatedConsumption.R

servicepoint <- as.data.frame(tbl(con,"service_point"))
meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate","meter_type"))

flow60_servicepoint_meter <- inner_join(servicepoint_meter,flow60,by=c("id.x"="id_service_point")) %>% filter(site=="Punggol" & meter_type=="SUB")

leaks_open <- flow60_servicepoint_meter %>% 
  dplyr::filter(MinCons_60 >0) %>%
  group_by(service_point_sn) %>%
  select(service_point_sn,meter_sn,MinCons_60,date) 
leaks_open$date <- as.Date(leaks_open$date)

leaks_close <- flow60_servicepoint_meter %>% 
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

#ListOfLeaks60_Modified$date <- ListOfLeaks60_Modified$date -2 ## to tally with EMIS

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
ListOfLeaks60 <- df_final

leak_alarm_Punggol <- as.data.frame(tbl(con,"leak_alarm")) %>% 
  dplyr::filter(site=="Punggol")

ListOfLeaks5 <- leak_alarm_Punggol %>%
                dplyr::select_("service_point_sn","start_date","end_date","status")
ListOfLeaks5$service_point_sn <- as.character(ListOfLeaks5$service_point_sn)

length(unique(ListOfLeaks60$service_point_sn))
length(unique(ListOfLeaks5$service_point_sn))
ListOfLeaks60_Unique <- unique(ListOfLeaks60$service_point_sn) %>% as.data.frame()
ListOfLeaks5_Unique <-unique(ListOfLeaks5$service_point_sn) %>% as.data.frame()

## to find out how service_point_sn of leaks from Min5 also in Min60 
intersect(unique(ListOfLeaks60$service_point_sn), unique(ListOfLeaks5$service_point_sn))
setdiff(unique(ListOfLeaks60$service_point_sn), unique(ListOfLeaks5$service_point_sn))

setdiff(unique(ListOfLeaks5$service_point_sn),unique(ListOfLeaks60$service_point_sn))

write.csv(ListOfLeaks5,"/srv/shiny-server/DataAnalyticsPortal/data/Leak_Min5.csv")
write.csv(df_final,"/srv/shiny-server/DataAnalyticsPortal/data/Leak_Min60.csv")
