rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,dplyr,xlsx,data.table,lubridate,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family <- as.data.frame(tbl(con,"family") %>%
                          dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))) %>%
  group_by(id_service_point) %>%
  dplyr::filter(move_in_date==max(move_in_date))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
meter <- as.data.frame(tbl(con,"meter"))
servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) %>% as.data.frame()

Punggol_Occupied <- family_servicepoint %>% dplyr::filter(site=="Punggol" & status=="ACTIVE")
Punggol_Vacant <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id=="EMPTY" & status=="VACANT")
## need to check whether those id_service_point with status=VACANT has also status=ACTIVE which has later move_in_date
## e.g id_service_point=213,222,492

Punggol_Not_Vacant <-  as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(id_service_point %in% Punggol_Vacant$id_service_point) %>%
  dplyr::filter(status=="ACTIVE")
Punggol_Real_Vacant <- setdiff(Punggol_Vacant$id_service_point,Punggol_Not_Vacant$id_service_point)
Punggol_Real_Vacant_ServicePoint_Sn <- servicepoint %>% dplyr::filter(id %in% Punggol_Real_Vacant) %>% dplyr::select_("service_point_sn")

DB_Consumption_last6months <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_last6months.fst")

DB_Consumption_last6months$adjusted_date <- DB_Consumption_last6months$date_consumption-lubridate::hours(1)

# Count number of Consecutive Days of Zero Consumption for the past 6 months
DB_Consumption_last6months_ServicePoint <- inner_join(DB_Consumption_last6months,servicepoint,by=c("id_service_point"="id")) %>%
  dplyr::select_("service_point_sn","adjusted_consumption","index_value","date_consumption") %>%
  dplyr::mutate(Date=date(date_consumption)) %>%
  dplyr::select_("service_point_sn","adjusted_consumption","Date") %>%
  dplyr::group_by(service_point_sn,Date) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE)) %>%
  dplyr::filter(DailyConsumption==0) %>%
  dplyr::mutate(consecutiveDay = c(NA,diff(Date)==1)) %>%
  dplyr::filter(!is.na(consecutiveDay)) %>%
  dplyr::mutate(Duration = ave(consecutiveDay, cumsum(consecutiveDay == FALSE), FUN = cumsum)) %>%
  # cumulative sum that resets when FALSE is encountered
  dplyr::filter(Duration!=0) %>%
  dplyr::mutate(temp=cumsum((Duration==1)*1)) %>%
  dplyr::group_by(service_point_sn,temp) %>% 
  dplyr::summarise(StartDate=min(Date),EndDate=max(Date)) %>%
  dplyr::select_("service_point_sn","StartDate","EndDate")

DB_Consumption_last6months_ServicePoint$temp <- NULL
DB_Consumption_last6months_ServicePoint$Duration <- difftime(DB_Consumption_last6months_ServicePoint$EndDate,
                                                             DB_Consumption_last6months_ServicePoint$StartDate, units = c("days"))+1

ZeroConsumption <- servicepoint_meter[match(DB_Consumption_last6months_ServicePoint$service_point_sn,
                                            servicepoint_meter$service_point_sn),] %>%
                   dplyr::select_("service_point_sn","meter_sn","block","floor","unit","site") 

ZeroConsumptionCount <- inner_join(ZeroConsumption,DB_Consumption_last6months_ServicePoint,by="service_point_sn") %>% 
                        dplyr::filter(!(service_point_sn %in% Punggol_Real_Vacant_ServicePoint_Sn$service_point_sn)) %>%
                        dplyr::filter(Duration>=30) %>%
                        dplyr::arrange(desc(Duration)) %>% unique()

#ZeroConsumptionCount <- ZeroConsumptionCount %>% mutate(Duration=replace(Duration, Duration==179, ">179")) 

family_All <- as.data.frame(tbl(con,"family") %>%
                          dplyr::filter(pub_cust_id!="EMPTY"))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
familyAll_servicepoint <- inner_join(family_All,servicepoint,by=c("id_service_point"="id")) %>% as.data.frame()

Yuhua_Vacant <- familyAll_servicepoint %>% dplyr::filter(site=="Yuhua" & status=="VACANT")

ZeroConsumptionCount <- inner_join(ZeroConsumptionCount,familyAll_servicepoint,by=c("service_point_sn","block","site","floor","unit")) %>%
                        dplyr::select_("service_point_sn","meter_sn","block","floor","unit","site","StartDate","EndDate","Duration","online_status") %>%
                        dplyr::filter(online_status!='DEACTIVATED' & !(service_point_sn %in% Yuhua_Vacant$service_point_sn))

ZeroConsumptionCount <- unique(ZeroConsumptionCount)

ZeroConsumptionCount <- ZeroConsumptionCount %>% dplyr::filter(EndDate==today())

## to detect suspicion meter based on weekly net consumption
load("/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/WeeklyNetConsumption.RData")
WeeklyNetConsumption_Zero <- inner_join(WeeklyPunggolNetConsumptionDetails_NA,ZeroConsumptionCount,by=c("service_point_sn")) %>%
  dplyr::select_("service_point_sn","meter_type","supply","week","LastDayofWeek","WeeklyConsumption","NetConsumption","Duration","StartDate","EndDate") %>%
  unique() 

WeeklyNetConsumptionZero <- WeeklyNetConsumption_Zero %>%
  dplyr::group_by(service_point_sn,Duration) %>%
  dplyr::mutate(StartDate_DiffDays=difftime(LastDayofWeek,StartDate,units = "days")) %>%
  dplyr::mutate(EndDate_DiffDays=difftime(EndDate,LastDayofWeek,units = "days")) %>% as.data.frame() 

WeeklyNetConsumptionZero$NetConsumption <- as.numeric(WeeklyNetConsumptionZero$NetConsumption)

## find that average weekly consumption before it stops.
## NetConsumptionIncrease must be more tahan Average Weekly Consumption before it stops
WeeklyAvgNetConsumption <- WeeklyNetConsumptionZero %>% dplyr::group_by(service_point_sn) %>%
  dplyr::summarise(AvgNetConsumption1=round(mean(NetConsumption[WeeklyConsumption==0],na.rm = TRUE)),
                   AvgNetConsumption2=round(mean(NetConsumption[WeeklyConsumption!=0],na.rm = TRUE)),
                   NetConsumptionIncrease=AvgNetConsumption1-AvgNetConsumption2,
                   AverageWeeklyConsumption=round(mean(WeeklyConsumption[WeeklyConsumption!=0],na.rm = TRUE)))

#WeeklyAvgNetConsumption <-na.omit(WeeklyAvgNetConsumption)
WeeklyAvgNetConsumption <- tidyr::replace_na(WeeklyAvgNetConsumption, list(AvgNetConsumption2=0, AverageWeeklyConsumption=0)) # replace NA with 0
WeeklyAvgNetConsumption$NetConsumptionIncrease <- WeeklyAvgNetConsumption$AvgNetConsumption1 - WeeklyAvgNetConsumption$AvgNetConsumption2
  
WeeklyAvgNetConsumption <- WeeklyAvgNetConsumption %>% dplyr::filter(NetConsumptionIncrease>AverageWeeklyConsumption)

WeeklyNetConsumptionZero_Count <- WeeklyNetConsumptionZero %>% 
  dplyr::filter(StartDate_DiffDays>=0 & EndDate_DiffDays>=0) %>%
  dplyr::filter(WeeklyConsumption==0) %>%
  dplyr::group_by(service_point_sn,StartDate,EndDate,Duration) %>%
  dplyr::summarise(WeekCount=n())

SuspicionMeters <- inner_join(WeeklyNetConsumptionZero_Count,WeeklyAvgNetConsumption,by="service_point_sn") %>% 
  dplyr::filter(WeekCount>=10) %>% dplyr::arrange(desc(WeekCount)) %>% 
  dplyr::select_("service_point_sn","StartDate","EndDate","Duration","WeekCount","NetConsumptionIncrease","AverageWeeklyConsumption")

ZeroConsumptionCount <- ZeroConsumptionCount %>% dplyr::mutate(Comments=ifelse(service_point_sn=="3004521383","neighbour says he uses as storeroom",
                                                                        ifelse(service_point_sn=="3100250274","gate is chained up",
                                                                        ifelse(service_point_sn %in% SuspicionMeters$service_point_sn,
                                                                               "Meter is suspected to be blocked.",""))))

PunggolZeroConsumption_Sn <- ZeroConsumptionCount %>% dplyr::filter(site=="Punggol") %>% dplyr::select_("service_point_sn")
ServicePointSn_Supply<- WeeklyNetConsumptionZero %>% 
                        dplyr::filter(service_point_sn %in% PunggolZeroConsumption_Sn$service_point_sn) %>%
                        dplyr::select_("service_point_sn","supply") %>% unique()

ZeroConsumptionCount <- full_join(ZeroConsumptionCount,ServicePointSn_Supply,by="service_point_sn")
ZeroConsumptionCount <- ZeroConsumptionCount %>%
                        dplyr::select_("service_point_sn","meter_sn","block","supply","floor","unit","site","StartDate",
                                        "EndDate","Duration","online_status","Comments")          

Updated_DateTime_ZeroConsumption <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(ZeroConsumptionCount,Updated_DateTime_ZeroConsumption,file="/srv/shiny-server/DataAnalyticsPortal/data/ZeroConsumptionCount.RData")
write.csv(ZeroConsumptionCount,"/srv/shiny-server/DataAnalyticsPortal/data/ZeroConsumptionCount.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ZeroConsumption successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)