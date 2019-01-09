# pls add in the DAP a chart with the average LPCD per family size (online customers only),
# you can also remove the extremes if there is not enough values like > 5 or 6 HH members

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr,fst,geosphere)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
          & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))
family_servicepoint_online <- family_servicepoint %>% dplyr::filter(online_status=="ACTIVE") %>%
                              dplyr::select_("service_point_sn","num_house_member")

table(family_servicepoint_online$num_house_member)

Count_FamilyServicePointOnline <- family_servicepoint_online %>%
  dplyr::group_by(num_house_member) %>%
  dplyr::summarise(Count=n())

Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst")
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")

Punggol_All <- rbind(Punggol_2017,Punggol_thisyear)

X <- Punggol_All %>% dplyr::filter(room_type != 'HDBCD' & !(is.na(unit)))

DailyCons <- X %>% group_by(service_point_sn,block,Date,room_type) %>% dplyr::summarise(Consumption=sum(adjusted_consumption,na.rm = TRUE))

DailyCons_FamilyServicePointOnline <- inner_join(DailyCons,family_servicepoint_online,by=c("service_point_sn")) %>%
  dplyr::select_("service_point_sn","Date","Consumption","num_house_member") %>%
  dplyr::mutate(LPCD=round(Consumption/num_house_member)) %>%
  dplyr::group_by(num_house_member) %>%
  dplyr::summarise(AverageLPCD=round(mean(LPCD)))

HHSize_AverageOnlineLPCD_lessThan6 <- inner_join(DailyCons,family_servicepoint_online,by=c("service_point_sn")) %>%
  dplyr::select_("service_point_sn","Date","Consumption","num_house_member") %>%
  dplyr::mutate(greaterthanorequalto6=ifelse(num_house_member>=6,1,0)) %>%
  dplyr::filter(greaterthanorequalto6==0) %>%
  dplyr::mutate(LPCD=round(Consumption/num_house_member)) %>%
  dplyr::group_by(num_house_member) %>%
  dplyr::summarise(AverageLPCD=round(mean(LPCD))) %>% 
  as.data.frame()

DailyCons_FamilyServicePointOnline_greaterthanorequalto6 <- inner_join(DailyCons,family_servicepoint_online,by=c("service_point_sn")) %>%
  dplyr::select_("service_point_sn","Date","Consumption","num_house_member") %>%
  dplyr::mutate(greaterthanorequalto6=ifelse(num_house_member>=6,1,0)) %>%
  dplyr::filter(greaterthanorequalto6==1) %>%
  dplyr::mutate(LPCD=round(Consumption/num_house_member))

HHSize_AverageOnlineLPCD_GreaterThanOrEqualto6 <- data.frame(num_house_member="6 or more",
                                                       AverageLPCD=round(mean(DailyCons_FamilyServicePointOnline_greaterthanorequalto6$LPCD)))

HHSize_AverageOnlineLPCD <- rbind(HHSize_AverageOnlineLPCD_lessThan6,HHSize_AverageOnlineLPCD_GreaterThanOrEqualto6)

write.csv(HHSize_AverageOnlineLPCD,file="/srv/shiny-server/DataAnalyticsPortal/data/HHSize_AverageOnlineLPCD.csv",row.names=FALSE)

Updated_DateTime_OnlineLPCDPerFamilySize <- paste("Last Updated on ",now(),"."," Next Update on ",now()+months(1),".",sep="")
save(HHSize_AverageOnlineLPCD,Updated_DateTime_OnlineLPCDPerFamilySize,file = "/srv/shiny-server/DataAnalyticsPortal/data/OnlineLPCDPerFamilySize.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_OnlineLPCD_PerFamilySize successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)