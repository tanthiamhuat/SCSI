## Repopulate Monthly Consumption

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,tidyr,RPostgreSQL,data.table,xts,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family_ACTIVE <- as.data.frame(tbl(con,"family") %>% 
                 dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                               & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family_ACTIVE,servicepoint,by=c("id_service_point"="id","room_type")) 

Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_lastyear.fst")

## below is for Monthly Consumption
PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792") %>%  # remove AHL
  dplyr::mutate(day=date(adjusted_date),month=month(adjusted_date)) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

# PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
#   group_by(service_point_sn) %>%
#   dplyr::filter(date(adjusted_date)>=date(move_in_date) & (date(adjusted_date)<date(move_out_date) | is.na(move_out_date)))
# 
# PunggolConsumption <- unique(PunggolConsumption[c("service_point_sn","adjusted_consumption","adjusted_date","num_house_member","room_type","block")])

DailyConsumption <- PunggolConsumption_SUB %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

Monthly_Consumption <- DailyConsumption %>% group_by(Year,Month) %>%
  dplyr::summarise(ConsumptionPerMonth=sum(DailyConsumption,na.rm = TRUE))

Monthly_Consumption_Mar2016 <- Monthly_Consumption %>% filter(Year==2016 & Month==3)
DailyConsumption_Mar2016 <- DailyConsumption %>% filter(Year==2016 & Month==3 & date>="2016-03-15")
family_servicepoint_Mar2016 <- family_servicepoint %>% filter(service_point_sn %in% DailyConsumption_Mar2016$service_point_sn)
total_HH_Mar2016 <- sum(family_servicepoint_Mar2016$num_house_member)

MetersCount <- DailyConsumption_Mar2016 %>% dplyr::group_by(date) %>% dplyr::summarise(Count=n())

LPCD_Mar2016 <- sum(DailyConsumption_Mar2016$DailyConsumption)/(total_HH_Mar2016*17)
