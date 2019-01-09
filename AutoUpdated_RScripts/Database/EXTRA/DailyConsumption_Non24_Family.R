## Even those without 24 counts per day, we still populate into the daily_consumption table
## 3004480369, id_service_point=81, PG_B1#14-83, move_in_date=2017-03-13, this customer not in daily_consumption table
## Repopulate daily_consumption table, start_date 2017-01-01

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))) %>%
  group_by(id_service_point) %>%
  dplyr::filter(move_in_date==max(move_in_date))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

# length(unique(PunggolConsumption_SUB$service_point_sn))=533, excluding ChildCare, and including AHL.
PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

All_Customers <- unique(PunggolConsumption$service_point_sn)

PunggolConsumption_extracted <- PunggolConsumption %>% filter(date(adjusted_date) =="2017-08-13")

Punggol_Complete <- PunggolConsumption_extracted %>% 
                     dplyr::group_by(service_point_sn) %>%
                     dplyr::summarize(n=n()) %>%
                     dplyr::filter(n==24) %>% as.data.frame()

ServicePoint_Incomplete <- as.data.frame(All_Customers[!(All_Customers %in% Punggol_Complete$service_point_sn)==TRUE])
colnames(ServicePoint_Incomplete) <- "service_point_sn"

service_point <- as.data.frame(tbl(con,"service_point"))
family <- as.data.frame(tbl(con,"family"))

Affected_SP <- inner_join(ServicePoint_Incomplete,service_point,by="service_point_sn") %>% 
               dplyr::select_("service_point_sn","id")
Affected_SP_Family <- inner_join(Affected_SP,family,by=c("id"="id_service_point")) %>%
                      dplyr::select_("service_point_sn","address","status","online_status")

AffectedCustomers <- Affected_SP_Family %>% dplyr::filter(status=="ACTIVE",online_status=="ACTIVE")
