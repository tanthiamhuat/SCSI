gc()
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

days_range <- seq(as.Date("2018-06-12"),as.Date("2018-06-13"),by=1) 

family_ACTIVE <- as.data.frame(tbl(con,"family") %>% 
                 dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & 
                               status=="ACTIVE" & id_service_point!=601)) # exclude AHL
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family_ACTIVE,servicepoint,by=c("id_service_point"="id","room_type")) 

consumption_last6months_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last6months_servicepoint.fst",as.data.table = TRUE)
PunggolConsumption_SUB <- consumption_last6months_servicepoint[site=="Punggol" & meter_type=="SUB" &
                                                               !(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792"]

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
  group_by(service_point_sn) %>%
  dplyr::filter(date(date_consumption)>=date(move_in_date) & (date(date_consumption)<date(move_out_date) | is.na(move_out_date)))

PunggolConsumption <- unique(PunggolConsumption[c("service_point_sn","adjusted_consumption","date_consumption","num_house_member","room_type","block")])

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(date_consumption),date=date(date_consumption),Month=month(date_consumption)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

# for (i in 1:length(days_range))
# {
#   DailyLPCD[[i]] <- DailyConsumption %>% dplyr::filter(date==days_range[i]) %>%
#                     dplyr::group_by(date) %>%
#                     dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
#                     dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH)
# }
# DailyLPCD <- rbindlist(DailyLPCD)

## use lapply to speed up
DailyLPCD_list <- lapply(days_range,function(day){
  return(DailyConsumption %>% 
         dplyr::filter(date==day) %>%
         dplyr::group_by(date) %>%
         dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
         dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH)
  )
})
DailyLPCD <- rbindlist(DailyLPCD_list)

DailyLPCD_xts <- xts(DailyLPCD$DailyLPCD,order.by=as.Date(DailyLPCD$date))
colnames(DailyLPCD_xts) <- "DailyLPCD"
DailyLPCD_CSV <- DailyLPCD%>% dplyr::select_("date","DailyLPCD")

save(DailyLPCD,DailyLPCD_xts,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD_Apr2018.RData")
