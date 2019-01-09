rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,tidyr,RPostgreSQL,data.table)

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")

## below is for the Monthly LPCD
PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792") %>%  # remove AHL
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
  group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date)>=date(move_in_date) & (date(adjusted_date)<date(move_out_date) | is.na(move_out_date)))

PunggolConsumption <- unique(PunggolConsumption[c("service_point_sn","adjusted_consumption","adjusted_date","num_house_member","room_type")])

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,month,date,room_type) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE),n = n()) %>%
  #dplyr::filter(n==24) %>% # full count of 24 per day
  dplyr::mutate(NS_HHSize=ifelse(room_type %in% c("HDB01","HDB02"),2.16,
                          ifelse(room_type=="HDB03",2.67,
                          ifelse(room_type=="HDB04",3.46,
                          ifelse(room_type=="HDB05",3.84)))))

MonthlyConsumption <- DailyConsumption %>% group_by(service_point_sn,Year,month,NS_HHSize) %>%
  dplyr::summarise(ConsumptionPerMonth=sum(DailyConsumption,na.rm = TRUE))

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/OccupancyRate/AutoUpdated_MonthlyOccupancyRate_V2.R',local = TRUE)

monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy")) %>%
  dplyr::mutate(Year=year(lastupdated),month=month(lastupdated)) 

LPCD_PerMonth <- inner_join(MonthlyConsumption,monthly_occupancy,by=c("service_point_sn","Year","month")) %>%
  group_by(service_point_sn,Year,month) %>%
  dplyr::mutate(LPCD=ifelse(ConsumptionPerMonth==0,0,
                            ifelse(ConsumptionPerMonth!=0,
                                   round(ConsumptionPerMonth/(NS_HHSize*occupancy_days)),0)),
                LPCD_Weighted=LPCD*NS_HHSize) %>%
  group_by(Year,month) %>%
  dplyr::summarise(AverageLPCD=round(sum(LPCD_Weighted,na.rm = TRUE)/sum(NS_HHSize)))

LPCD_PerMonth$month <- factor(month.abb[LPCD_PerMonth$month],levels = month.abb)
LPCD_PerMonth$Year <- as.character(LPCD_PerMonth$Year)
LPCD_PerMonth <- as.data.frame(LPCD_PerMonth)

LPCD_PerMonth <- LPCD_PerMonth[-1,] # remove first row

save(LPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/LPCD_PerMonth.RData")
write.csv(LPCD_PerMonth,file="/srv/shiny-server/DataAnalyticsPortal/data/LPCD_PerMonth.csv")

## below is for Monthly Consumption
MonthlyConsumption <- MonthlyConsumption %>%
                      group_by(Year,month) %>%
                      dplyr::summarise(MonthlyConsumption=sum(ConsumptionPerMonth))

MonthlyConsumption <- MonthlyConsumption[-1,] # remove first row
MonthlyConsumption$month <- factor(month.abb[MonthlyConsumption$month],levels = month.abb)
MonthlyConsumption$Year <- as.character(MonthlyConsumption$Year)
MonthlyConsumption <- as.data.frame(MonthlyConsumption)

MonthlyConsumption_wide <- spread(MonthlyConsumption, Year, MonthlyConsumption)
colnames(MonthlyConsumption_wide)[2] <-"MonthlyConsumption_2016"
colnames(MonthlyConsumption_wide)[3] <-"MonthlyConsumption_2017"

MonthlyConsumption_wide$month <- factor(month.abb[MonthlyConsumption_wide$month],
                                        levels = month.abb)

save(MonthlyConsumption,MonthlyConsumption_wide,file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyConsumption.RData")
write.csv(MonthlyConsumption_wide,file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyConsumption.csv")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyConsumption_LPCD successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)