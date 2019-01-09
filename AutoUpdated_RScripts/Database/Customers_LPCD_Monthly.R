## Customers LPCD for each month

#rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,RPostgreSQL,lubridate,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

Punggol_2016 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2016.fst")
Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst")
Punggol_All <- rbind(Punggol_2016,Punggol_2017)
Punggol_All <- Punggol_All %>% dplyr::filter(meter_type=="SUB" & service_point_sn!="3100660792")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date) %>%
  arrange(adjusted_date)

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
  group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date)>=date(move_in_date) & (date(adjusted_date)<date(move_out_date) | is.na(move_out_date)))

DailyConsumption <- PunggolConsumption %>%
  dplyr::mutate(year=year(adjusted_date),date=date(adjusted_date),month=month(adjusted_date)) %>%
  group_by(service_point_sn,block,year,date,month,num_house_member) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption),n = n()) %>%
  dplyr::filter(! service_point_sn %in% c("3101127564","3100660792") & n==24) # remove Child-Care and AHL, full count of 24 per day

ConsumptionPerMonth <- DailyConsumption %>% group_by(service_point_sn,block,num_house_member,year,month) %>%
                       dplyr::summarise(ConsumptionPerMonth=sum(DailyConsumption,na.rm = TRUE))

monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy")) %>% 
                     dplyr::mutate(year=year(lastupdated),month=month(lastupdated)) %>%
                     dplyr::filter(!((year==year(today())) & (month==month(today()))))

# Duration April 2016 to May 2017
Customers_LPCD_Before <- inner_join(ConsumptionPerMonth,monthly_occupancy,by=c("service_point_sn","year","month")) %>%
                         dplyr::group_by(service_point_sn,year,month) %>%
                         dplyr::filter(date(lastupdated)<="2017-05-31" & date(lastupdated)>="2016-04-01") %>%
                         dplyr::mutate(my_average_lpcd=ifelse(ConsumptionPerMonth==0,0,
                                                       ifelse(ConsumptionPerMonth!=0,
                                       round(ConsumptionPerMonth/(num_house_member*occupancy_days)),0))
                                ) %>%
                  dplyr::select_("service_point_sn","year","month","my_average_lpcd","num_house_member") %>%
                  as.data.frame()

# June 2017 onwards
Customers_LPCD_After <- inner_join(ConsumptionPerMonth,monthly_occupancy,by=c("service_point_sn","year","month")) %>%
                        dplyr::group_by(service_point_sn,year,month) %>%
                        dplyr::filter(date(lastupdated)>="2017-06-08") %>%
                        dplyr::mutate(my_average_lpcd=ifelse(ConsumptionPerMonth==0,0,
                                                      ifelse(ConsumptionPerMonth!=0,
                                      round(ConsumptionPerMonth/(num_house_member*occupancy_days)),0))
  ) %>%
  dplyr::select_("service_point_sn","year","month","my_average_lpcd","num_house_member") %>%
  as.data.frame()

ConsumptionPerMonth_March2016 <- DailyConsumption %>% group_by(service_point_sn,block,num_house_member,year,month) %>%
                                 dplyr::filter(year==2016 & month==3) %>%
                                 dplyr::summarise(ConsumptionPerMonth=sum(DailyConsumption,na.rm = TRUE))

Customers_March2016 <- inner_join(ConsumptionPerMonth_March2016,monthly_occupancy,by=c("service_point_sn","year","month")) %>%
  dplyr::group_by(service_point_sn,year,month) %>%
  dplyr::mutate(my_average_lpcd=ifelse(ConsumptionPerMonth==0,0,
                                       ifelse(ConsumptionPerMonth!=0,
                                              round(ConsumptionPerMonth/(num_house_member*occupancy_days)),0))
  ) %>%
  dplyr::select_("service_point_sn","year","month","my_average_lpcd","num_house_member") %>%
  as.data.frame()
