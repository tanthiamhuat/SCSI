rm(list=ls(all=TRUE));invisible(gc());
cat('\014')

ptm <- proc.time()

# import packages
if(!'pacman' %in% installed.packages()[,1]){install.packages('pacman')}
require(pacman)
pacman::p_load(plyr,dplyr,tidyr,ggplot2,lubridate)

setwd("/srv/shiny-server/DataAnalyticsPortal/Profiling_V2")
load("/srv/shiny-server/DataAnalyticsPortal/Profiling_V2/Output/01-Punggol_Indicators.RData")

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE"))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))

family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id")) 

family_servicepoint <- family_servicepoint %>%
  group_by(service_point_sn) %>%
  dplyr::filter(move_in_date==max(move_in_date))  # get the latest family with latest move_in_date

Punggol_lastyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_lastyear.fst")[,1:13]
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")
Punggol_All <- rbind(Punggol_lastyear,Punggol_thisyear)

X <- Punggol_All %>% dplyr::filter(Date >= ymd('2016-03-01'))
X <- X %>% dplyr::filter(room_type != 'HDBCD' & !(is.na(unit)))
X <- X %>% dplyr::filter(Date >=ymd('2016-03-01'))

DailyCons <- X %>% group_by(service_point_sn,block,Date,room_type) %>% dplyr::summarise(Consumption=sum(adjusted_consumption,na.rm = TRUE))

DailyCons_FamilyServicePoint <- inner_join(DailyCons,family_servicepoint,by=c("service_point_sn","block")) %>%
                                dplyr::select_("service_point_sn","block","Date","Consumption","num_house_member") %>%
                                dplyr::mutate(LPCD=round(Consumption/num_house_member))

write.csv(DailyCons_FamilyServicePoint,file="/srv/shiny-server/DataAnalyticsPortal/data/CustomersLPCD.csv")
