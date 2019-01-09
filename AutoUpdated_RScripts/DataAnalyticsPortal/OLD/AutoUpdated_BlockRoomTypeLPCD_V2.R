rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family <- as.data.frame(tbl(con,"family")) %>% dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE")
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))

load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
X <- Punggol_All %>% dplyr::filter(room_type != 'NIL', adjusted_consumption != 'NA') # contain data on sub meter, including childcare

lastmonth <- month(today())-1
if (lastmonth==0) {lastmonth=12}

X1 <- X %>% filter(Date >= ymd('2017-01-01'))%>% filter(room_type!='HDBCD') %>%
      group_by(service_point_sn,block,M,Date,room_type) %>% 
      dplyr::summarise(Consumption = sum(Consumption))
## X1: Consumption for each day

X1$HHsize <- family_servicepoint$num_house_member[match(X1$service_point_sn,family_servicepoint$service_point_sn)]

X2 <- X1 %>% group_by(service_point_sn,block,HHsize,room_type) %>%
      dplyr::filter(M==lastmonth) %>%
      dplyr::summarise(ConsumptionPerMonth=sum(Consumption))

monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy"))
monthly_occupancy_extracted <- monthly_occupancy %>% 
                               dplyr::filter(month(lastupdated)==lastmonth & service_point_sn!="3101127564" &
                                             year(lastupdated)=="2017") 
                               # exclude ChildCare

X2$occupancy_days <- monthly_occupancy_extracted$occupancy_days
X2 <- X2 %>% mutate(LPCD=ifelse(ConsumptionPerMonth==0,0,
                         ifelse(ConsumptionPerMonth!=0,
                                round(ConsumptionPerMonth/(HHsize*occupancy_days)),0)))

BlockLPCD <- X2 %>% group_by(block) %>%
             dplyr::summarise(BlockLPCD=mean(LPCD,na.rm = TRUE))

RoomTypeLPCD <- X2 %>% group_by(room_type) %>%
  dplyr::summarise(RoomTypeLPCD=mean(LPCD,na.rm = TRUE))

save(BlockLPCD,RoomTypeLPCD,file = "/srv/shiny-server/DataAnalyticsPortal/data/BlockRoomTypeLPCD.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_BlockRoomTypeLPCD_V2 successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)