rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family_PG <- as.data.frame(tbl(con,"family") %>% 
             dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD","OTHER","NIL")) &
                           id_service_point != '601')) %>% ## exclude AHL
             dplyr::filter(substr(address,1,2)=="PG")

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_PG_servicepoint <- inner_join(family_PG,servicepoint,by=c("id_service_point"="id")) 

Punggol_last6months <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")

X <- Punggol_last6months %>% dplyr::filter(room_type != 'NIL', adjusted_consumption != 'NA') # contain data on sub meter, including childcare

lastmonth <- month(today())-1
if (lastmonth==0) {lastmonth=12}

if (lastmonth==1) {
  last2month=12
} else {
  last2month=lastmonth-1
}

thisyear <- year(today())

X1 <- X %>% filter(Date >= ymd('2016-02-29'))%>% filter(room_type!='HDBCD') %>%
      group_by(service_point_sn,block,M,Y,Date,room_type) %>% 
      dplyr::summarise(Consumption = sum(Consumption))
## X1: Consumption for each day


X1$HHsize <- family_PG_servicepoint$num_house_member[match(X1$service_point_sn,family_PG_servicepoint$service_point_sn)]

X2 <- X1 %>% group_by(service_point_sn,block,HHsize,room_type,Y,M) %>% 
      dplyr::filter(Date>=(today() %m-% months(2))) %>%
      dplyr::summarise(ConsumptionPerMonth=sum(Consumption)) %>%
      dplyr::rename(Month=M)

monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy"))
monthly_occupancy_extracted <- monthly_occupancy %>% 
                               dplyr::filter(date(lastupdated) >(today() %m-% months(2)) &
                                             service_point_sn!="3101127564") %>%
                               dplyr::mutate(Month=month(lastupdated))
                               # exclude ChildCare

X2_monthly_occupancy_extracted <- inner_join(X2,monthly_occupancy_extracted,by=c("service_point_sn","Month"))

X2 <- X2_monthly_occupancy_extracted %>% dplyr::filter(!is.na(HHsize)) %>%
      mutate(LPCD=ifelse(ConsumptionPerMonth==0,0,
                  ifelse(ConsumptionPerMonth!=0,
                         round(ConsumptionPerMonth/(HHsize*occupancy_days)),0))) %>%
      dplyr::filter(!is.infinite(LPCD))


BlockLPCD <- X2 %>% dplyr::group_by(block,Month) %>%
             dplyr::summarise(BlockLPCD=mean(LPCD,na.rm = TRUE)) %>% as.data.frame()
BlockLPCD$Month <- factor(month.abb[BlockLPCD$Month],levels = month.abb)

RoomTypeLPCD <- X2 %>% group_by(room_type,Month) %>%
  dplyr::summarise(RoomTypeLPCD=mean(LPCD,na.rm = TRUE)) %>% as.data.frame()
RoomTypeLPCD$Month <- factor(month.abb[RoomTypeLPCD$Month],levels = month.abb)

Updated_DateTime_BlockNationalAverageLPCD <- paste("Last Updated on ",now(),"."," Next Update on ",now()+months(1),".",sep="")

save(BlockLPCD,RoomTypeLPCD,Updated_DateTime_BlockNationalAverageLPCD,file = "/srv/shiny-server/DataAnalyticsPortal/data/BlockRoomTypeLPCD.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_BlockRoomTypeLPCD successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)