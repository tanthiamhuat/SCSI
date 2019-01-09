##	Neighbours comparison: 
## 	(a) My comparison (my yesterday lpcd) change everyday
##      i)  Monthly consumption of each customer
##      ii) Quantity of HH members
##      iii) Monthly Occupancy Rate of each customer

rm(list=ls())  # remove all variables
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

consumption_last6months_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last6months_servicepoint.fst",as.data.table=TRUE)
PunggolYuhua <- consumption_last6months_servicepoint[site=="Punggol" | site=="Yuhua"]

PunggolYuhuaSUB <- PunggolYuhua[meter_type=="SUB"]

lastmonth <- month(floor_date(Sys.Date() - months(c(1)), "month"))
thisyear <- year(Sys.Date())
if (lastmonth==12){
 thisyear <- thisyear-1 
}

PunggolYuhuaConsumption <- inner_join(PunggolYuhuaSUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
  group_by(service_point_sn) %>%
  dplyr::filter(date(date_consumption)>=date(move_in_date) & (date(date_consumption)<date(move_out_date) | is.na(move_out_date)))

DailyConsumption <- PunggolYuhuaConsumption %>%
  dplyr::mutate(year=year(date_consumption),month=month(date_consumption),date=date(date_consumption)) %>%
  group_by(service_point_sn,block,year,month,date,num_house_member) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption),n = n()) %>%
  dplyr::filter(service_point_sn !="3101127564" & n==24) # remove Child-Care, full count of 24 per day

X1 <- DailyConsumption %>% group_by(service_point_sn,block,num_house_member) %>%
      dplyr::filter(year==thisyear & month==lastmonth) %>%
      dplyr::summarise(ConsumptionPerMonth=sum(DailyConsumption,na.rm = TRUE))

monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy"))

monthly_occupancy_extracted <- monthly_occupancy %>% 
        dplyr::filter(date(lastupdated)>=(today() %m-% months(1)) & service_point_sn!="3101127564") 
# exclude ChildCare

ComparisonView <- inner_join(X1,monthly_occupancy_extracted,by="service_point_sn") %>%
                  dplyr::mutate(my_average_lpcd=ifelse(ConsumptionPerMonth==0,0,
                                                ifelse(ConsumptionPerMonth!=0,
                                                round(ConsumptionPerMonth/(num_house_member*occupancy_days)),0)),
                                x_litre_challenge=round(my_average_lpcd*0.8)) %>%
                  dplyr::select_("service_point_sn","block","my_average_lpcd","x_litre_challenge") %>%
                  as.data.frame()

ComparisonView$id <- as.integer(rownames(ComparisonView))
ComparisonView$date_created <- rep(today())

ComparisonView <- ComparisonView %>% dplyr::select_("id","block","service_point_sn","my_average_lpcd",
                                                    "x_litre_challenge","date_created")

# dbSendQuery(proddb, "delete from comparison_view")
# dbWriteTable(proddb, "comparison_view", ComparisonView, append=TRUE, row.names=F, overwrite=FALSE)
# dbDisconnect(proddb)

dbSendQuery(mydb, "delete from comparison_view")
dbWriteTable(mydb, "comparison_view", ComparisonView, append=TRUE, row.names=F, overwrite=FALSE)
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ComparisonView_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)
