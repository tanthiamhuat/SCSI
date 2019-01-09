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
#Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

daily_consumption_DB <- as.data.frame(tbl(con,"daily_consumption"))

Punggol_All <- Punggol_All %>% filter(adjusted_date >="2017-01-01")

today <- today()
Punggol_All_Non24 <- Punggol_All %>% 
                     dplyr::group_by(service_point_sn,Date) %>%
                     dplyr::summarize(n=n()) %>%
                     dplyr::filter(n!=24 & Date!=today) %>% as.data.frame()

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
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) %>%
                       dplyr::filter(service_point_sn %in% Punggol_All_Non24$service_point_sn)

# length(unique(PunggolConsumption_SUB$service_point_sn))=533, excluding ChildCare, and including AHL.
PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

DailyConsumptionList <- list()
for (i in 1:nrow(Punggol_All_Non24))
{
  DailyConsumptionList[[i]] <- PunggolConsumption %>%
                               dplyr::mutate(Date=date(adjusted_date)) %>%
                               dplyr::filter(service_point_sn == Punggol_All_Non24[i,1] & 
                                             Date == Punggol_All_Non24[i,2]) %>% 
                               group_by(service_point_sn,Date) %>%
                               dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE)) 
}

DailyConsumption <- as.data.frame(rbindlist(DailyConsumptionList))
colnames(DailyConsumption) <- c("service_point_sn","date_consumption","nett_consumption")

daily_consumption_DB_extracted <- daily_consumption_DB[,c(2,3,5)]
daily_consumption_DB_extracted$date_consumption <- as.Date(daily_consumption_DB_extracted$date_consumption)+days(1)

DailyConsumption_diff <- anti_join(DailyConsumption,daily_consumption_DB_extracted)
if (nrow(DailyConsumption_diff) > 0)
{
  DailyConsumption_diff$overconsumption <- 0
  DailyConsumption_diff$is_leak <- FALSE
  
  number_customers <- length(DailyConsumption_diff$service_point_sn) 
  
  DailyConsumption_diff$id <- as.integer(seq(max(daily_consumption_DB$id)+1,
                                         max(daily_consumption_DB$id)+number_customers,1))
  
                                         
  DailyConsumption_diff <- DailyConsumption_diff[,c(ncol(DailyConsumption_diff),c(1:ncol(DailyConsumption_diff)-1))]

  DailyConsumption_diff <- as.data.frame(DailyConsumption_diff)

  DailyConsumption_diff <- DailyConsumption_diff %>% dplyr::select_("id","service_point_sn","nett_consumption",
                                                            "overconsumption","date_consumption","is_leak")
  ## daily appended table
  dbWriteTable(mydb, "daily_consumption", DailyConsumption_diff, append=TRUE, row.names=F, overwrite=FALSE) # append table
  dbDisconnect(mydb)
}
