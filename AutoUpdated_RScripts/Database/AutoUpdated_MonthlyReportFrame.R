rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,plyr,dplyr,data.table,lubridate,stringr,ISOweek)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

#' Report order
#' 0: Online report
#' 1: Leak report
#' 2: high flow AND high usage AND (last_month_average must be higher than last2month average)
#' 3: OverConsumption
#' 4: Generic
#' 
#' TO BE RUN 1 DAY BEFORE THE END OF THE MONTH

# Report to send
Report <- as.data.frame(tbl(con,'monthly_report'))
Report$frame <- 'R4' # by default, receive Generic Report

#' Frame 3: OverConsumption 
# FOR R3
# last2month consumption =0 
# OR 
# lastmonth / last2month > 5 (coefficient),
# =>> then frame format of the report has to be R4
OverC <- Report %>% select(id,service_point_sn,last_month_average,last_2month_average)
OverC <- OverC %>% dplyr::mutate(ratio = last_month_average/last_2month_average)
OverC <- OverC %>% filter(ratio >= 1.5 & ratio < 5) %>% dplyr::filter(last_2month_average!=0)
Report$frame[match(OverC$id,Report$id)] <- 'R3'

#' Frame 2: OverConsumption + (HIGH_FLOW | HIGH_USAGE)
# family <- tbl(con,"family") %>% 
#   dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
# HighFl_Us <- family %>% filter(status =='ACTIVE') %>% filter(high_flow | high_shower_usage)
# service_point <- tbl(con,"service_point")
# HighFl_Us <- inner_join(select(HighFl_Us,id_service_point),select(service_point,id,service_point_sn),by = c('id_service_point' = 'id'))
# HighFl_Us <- as.data.frame(HighFl_Us)
# HighFl_Us$id <- NULL
# HighFl_Us <- inner_join(HighFl_Us,OverC)
# Report$frame[match(HighFl_Us$id,Report$id)] <- 'R2'

#' Frame R2: high flow AND high usage AND (last_month_average must be higher than last2month average)
family <- tbl(con,"family") %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
HighFl_Us <- family %>% filter(status =='ACTIVE') %>% filter(high_flow & high_shower_usage)
service_point <- tbl(con,"service_point")
HighFl_Us <- inner_join(select(HighFl_Us,id_service_point),select(service_point,id,service_point_sn),by = c('id_service_point' = 'id'))
HighFl_Us <- as.data.frame(HighFl_Us)
HighFl_Us$id <- NULL
lastmonthaverage_higher_last2monthaverage <- Report %>% select(id,service_point_sn,last_month_average,last_2month_average) %>%
                                             filter(last_month_average>last_2month_average)
HighFl_Us_lastmonthaverage_higher_last2monthaverage <- inner_join(HighFl_Us,lastmonthaverage_higher_last2monthaverage)

Report$frame[match(HighFl_Us_lastmonthaverage_higher_last2monthaverage$id,Report$id)] <- 'R2'
 
#' Frame 1: Leak
#' leak open when the report is sent
leaks <- tbl(con,"leak_alarm") 
leaks <- leaks %>% filter(status=='Open' & site %in% c("Punggol","Yuhua")) %>% select(service_point_sn)
leaks <- as.data.frame(leaks)
Report$frame[which(Report$service_point_sn %in% leaks$service_point_sn)] <- 'R1'

#' Frame 0: online customers
#' 
family <- tbl(con,"family") %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                & !(room_type %in% c("MAIN","BYPASS","HDBCD")) 
                & online_status == 'ACTIVE')
service_point <- tbl(con,"service_point")
family <- inner_join(select(family,id_service_point),select(service_point,id,service_point_sn),by = c('id_service_point' = 'id'))

online <- as.data.frame(family)
Report$frame[which(Report$service_point_sn %in% online$service_point_sn)] <- 'R0'

sql_update <- paste0("UPDATE monthly_report SET frame = '",Report$frame,"', insert_date = '",Sys.time(),"' WHERE id = '",Report$id ,"'")

sapply(sql_update, function(x){dbSendQuery(mydb, x)})
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyReportFrame successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)