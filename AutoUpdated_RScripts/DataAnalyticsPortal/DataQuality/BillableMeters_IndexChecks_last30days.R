rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table)

source(paste0(path,'AutoUpdated_RScripts/ToDisconnect.R'))  
source(paste0(path,'AutoUpdated_RScripts/DB_Connections.R'))

last30days <- today()-30
today <- today()
index_last30days <- as.data.table(tbl(con,"index") %>% dplyr::filter(date(current_index_date) >= last30days &
                                                              date(current_index_date) < today))

servicepoint <- as.data.frame(tbl(con,"service_point"))

servicepoint_available <- servicepoint %>% dplyr::select_("id","service_point_sn","site","block","meter_type") %>%
                          dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B") %>%
                          dplyr::filter(site %in% c("Punggol","Tuas","Yuhua"))
colnames(servicepoint_available)[1] <- "id_service_point"

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

BillableMeters <- index_last30days %>% dplyr::filter(id_service_point !='601') %>%  # exclude AHL
                  dplyr::mutate(Date=date(current_index_date)) %>%
                  dplyr::group_by(id_service_point,Date) %>%
                  dplyr::summarise(HourCount=n(),Complete=sum(HourCount>=24), # need to include more than 24 because some with 24 or more readings
                                   NotComplete=sum(HourCount<24 & HourCount>0)) %>%
                  dplyr::group_by(Date) %>%
                  dplyr::summarise(QtyCompleteIndex=sum(Complete),QtyIncompleteIndex=sum(NotComplete),
                                   QtyZeroIndex=nrow(servicepoint_available)-QtyCompleteIndex-QtyIncompleteIndex)

index_tmp <- index_last30days %>%
  dplyr::mutate(Date=date(current_index_date)) %>%
  dplyr::select_("id_service_point","Date") %>% unique() %>%
  dplyr::group_by(Date) %>% as.data.frame()

index_tmp <- inner_join(index_tmp,servicepoint_available,by="id_service_point")

# miss.servicepointsn <- function(df){
#   missing.servicepointsn <- setdiff(servicepoint_available$service_point_sn,df$service_point_sn)
#   return(data.frame(Date=df$Date[1],service_point_sn=missing.servicepointsn))
# }
# 
# missing_service_point_sn <- ddply(.data = index_tmp,.variables = .(Date),.fun = miss.servicepointsn)

missing_service_point_sn <- index_tmp %>% dplyr::group_by(Date) %>%
                            do(anti_join(servicepoint_available, ., 'service_point_sn')) %>%
                            dplyr::arrange(desc(Date))

ZeroIndex_service_point_sn_Count <- missing_service_point_sn %>% 
                                    dplyr::group_by(service_point_sn) %>%
                                    dplyr::mutate(Count=n()) %>%
                                    dplyr::select(-one_of("Date")) %>% ## drop Date column
                                    dplyr::select(-one_of("id_service_point")) %>% ## drop id_service_point column
                                    distinct(service_point_sn, Count, .keep_all = TRUE)

ZeroIndex_service_point_sn_Meter_Count <- inner_join(ZeroIndex_service_point_sn_Count,meter,by=c("service_point_sn"="id_real_estate","meter_type")) %>%
                                          dplyr::select_("service_point_sn","meter_sn","meter_type","site","block","Count")

index_last30days_Count <- index_last30days %>% 
  dplyr::mutate(Date=date(current_index_date)) %>%
  dplyr::group_by(id_service_point,Date) %>%
  dplyr::mutate(Count=n()) %>%
  dplyr::select_("id_service_point","Date","Count") %>%
  distinct(id_service_point,Date,Count, .keep_all = TRUE)

index_last30days_Count_InComplete <- index_last30days_Count %>% dplyr::filter(Count!=24)
Incomplete_service_point_sn <- inner_join(index_last30days_Count_InComplete,servicepoint_available,by="id_service_point") %>%
  as.data.frame() %>% 
  dplyr::select_("Date","service_point_sn","site","block","meter_type")

IncompleteIndex_service_point_sn_Count <- Incomplete_service_point_sn %>% 
  dplyr::group_by(service_point_sn) %>%
  dplyr::mutate(Count=n()) %>%
  dplyr::select(-one_of("Date")) %>% ## drop Date column
  distinct(service_point_sn, Count, .keep_all = TRUE)

IncompleteIndex_service_point_sn_Meter_Count <- inner_join(IncompleteIndex_service_point_sn_Count,meter,by=c("service_point_sn"="id_real_estate","meter_type")) %>%
  dplyr::select_("service_point_sn","meter_sn","meter_type","site","block","Count")

Updated_DateTime_BillableMetersIndexChecks <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(BillableMeters,ZeroIndex_service_point_sn_Meter_Count,IncompleteIndex_service_point_sn_Meter_Count,missing_service_point_sn,
     Updated_DateTime_BillableMetersIndexChecks,
     file=paste0(path,'data/BillableMeters_IndexChecks_last30days.RData'))

time_taken <- proc.time() - ptm
ans <- paste("BillableMeters_IndexChecks_last30days successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file=paste0(path,'data/log_DT.txt'),append=TRUE)