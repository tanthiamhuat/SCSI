rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

ptm <- proc.time()

pacman::p_load(RPostgreSQL,dplyr,xlsx,data.table,lubridate,fst)

source(paste0(path,'AutoUpdated_RScripts/ToDisconnect.R'))  
source(paste0(path,'AutoUpdated_RScripts/DB_Connections.R'))

family <- as.data.frame(tbl(con,"family") %>%
                          dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))) %>%
  group_by(id_service_point) %>%
  dplyr::filter(move_in_date==max(move_in_date))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
meter <- as.data.frame(tbl(con,"meter"))
servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) %>% as.data.frame()

Punggol_Occupied <- family_servicepoint %>% dplyr::filter(site=="Punggol" & status=="ACTIVE")
Punggol_Vacant <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id=="EMPTY" & status=="VACANT")
## need to check whether those id_service_point with status=VACANT has also status=ACTIVE which has later move_in_date
## e.g id_service_point=213,222,492

Punggol_Not_Vacant <-  as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(id_service_point %in% Punggol_Vacant$id_service_point) %>%
  dplyr::filter(status=="ACTIVE")
Punggol_Real_Vacant <- setdiff(Punggol_Vacant$id_service_point,Punggol_Not_Vacant$id_service_point)
Punggol_Real_Vacant_ServicePoint_Sn <- servicepoint %>% dplyr::filter(id %in% Punggol_Real_Vacant) %>% dplyr::select_("service_point_sn")

Punggol_2016 <- read.fst(paste0(path,'data/Punggol_2016.fst'))[,1:13]
Punggol_2017 <- read.fst(paste0(path,'data/Punggol_2017.fst'))
Punggol_thisyear <- read.fst(paste0(path,'data/Punggol_thisyear.fst'))
Punggol_pastyears <- rbind(Punggol_2016,Punggol_2017)
Punggol_All <- rbind(Punggol_pastyears,Punggol_thisyear)

PunggolZeroConsumption <- Punggol_All %>% 
                          dplyr::mutate(date=date(date_consumption)) %>%
                          dplyr::select_("service_point_sn","adjusted_consumption","date","block","floor","unit") %>%
                          dplyr::group_by(service_point_sn,block,floor,unit,date) %>%
                          dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm=TRUE)) %>%
                          dplyr::filter(DailyConsumption==0) %>%
                          dplyr::mutate(consecutiveDay = c(NA,diff(date)==1)) %>%
                          dplyr::filter(!is.na(consecutiveDay)) %>%
                          dplyr::mutate(Duration = ave(consecutiveDay, cumsum(consecutiveDay == FALSE), FUN = cumsum)) %>%
                          # cumulative sum that resets when FALSE is encountered
                          dplyr::filter(Duration!=0) %>% 
                          dplyr::mutate(temp=cumsum((Duration==1)*1)) %>%
                          dplyr::group_by(service_point_sn,temp) %>%
                          dplyr::summarise(StartDate=min(date),EndDate=max(date))
PunggolZeroConsumption$temp <- NULL
PunggolZeroConsumption$Duration <- difftime(PunggolZeroConsumption$EndDate,PunggolZeroConsumption$StartDate, units = c("days"))+1

PunggolZeroConsumption <- PunggolZeroConsumption %>%
                          dplyr::mutate(status=ifelse(service_point_sn %in% Punggol_Real_Vacant_ServicePoint_Sn$service_point_sn,
                                               "VACANT","ACTIVE")) %>%
                          dplyr::filter(status!="VACANT") %>% 
                          dplyr::filter(!service_point_sn %in% c("3004521383","3100250274"))
# 3004521383  --> neighbour says he uses as storeroom
# 3100250274  --> gate is chained up
PunggolZeroConsumption$status <- NULL

save(PunggolZeroConsumption,file=paste0(path,'data/PunggolZeroConsumption_StartEndDate.RData'))

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_AllZeroConsumption successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file=paste0(path,'data/log.txt'),append=TRUE)