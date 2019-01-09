## Accumulative, Appended on Daily basis for DailyLPCD

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,tidyr,RPostgreSQL,data.table,xts,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.RData")

family_ACTIVE <- as.data.frame(tbl(con,"family") %>% 
                 dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & 
                               status=="ACTIVE" & id_service_point!=601)) # exclude AHL
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family_ACTIVE,servicepoint,by=c("id_service_point"="id","room_type")) 

Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last30days.fst")

yesterday <- today()-1

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792") %>%  # remove AHL
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,day,month) %>%
  arrange(adjusted_date)

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 
  # %>%
  # group_by(service_point_sn) %>%
  # dplyr::filter(date(adjusted_date)>=date(move_in_date) & (date(adjusted_date)<date(move_out_date) | is.na(move_out_date)))

PunggolConsumption <- unique(PunggolConsumption[c("service_point_sn","adjusted_consumption","adjusted_date","num_house_member","room_type","block")])

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

DailyLPCD_New <- DailyConsumption %>% dplyr::filter(date==yesterday) %>%
             dplyr::group_by(date) %>%
             dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
             dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH) %>% as.data.frame()

DailyLPCD <- rbind(DailyLPCD,DailyLPCD_New)

DailyLPCD_CSV <- DailyLPCD%>% dplyr::select_("date","DailyLPCD")

DailyLPCD_xts <- xts(DailyLPCD$DailyLPCD,order.by=as.Date(DailyLPCD$date))
colnames(DailyLPCD_xts) <- "DailyLPCD"

DailyLPCD_Block_New <- DailyConsumption %>% dplyr::filter(date==yesterday) %>%
  dplyr::group_by(date,block) %>%
  dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH) %>% as.data.frame()

DailyLPCD_Block <- rbind(DailyLPCD_Block,DailyLPCD_Block_New)

familymembers_Block <- family_ACTIVE %>% dplyr::mutate(block=substr(address,1,5)) %>%  # be careful (for production it is block=substr(address,1,4))
  dplyr::group_by(block) %>%
  dplyr::summarise(TotalBlockMember=sum(num_house_member))

Updated_DateTime_DailyLPCD <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(DailyLPCD,DailyLPCD_xts,DailyLPCD_Block,familymembers_Block,Updated_DateTime_DailyLPCD,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.RData")
write.csv(DailyLPCD_CSV,file="/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.csv",row.names = FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_DailyLPCD successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)