## to compare Consumption and Index count from Raw, AWS and GDC

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/RawIndex.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Index.RData")

Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%
  dplyr::select_("service_point_sn","block","room_type","floor","adjusted_consumption","adjusted_date","day","month") %>%
  arrange(adjusted_date)

family <- as.data.frame(tbl(con,"family")) %>%
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601")
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) %>% filter(site=="Punggol")

# length(unique(PunggolConsumption_SUB$service_point_sn))=533, excluding ChildCare, and excluding AHL.
PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type"))

RawConsumption <- na.omit(RawConsumption)
RawConsumptionFiltered <- RawConsumption %>% 
                          dplyr::filter(date(ReadingDate)>="2017-01-01" & 
                                 ExternalMeteringPointReference %in% unique(PunggolConsumption$service_point_sn)) %>%
                          dplyr::mutate(Date=date(ReadingDate)) %>%
                          dplyr::group_by(Date) %>%
                          dplyr::summarise(RawCount=n(),RawDailyConsumption=sum(Consumption,na.rm=TRUE))

id_service_point_extracted <- servicepoint %>% dplyr::filter(service_point_sn %in% unique(PunggolConsumption$service_point_sn))

AWSConsumptionFiltered <- DB_Consumption %>% 
                          filter(date(date_consumption)>="2017-01-01" & 
                                 id_service_point %in% id_service_point_extracted$id) %>%
                          dplyr::group_by(date(date_consumption)) %>%
                          dplyr::summarise(AWSCount=n(),AWSDailyConsumption=sum(interpolated_consumption,na.rm=TRUE))

RawAWSConsumption <- cbind(RawConsumptionFiltered,AWSConsumptionFiltered[1:nrow(RawConsumptionFiltered),]) 
RawAWSConsumption["date(date_consumption)"] <- NULL
colnames(RawAWSConsumption)[1] <- "Date"

ConsumptionGDC <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/GDC_ConsumptionCounts.csv") ## need to get from GDC

Raw_AWS_GDC_ConsumptionCount <- cbind(RawAWSConsumption[1:nrow(ConsumptionGDC),],ConsumptionGDC[,3:4]) %>%
                                dplyr::mutate(Difference_RawAWS=RawDailyConsumption-AWSDailyConsumption) %>%
                                dplyr::mutate(Difference_RawGDC=RawDailyConsumption-GDCDailyConsumption)
  
save(Raw_AWS_GDC_ConsumptionCount,file = "/srv/shiny-server/DataAnalyticsPortal/data/Raw_AWS_GDC_ConsumptionCount.RData")

RawIndexFiltered <- RawIndex %>% 
                    filter(date(ReadingDate)>="2017-01-01" & 
                           ExternalMeteringPointReference %in% unique(PunggolConsumption$service_point_sn)) %>%
                    dplyr::group_by(date(ReadingDate)) %>%
                    dplyr::summarise(RawCount=n(),RawDailyIndex=sum(as.numeric(Index),na.rm = TRUE))
   
AWSIndexFiltered <- DB_Index %>% 
                    filter(date(current_index_date)>="2017-01-01" & 
                           id_service_point %in% id_service_point_extracted$id) %>%
                    dplyr::group_by(date(current_index_date)) %>%
                    dplyr::summarise(AWSCount=n(),AWSDailyIndex=sum(index,na.rm = TRUE))

RawAWSIndex <- cbind(RawIndexFiltered,AWSIndexFiltered[1:nrow(RawIndexFiltered),]) 
RawAWSIndex["date(current_index_date)"] <- NULL
colnames(RawAWSIndex)[1] <- "Date"

IndexGDC <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/GDC_IndexCounts.csv")  ## need to get from GDC

Raw_AWS_GDC_IndexCount <- cbind(RawAWSIndex[1:nrow(IndexGDC),],IndexGDC[,3:4]) %>%
                          dplyr::mutate(Difference_RawAWS=RawDailyIndex-AWSDailyIndex) %>%
                          dplyr::mutate(Difference_RawGDC=RawDailyIndex-GDCDailyIndex)

Updated_DateTime_CountsDiscrepancies <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(Raw_AWS_GDC_IndexCount,Updated_DateTime_CountsDiscrepancies,
     file = "/srv/shiny-server/DataAnalyticsPortal/data/Raw_AWS_GDC_IndexCount.RData")

## compare flow tables in AWS and GDC
flow_AWS <- tbl(con,"flow") %>% as.data.frame() %>%
            dplyr::mutate(Date=date(flow_date)) %>%
            dplyr::group_by(Date) %>%
            dplyr::summarise(AWS_Count=n())

## from GDC
con_prod <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="proddb")
flow_GDC <- tbl(con_prod,"flow") %>% as.data.frame() %>%
            dplyr::mutate(Date=date(flow_date)) %>%
            dplyr::group_by(Date) %>%
            dplyr::summarise(GDC_Count=n())

flow_AWSGDC <- inner_join(flow_AWS,flow_GDC,by="Date")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_CountsDiscrepancies successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)