rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

last90day <- today()-90
load("/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption.RData")  # from FTP Server
consumption_Raw <- RawConsumption %>%
                   dplyr::mutate(date=date(ReadingDate)) %>%
                   dplyr::filter(date>last90day) %>%
                   select(ExternalMeteringPointReference,Consumption,Index,ReadingDate)
colnames(consumption_Raw) <- c("service_point_sn","consumption_raw","index","ReadingDate")

load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")  # from DB
consumption_DB <- DB_Consumption %>% 
                  dplyr::mutate(date=date(date_consumption)) %>%
                  dplyr::filter(date>last90day) %>%
                  as.data.frame()

service_point <- as.data.frame(tbl(con,"service_point"))
consumption_DB <- inner_join(consumption_DB,service_point,by=c("id_service_point"="id")) %>%
                  dplyr::select_("service_point_sn","interpolated_consumption","index_value","date_consumption") 

## A1) Check for Missing Data (hourly interval not equal to 1) in consumption_Raw
RawConsumption_MissingData <- consumption_Raw %>%
                              arrange(service_point_sn,ReadingDate) %>%
                              group_by(service_point_sn) %>%
                              dplyr::mutate(hr_diff = difftime(ReadingDate, lag(ReadingDate),units="hours")) %>%
                              filter(hr_diff!=1)
RawConsumptionMissingData <- length(unique(RawConsumption_MissingData$service_point_sn))

## A2) Check for Missing Data (hourly interval not equal to 1) in consumption_DB
DBConsumption_MissingData <- consumption_DB %>% 
                             arrange(service_point_sn,date_consumption) %>%
                             group_by(service_point_sn) %>%
                             dplyr::mutate(hr_diff = difftime(date_consumption, lag(date_consumption),units="hours")) %>%
                             filter(hr_diff!=1)
DBConsumptionMissingData <- length(unique(DBConsumption_MissingData$service_point_sn))

## B1) Check for Repeated Data (hourly interval =0) in consumption_Raw
RawConsumption_RepeatedData <- consumption_Raw %>%
                               arrange(service_point_sn,ReadingDate) %>%
                               group_by(service_point_sn) %>%
                               dplyr::mutate(hr_diff = difftime(ReadingDate, lag(ReadingDate),units="hours")) %>%
                               filter(hr_diff==0)
RawConsumptionRepeatedData <- length(unique(RawConsumption_RepeatedData$service_point_sn))

## B2) Check for Repeated Data (hourly interval =0) in consumption_DB
DBConsumption_RepeatedData <- consumption_DB %>%
                            arrange(service_point_sn,date_consumption) %>%
                            group_by(service_point_sn) %>%
                            dplyr::mutate(hr_diff = difftime(date_consumption, lag(date_consumption),units="hours")) %>%
                            filter(hr_diff==0)
DBConsumptionRepeatedData <- length(unique(DBConsumption_RepeatedData$service_point_sn))

## C1) Check for Interpolated Consumption = NA in consumption_Raw
RawConsumption_NA <- consumption_Raw[!complete.cases(consumption_Raw$consumption_raw),]
RawConsumption_NA <- sum(is.na(consumption_Raw$consumption_raw))

## C2) Check for Interpolated Consumption = NA in consumption_DB
DBConsumption_NA <- consumption_DB[!complete.cases(consumption_DB$interpolated_consumption),]
DBConsumption_NA <- sum(is.na(consumption_DB$interpolated_consumption))

## D1)	Check for Inconsistency between Interpolated Index and Interpolated Consumption in consumpotion_Raw
RawConsumptionIndex_Diff <- consumption_Raw %>%
                            arrange(service_point_sn,ReadingDate) %>%
                            group_by(service_point_sn) %>%
                            mutate(index_diff = c(0,diff(index)), interpolated_diff = index_diff - consumption_raw) %>%
                            filter(interpolated_diff > 0 )
RawConsumptionIndex_Inconsistency <- length(RawConsumptionIndex_Diff$interpolated_diff)

## D2)	Check for Inconsistency between Interpolated Index and Interpolated Consumption in consumption_DB
DBConsumptionIndex_Diff <- consumption_DB %>%
                           arrange(service_point_sn,date_consumption) %>%
                           group_by(service_point_sn) %>%
                           mutate(index_diff = c(0,diff(index_value)), interpolated_diff = index_diff - interpolated_consumption) %>%
                           filter(interpolated_diff > 0 )
DBConsumptionIndex_Inconsistency <- length(DBConsumptionIndex_Diff$interpolated_diff)

## E) Comparison Consumption from DB and Raw, based on same service_point_sn and Index
colnames(consumption_DB) <- c("service_point_sn","consumption_DB","index","ReadingDate")
ConsumptionComparison <- inner_join(consumption_Raw,consumption_DB,by=c("service_point_sn","ReadingDate","index"))

ConsumptionComparison <- ConsumptionComparison %>% mutate(ConsumptionNotEqual=ifelse(consumption_raw!=consumption_DB,1,
                                                                              ifelse(consumption_raw==consumption_DB,0,0)))

ConsumptionNotEqual <- ConsumptionComparison %>% filter(ConsumptionNotEqual==1) 
ConsumptionNotEqual <- ConsumptionNotEqual[-ncol(ConsumptionNotEqual)]
ConsumptionNotEqual <- ConsumptionNotEqual %>% 
                       dplyr::mutate(Date=date(ReadingDate),Time=strftime(ReadingDate, format="%H:%M:%S")) %>%
                       dplyr::select_("service_point_sn","index","consumption_raw","consumption_DB","Date","Time")

DB_Raw_Consumption_NotEqual <- nrow(ConsumptionNotEqual)

DataQualityChecks = data.table(ItemDescription=c("RawConsumptionMissingData","RawConsumptionRepeatedData",
                                                 "RawConsumption_NA","RawConsumptionIndex_Inconsistency",
                                                 "DBConsumptionMissingData","DBConsumptionRepeatedData",
                                                 "DBConsumption_NA","DBConsumptionIndex_Inconsistency",
                                                 "DB_Raw_Consumption_NotEqual"),
                               TotalRowAffected=c(RawConsumptionMissingData,RawConsumptionRepeatedData,
                                                  RawConsumption_NA,RawConsumptionIndex_Inconsistency,
                                                  DBConsumptionMissingData,DBConsumptionRepeatedData,
                                                  DBConsumption_NA,DBConsumptionIndex_Inconsistency,
                                                  DB_Raw_Consumption_NotEqual))

DataQualityChecks <- DataQualityChecks %>%
    dplyr::mutate(Status=ifelse(TotalRowAffected >0,"Bad","Good"),
          Responsibility=ifelse(substr(ItemDescription,1,2)=="DB","Suez SG","Ondeo"))

save(DataQualityChecks,ConsumptionNotEqual,file="/srv/shiny-server/DataAnalyticsPortal/data/DataQualityChecks.RData")

time_taken <- proc.time() - ptm
ans <- paste("DataConsistency successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
