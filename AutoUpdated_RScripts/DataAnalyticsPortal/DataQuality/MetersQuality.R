# Once we have seen the 4 VHF concentrators at full operation for one full week, I think it is time to analyze the rough 5% of VHF 
# readings we still miss every day.
# 
# Could you produce a list of meters which did not reach the 100% in the last 7 days so we can see what we can do with them? 
# A line per meter and number of readings per day would do.

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

last8days <- today()-8
last2days <- today()-2

RawIndex <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/RawIndex.fst",as.data.table=TRUE)
RawConsumption <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/RawConsumption.fst",as.data.table=TRUE)

servicepoint <- as.data.frame(tbl(con,"service_point"))

RawIndexServicePoint <- inner_join(RawIndex,servicepoint,by=c("ExternalMeteringPointReference"="service_point_sn")) %>%
                        dplyr::filter(site %in% c("Punggol","Tuas")) %>%
                        dplyr::select_("ExternalMeteringPointReference","ReadingDate","Index","site")

RawIndex_last7days <- RawIndexServicePoint %>% dplyr::mutate(Date=date(ReadingDate)) %>%
                      dplyr::filter(Date>=last8days & Date<=last2days)

RawIndex_last7days_Details <- RawIndex_last7days %>%
                              dplyr::group_by(ExternalMeteringPointReference,Date,site) %>%
                              dplyr::summarise(Count=n())

RawIndex_last7days_Percentage <- RawIndex_last7days_Details %>% 
                                 dplyr::group_by(ExternalMeteringPointReference,site) %>%
                                 dplyr::summarise(TotalCount=sum(Count)) %>%
                                 dplyr::mutate(Percent=round(TotalCount/(24*7)*100))
                                 
RawConsumptionServicePoint <- inner_join(RawConsumption,servicepoint,by=c("ExternalMeteringPointReference"="service_point_sn")) %>%
  dplyr::filter(site %in% c("Punggol","Tuas")) %>%
  dplyr::select_("ExternalMeteringPointReference","ReadingDate","Consumption","site")

RawConsumption_last7days <- RawConsumptionServicePoint %>% dplyr::mutate(Date=date(ReadingDate)) %>%
  dplyr::filter(Date>=last8days & Date<=last2days)

RawConsumption_last7days_Details <- RawConsumption_last7days %>%
  dplyr::group_by(ExternalMeteringPointReference,Date,site) %>%
  dplyr::summarise(Count=n())

RawConsumption_last7days_Percentage <- RawConsumption_last7days_Details %>% 
  dplyr::group_by(ExternalMeteringPointReference,site) %>%
  dplyr::summarise(TotalCount=sum(Count)) %>%
  dplyr::mutate(Percent=round(TotalCount/(24*7)*100))

write.csv(RawIndex_last7days_Percentage,file="/srv/shiny-server/DataAnalyticsPortal/data/RawIndex_last7days_Percentage.csv")
write.csv(RawIndex_last7days_Details,file="/srv/shiny-server/DataAnalyticsPortal/data/RawIndex_last7days_Details.csv")
write.csv(RawConsumption_last7days_Percentage,file="/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption_last7days_Percentage.csv")
write.csv(RawConsumption_last7days_Details,file="/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption_last7days_Details.csv")
