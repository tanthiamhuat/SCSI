load("/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption_2017.RData")

RawConsumption_MissingCounts <- RawConsumption_2017 %>% 
  dplyr::mutate(Date=date(ReadingDate)) %>%
  dplyr::filter(is.na(Consumption)) %>%
  dplyr::group_by(Date) %>%
  dplyr::summarise(Count=n())

RawConsumption24DailyCounts <- RawConsumption_2017 %>% 
                               dplyr::mutate(Date=date(ReadingDate)) %>%
                               dplyr::group_by(Date,ExternalMeteringPointReference) %>%
                               dplyr::filter(Date >="2017-10-01") %>%
                               dplyr::summarise(Count=n()) %>%
                               dplyr::filter(Count==24) %>% as.data.frame() %>%
                               dplyr::group_by(Date) %>%
                               dplyr::summarise(MeterCounts=n())

write.csv(RawConsumptionCounts,file="/srv/shiny-server/DataAnalyticsPortal/data/RawConsumptionCounts.csv")

library(plotly)
p <- plot_ly(RawConsumptionCounts, x = ~Date, y = ~Count, type = 'scatter', mode = 'lines')
p

DBConsumption_2017 <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_thisyear.fst")
DBConsumption_2017$date_consumption <- as.POSIXct(DBConsumption_2017$date_consumption, origin="1970-01-01")

DBConsumption24DailyCounts <- DBConsumption_2017 %>% 
  dplyr::mutate(Date=date(date_consumption)) %>%
  dplyr::group_by(Date,id_service_point) %>%
  dplyr::summarise(Count=n()) %>%
  dplyr::filter(Count==24) %>% as.data.frame() %>%
  dplyr::group_by(Date) %>%
  dplyr::summarise(MeterCounts=n())