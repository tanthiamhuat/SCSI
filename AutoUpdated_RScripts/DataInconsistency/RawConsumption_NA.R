load("/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption.RData")
RawConsumption_NA <- RawConsumption %>% dplyr::filter(is.na(Consumption)) %>%
                     dplyr::mutate(Date=date(ReadingDate)) %>%
                     dplyr::group_by(Date) %>%
                     dplyr::summarise(Count=n()) %>% as.data.frame()

library(plotly)
p <- plot_ly(RawConsumption_NA, x = ~Date, y = ~Count, type = 'scatter', mode = 'lines')
p


x <- c(1:100)
random_y <- rnorm(100, mean = 0)
data <- data.frame(x, random_y)

p <- plot_ly(data, x = ~x, y = ~random_y, type = 'scatter', mode = 'lines')