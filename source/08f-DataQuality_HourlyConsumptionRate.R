local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/HourlyConsumptionRate_last90days.RData'))
HourlyConsumptionRateCSV <- read.csv(paste0(path,'data/HourlyConsumptionRate.csv'))

output$HourlyConsumptionRate <- renderPlotly({
  p <- plot_ly(HourlyConsumptionRate_last90days, x = ~DateTime, y = ~ConsumptionRate,type = 'scatter', mode = 'lines') %>%
    layout(title = "Hourly Consumption Rate (Punggol)", #margin = m,
           xaxis = list(title = "DateTime",tickangle = -17),
           yaxis = list (title = "Hourly Consumption Rate"))
  p
})

output$downloadConsumptionRateData <- downloadHandler(
  filename = function() { paste('HourlyConsumptionRate', '.csv', sep='') },
  content = function(file) {
    write.csv(HourlyConsumptionRateCSV, file, row.names = FALSE)
  }
)

output$ConsumptionRate_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">
                   HourlyConsumptionRate is calculated as below:<br>
                   1) In Punggol, there are a total of 548 meters, including MAIN and SUB meters. <br>
                   2) For each day and hour, the counts of meters are calculated. <br>
                   3) Its HourlyConsumptionRate is calculated as the counts over its expected count of 548.<br>
                   </td>
                   </tr>
                   </table>'))
    )
})

output$lastUpdated_ConsumptionRate <- renderText(Updated_DateTime_DataQualityConsumptionRate)