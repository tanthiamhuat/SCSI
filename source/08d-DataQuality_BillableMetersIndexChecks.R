local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/BillableMeters_IndexChecks_last30days.RData'))

BillableMeters <- as.data.frame(BillableMeters)
missing_service_point_sn <- as.data.frame(missing_service_point_sn)

output$BillableMeters_IndexChecks<- renderPlotly({
  p <- plot_ly(BillableMeters, x = ~Date, y = ~QtyCompleteIndex, type = 'bar', name = 'Qty_CompleteIndex',
               marker = list(color = "green")) %>%
    add_trace(BillableMeters, x = ~Date, y = ~QtyIncompleteIndex, type = 'bar', name = 'Qty_IncompleteIndex',
              marker = list(color = "orange")) %>%
    add_trace(BillableMeters, x = ~Date, y = ~QtyZeroIndex,  type = 'bar', name = 'Qty_ZeroIndex',
              marker = list(color = "red")) %>%
    layout(title = 'Quantity of Meters with Complete, Incomplete and Zero Index on each date.',
           xaxis = list(title = 'Date'),
           yaxis = list(title = 'Quantity of Meters'),  barmode = 'stack',
           legend = list(x = 0, y = -0.30)) %>%
    config(displayModeBar = F)
  p
})

output$downloadBillableMetersData <- downloadHandler(
  filename = function() { paste('BillableMeters', '.csv', sep='') },
  content = function(file) {
    write.csv(BillableMeters, file, row.names = FALSE)
  }
)

output$ZeroIndex_ServicePointSn_text <-  renderText("Below are the service_point_sn with ZeroIndex on each particular date.")

output$ZeroIndex_ServicePointSn_table <-  DT::renderDataTable(missing_service_point_sn,filter='bottom',
                                                              options = list(scrollX=FALSE,pageLength=10))

output$downloadZeroIndexServicePointData <- downloadHandler(
  filename = function() { paste('MissingServicePointSn', '.csv', sep='') },
  content = function(file) {
    write.csv(missing_service_point_sn, file, row.names = FALSE)
  }
)

output$IncompleteIndex_ServicePointSn_text <-  renderText("Below are the service_point_sn with IncompleteIndex, with its Count out of 30 days.
                                                           Count refers to the number of days that meter has incomplete index (less than 24 per day) out of the last 30 days.")
output$IncompleteIndex_ServicePointSn_table <-  DT::renderDataTable(IncompleteIndex_service_point_sn_Meter_Count,filter='bottom',
                                                                    options = list(scrollX=FALSE,pageLength=10))

output$lastUpdated_BillableMeters <- renderText({Updated_DateTime_BillableMetersIndexChecks})