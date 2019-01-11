local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/ZeroConsumptionCount.RData'))
ZeroConsumptionCSV <- read.csv2(paste0(path,'data/ZeroConsumptionCount.csv'),header = TRUE,sep=",")
ZeroConsumptionCount$EndDate <- ""
ZeroConsumptionCSV$EndDate <- ""

output$ZeroConsumption_table <-  DT::renderDataTable(ZeroConsumptionCount,filter='bottom',
                                                     options = list(scrollX=FALSE,pageLength=10))

output$downloadZeroConsumptionData <- downloadHandler(
  filename = function() { paste('ZeroConsumption', '.csv', sep='') },
  content = function(file) {
    write.csv(ZeroConsumptionCSV, file, row.names = FALSE)
  }
)

output$ZeroConsumption_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">The above table shows the households with currently 0 consumption, 
                                         while the households are declared with customers (non-vacant). 
                                         Duration refers to the number of consecutive days with zero consumption untill yesterday.
                   </td>
                   </tr>
                   </table>'))
  )
})

output$lastUpdated_ZeroConsumption <- renderText({Updated_DateTime_ZeroConsumption})