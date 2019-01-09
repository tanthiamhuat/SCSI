local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'data/overconsumption.RData'))
OverConsumptionCSV <- read.csv2(paste0(path,'data/overconsumption.csv'),header = TRUE,sep=",")

output$OverConsumption_table <-  DT::renderDataTable(family_servicepoint_overconsumption_DAP,filter='bottom',
                                                 options = list(scrollX=FALSE,pageLength=10))

output$downloadOverConsumptionData <- downloadHandler(
  filename = function() { paste('OverConsumption', '.csv', sep='') },
  content = function(file) {
    write.csv(OverConsumptionCSV, file, row.names = FALSE)
  }
)

output$OverConsumption_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">The overconsumption is triggered if the average daily consumption of the past 7 days is >50% 
                                         higher compared to the last 3 months.
                   </td>
                   </tr>
                   </table>'))
)
})

output$lastUpdated_OverConsumption <- renderText({Updated_DateTime_OverConsumption_Alarm})