local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/UnexpectedConsumption.RData'))

output$UnexpectedConsumption_table <-  DT::renderDataTable(unexpected_consumption,rownames=FALSE, 
                                                         options = list(scrollX=FALSE,pageLength=10,
                                                                        bFilter=0,searching=FALSE,
                                                                        autoWidth = TRUE,
                                                                        columnDefs = list(list(width = '50px', targets = "_all"))
                                                         ))

output$UnexpectedConsumption_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">The households above are declared "vacant" (with no customer) but a consumption has been recorded.<br><br>
                   1 day is considered unoccupied if no consumption has been detected throughout the day. 
                   For example, if 4 days over 5 are occupied, the daily occupancy rate is 4/5 = 80%.
                   occupancy_rate is its daily occupancy rate over the last 5 days.
                   </td>
                   </tr>
                   </table>'))
  )
})

output$lastUpdated_UnexpectedConsumption <- renderText({Updated_DateTime_UnexpectedConsumption})