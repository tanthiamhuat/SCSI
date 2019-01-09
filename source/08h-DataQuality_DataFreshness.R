local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'data/PunggolYuhua_SUB_Freshness.RData'))

h <- hist(PunggolYuhua_SUB_Freshness$Freshness)

output$DataFreshness_plot<- renderPlotly({
  p <- plot_ly(x = h$mids, y = h$counts) %>% 
    add_bars() %>% 
    layout(title = "Histogram for Customers' Freshness",
           xaxis = list(title = "Freshness (hrs)"),
           yaxis = list (title = "Number of Customers")) 
  p
})

output$DataFreshness_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">

                   The histogram plot of data freshness, which include both Punggol and Yuhua (STEE only), is updated on an hourly basis.
                   The histogram measures the time difference (in hours) between what is the time now (hour) and what the latest consumption
                   hour available to the customer.

                   </td>
                   </tr>
                   </table>'))
    )
})

output$lastUpdated_DataFreshness <- renderText(Updated_DateTime_DataFreshness)