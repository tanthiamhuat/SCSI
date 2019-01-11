local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/BlockWaterSavings.RData'))
BlockWaterSavingsValuesPlot <- BlockWaterSavingsValues[1:5,]

output$BlockSavings_plot<- renderPlotly({
p <- plot_ly(BlockWaterSavingsValuesPlot, x = ~block, y = ~Savings_w_LeakAlarm_Percent, type = 'bar', name = 'Leak Alarm') %>%
     add_trace(y = ~Savings_w_Gamification_Percent, name = 'Gamification') %>%
     layout(yaxis = list(title = 'Percentage (%)'), barmode = 'group', 
            title = "Water Savings") %>%
     config(displayModeBar = F)
p
})

output$BlockSavings_table <-  DT::renderDataTable(
     BlockWaterSavingsValues, options = list(dom='t', paging=FALSE,ordering=FALSE),rownames = FALSE,escape=FALSE
)

output$BlockSavings_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">
                  The water savings due to Leak alarm are calculated by comparing before / after leak alarm activation on 20 April 2016, and before customer engagement on 10 June 2017. <br>
                  Mathematically, the savings are the difference in LPCD values before and after leaks occurred.  <br>
                  The water savings due to Gamification are calculated by comparing before / after App launch on 10 June 2017, and after leak alarm activation on 20 April 2016. <br>
                  Mathematically, the savings are the difference between the average daily LPCD before and after the App launch. <br>
                  (The quantity of households members are being taken care of based PUB’s monthly declaration of residents moving in and out.) <br>

                  Take note the Savings and Consumption values are all in m<sup>3</sup>.
                   </td>
                   </tr>
                   </table>'))
  )
})

output$lastUpdated_BlockSavings <- renderText({Updated_DateTime_BlockWaterSavings})