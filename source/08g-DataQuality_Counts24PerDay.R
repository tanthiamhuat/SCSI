local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/Consumption_Count_24_Block_wide.RData'))

Count24PerDay <- as.data.frame(Consumption_Count_24Block_wide)

output$Counts24PerDay_table <-  DT::renderDataTable(Consumption_Count_24Block_wide,filter='bottom',
                                                    #          options = list(scrollX=TRUE,pageLength=10)
                                                   options = list(scrollX=TRUE,scrollY='580px',pageLength=105,dom='t',paging=TRUE,ordering=FALSE),
                                                   rownames = FALSE,escape=FALSE
                                                   )

output$downloadCounts24PerDayData <- downloadHandler(
  filename = function() { paste('Count24PerDay', '.csv', sep='') },
  content = function(file) {
    write.csv(Count24PerDay, file, row.names = FALSE)
  }
)
output$Counts24PerDay_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">

                  The above table shows the percentage of the number of customers having 24 counts of consumption data against its 
                  expected number of customers for each day and for each block. For example, for PG_B1 on a certain day, its expected
                  number of customers having 24 counts is 93. If there is only 90 customers receiving 24 counts on that date, then its
                  percentage is (90/93)*100 = 97% <br><br>

                  2018-04-11 is the date when the non-interpolated consumption begins.

                   </td>
                   </tr>
                   </table>'))
    )
})

output$lastUpdated_Counts24PerDay <- renderText(Updated_DateTime_Counts24PerDay)