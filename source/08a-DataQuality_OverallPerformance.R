local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/DailyHourlyIndexRate_last30days.RData'))

HourlyIndexReadingRate_xts <- xts(HourlyIndexReadingRate$HourlyIndexReadingRate,HourlyIndexReadingRate$date)
DailyIndexReadingRate_xts <- xts(DailyIndexReadingRate$DailyIndexReadingRate,as.Date(DailyIndexReadingRate$date))
DailyHourlyIndexReadingRate_xts <- cbind(DailyIndexReadingRate_xts,HourlyIndexReadingRate_xts)
colnames(DailyHourlyIndexReadingRate_xts) <- c("DailyIndexReadingRate","HourlyIndexReadingRate")
DailyHourlyIndexReadingRate <- as.data.table(DailyHourlyIndexReadingRate_xts)
colnames(DailyHourlyIndexReadingRate)[1] <- "Date"

output$Overall_Performance <- renderDygraph({
   graph <- dygraph(DailyHourlyIndexReadingRate_xts, main = "Overall Performance") %>%
    dyRangeSelector() %>%
    dyAxis("y",label=HTML('Reading Rate (%)'),valueRange=c(0,120))  # valueRange=c(40,113)
   graph
})

output$downloadOverallPerformanceData <- downloadHandler(
  filename = function() { paste('OverallPerformance', '.csv', sep='') },
  content = function(file) {
    write.csv(DailyHourlyIndexReadingRate, file, row.names = FALSE)
  }
)

output$OverallPerformance_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">

                  DailyIndexReadingRate is calculated as below:<br>
                  1) For each particular day, the number of meters available is counted. <br>
                  2) The DailyIndexReadingRate is then calculated as the Total Number of Meters for each day divided by 
                     Total Billable Meters. <br><br>

                  HourlyIndexReadingRate is calculated as below:<br>
                  1) For each particular day, the HourlyIndexReadingRate is calculated for each meter. <br>
                     If for that particular meter, it has 20 counts of Index, then its HourlyIndexReadingRate
                     for that meter is 20/24=83.33.<br>
                  2) Next, grouping all those meters by date, we calculate the HourlyIndexReadingRate for each day <br>
                     as its mean of all its HourlyIndexReadingRate for each meter for that particular day. <br><br>
       
                 Both the DailyIndexReadingRate and HourlyIndexReadingRate take into consideration of both Punggol and Tuas
                 areas as a whole. <br>

                 Take note that those meters under maintenance list are included in the calculation
                 of DailyIndexReadingRate and HourlyIndexReadingRate, which will affect the result negatively.<br><br>

                 </td>
                   </tr>
                   </table>'))
    )
})

output$lastUpdated_OverallPerformance <- renderText(Updated_DateTime_DataQuality)