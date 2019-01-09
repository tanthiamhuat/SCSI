local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'data/DailyHourlyIndexRate_last30days.RData'))

HourlyIndexReadingRate_Punggol_xts <- xts(HourlyIndexReadingRate_Punggol$HourlyIndexReadingRate,HourlyIndexReadingRate_Punggol$date)
HourlyIndexReadingRate_Tuas_xts <- xts(HourlyIndexReadingRate_Tuas$HourlyIndexReadingRate,HourlyIndexReadingRate_Tuas$date)
HourlyIndexReadingRate_Yuhua_xts <- xts(HourlyIndexReadingRate_Yuhua$HourlyIndexReadingRate,HourlyIndexReadingRate_Yuhua$date)
HourlyIndexReadingRate_Site_xts <- cbind(cbind(HourlyIndexReadingRate_Punggol_xts,HourlyIndexReadingRate_Tuas_xts),
                                         HourlyIndexReadingRate_Yuhua_xts)
if (nrow(HourlyIndexReadingRate_Tuas_xts)==0){  ## probably Tuas data is missing for 30 days
  HourlyIndexReadingRate_Site_xts$Tuas <- rep(0,len=30)
}
colnames(HourlyIndexReadingRate_Site_xts) <- c("Punggol","Tuas","Yuhua")
HourlyIndexReadingRatePerArea <- as.data.table(HourlyIndexReadingRate_Site_xts)
colnames(HourlyIndexReadingRatePerArea)[1] <- "Date"

# replace NA with 0
HourlyIndexReadingRate_Site_xts[is.na(HourlyIndexReadingRate_Site_xts)] <- 0

output$IndexRate_Area <- renderDygraph({
  graph <- dygraph(HourlyIndexReadingRate_Site_xts, main = "Hourly Index Rate Per Area") %>%
    dyRangeSelector() %>%
    dyAxis("y",label=HTML('Reading Rate (%)'),valueRange = c(-5, 115)) %>%
    dyOptions(colors = c("#e6194b","#3cb44b","#000000")) 
  graph
})

output$downloadHourlyIndexReadingRatePerAreaData <- downloadHandler(
  filename = function() { paste('HourlyIndexReadingRatePerArea', '.csv', sep='') },
  content = function(file) {
    write.csv(HourlyIndexReadingRatePerArea, file, row.names = FALSE)
  }
)

output$IndexRateArea_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">
                   HourlyIndexReadingRate is calculated as below:<br>
                   1) For each particular day, the HourlyIndexReadingRate is calculated for each meter. <br>
                   If for that particular meter, it has 20 counts of Index, then its HourlyIndexReadingRate
                   for that meter is 20/24=83.33.<br>
                   2) Next, grouping all those meters by date, we calculate the HourlyIndexReadingRate for each day <br>
                   as its mean of all its HourlyIndexReadingRate for each meter for that particular day. <br>
                   3) Its HourlyIndexReadingRate is then extracted for each area in Punggol and Tuas.<br>
               
                   </td>
                   </tr>
                   </table>'))
    )
})

output$lastUpdated_IndexRateArea <- renderText(Updated_DateTime_DataQuality)