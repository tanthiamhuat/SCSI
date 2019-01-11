local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

library(gdata)
output$sgp_aws <- renderLeaflet({
  if(input$past_days=='0'){
    load(paste0(path,'data/MapDisplay_last5days.RData'))
  } else {
    load(paste0(path,'data/MapDisplay_last30days.RData'))
  }
  
  PunggolYuhuaTuas_TransmitterReceiver$ReadingRate <- as.numeric(PunggolYuhuaTuas_TransmitterReceiver$ReadingRate)
  
   leafIcons <- icons(
    iconUrl = ifelse(PunggolYuhuaTuas_TransmitterReceiver$Type=="Receiver",
                     "http://52.74.103.158/icon/receiver.png",  # /var/www/html/icon/
              ifelse(PunggolYuhuaTuas_TransmitterReceiver$Type=="Transmitter" &
                     PunggolYuhuaTuas_TransmitterReceiver$ReadingRate >= 90,       
                     "http://52.74.103.158/icon/green_transmitter.png",
              ifelse(PunggolYuhuaTuas_TransmitterReceiver$Type=="Transmitter" &
                     PunggolYuhuaTuas_TransmitterReceiver$ReadingRate >= 70 & 
                     PunggolYuhuaTuas_TransmitterReceiver$ReadingRate < 90,       
                     "http://52.74.103.158/icon/yellow_transmitter.png",
              ifelse(PunggolYuhuaTuas_TransmitterReceiver$Type=="Transmitter" &
                     PunggolYuhuaTuas_TransmitterReceiver$ReadingRate < 70,       
                     "http://52.74.103.158/icon/red_transmitter.png",NA)))),
   
    iconWidth = 20, iconHeight = 20
  )
  
  leaflet(data = PunggolYuhuaTuas_TransmitterReceiver) %>% addTiles() %>%
    addMarkers(~longitude, ~latitude, 
               popup=ifelse(PunggolYuhuaTuas_TransmitterReceiver$Type=="Receiver",
                            paste(PunggolYuhuaTuas_TransmitterReceiver$customer," (",PunggolYuhuaTuas_TransmitterReceiver$ReadingRate,")",sep=""),
                            paste(PunggolYuhuaTuas_TransmitterReceiver$customer," (",PunggolYuhuaTuas_TransmitterReceiver$ReadingRate,"%)",sep="")),
               icon = leafIcons) %>%
               addCircles(lng = 103.9060, lat = 1.400675, radius = 110)
    
})

output$Map_info <- renderUI({
  HTML('<table>
                  <tr>
                  <td> <p align="justify">Click on the individual block/customer to see its Index Reading Rate. 
                                          The percentage range with its corresponding colour code is shown:</p></td>
                  </tr>
                  </table>')
})

output$ReadingRateImage <- renderUI({
  images <- "http://52.74.103.158/icon/ReadingRate.bmp"
  tags$img(src= images)
})

output$MapDisplay_table <- DT::renderDataTable({ 
  if(input$past_days=='0'){
    load(paste0(path,'data/MapDisplay_last5days.RData'))
    MapDisplayTable_PunggolYuhuaTuas_last5days 
  } else {
    load(paste0(path,'data/MapDisplay_last30days.RData'))
    MapDisplayTable_PunggolYuhuaTuas_last30days 
  }
},options = list(dom='t', paging=FALSE,ordering=FALSE),
  rownames = FALSE,escape=FALSE
)

output$MapDisplay_plot<- renderPlotly({
  load(paste0(path,'data/MapDisplay_last5days.RData'))
  load(paste0(path,'data/MapDisplay_last30days.RData'))
  colnames(MapDisplayTable_PunggolYuhuaTuas_last5days)[2]<- "ReadingRate_last5days" 
  colnames(MapDisplayTable_PunggolYuhuaTuas_last30days)[2]<- "ReadingRate_last30days" 
  MapDisplayBarChart_PunggolYuhuaTuas <- cbind(MapDisplayTable_PunggolYuhuaTuas_last5days,MapDisplayTable_PunggolYuhuaTuas_last30days)
  MapDisplayBarChart_PunggolYuhuaTuas[3] <- NULL
  
  MapDisplayBarChart_PunggolYuhuaTuas <- drop.levels(MapDisplayBarChart_PunggolYuhuaTuas)
  
  p <- plot_ly(MapDisplayBarChart_PunggolYuhuaTuas, x = ~block, y = ~ReadingRate_last5days, type = 'bar', name = 'Last 5 days') %>%
    add_trace(y = ~ReadingRate_last30days, name = 'Last 30 days') %>%
    layout(yaxis = list(title = 'Index Reading Rate (%)'), barmode = 'group') %>%
    config(displayModeBar = F)
  p
})

output$lastUpdated_MapDisplay <- renderText(Updated_DateTime_MapDisplay)
