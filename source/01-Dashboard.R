local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/Dashboard.RData'))
load(paste0(path,'data/MapDisplay_last30days.RData'))
load(paste0(path,'data/GlobalWaterSavings.RData'))

## -------- Total Consumption ------------- ##
output$TotalWeeklyConsumption_plot<- renderPlotly({
  p <- plot_ly(weekly_consumption_last10weeks, x = ~week_number, y = ~WeeklyConsumption,type = 'scatter', mode = 'lines') %>%
       layout(title = "Total Weekly Consumption over last 10 weeks", #margin = m,
              xaxis = list(title = "Year_Weeknumber",tickangle = -17),
              yaxis = list (title = "Total Weekly Consumption (m<sup>3</sup>)",
                            range = c(min(weekly_consumption_last10weeks$WeeklyConsumption)-500,
                                      max(weekly_consumption_last10weeks$WeeklyConsumption)+500))) %>%
              config(displayModeBar = F)
  p
})

## -------- Leaks and Anomalies ------------- ##
output$TotalDailyLeak_plot<- renderPlotly({
  p <- plot_ly(LeakVolumePerDay_last10days, x = ~Date1, y = ~TotalLeakVolumePerDay,type = 'scatter', mode = 'lines') %>%
    layout(title = "Total Leak Volume Per Day over last 10 days",
           xaxis = list(title = "Date",tickangle = -15),
           yaxis = list (title = "Leak (Litres)",
                         range = c(round(min(LeakVolumePerDay_last10days$TotalLeakVolumePerDay)*0.9),
                                   round(max(LeakVolumePerDay_last10days$TotalLeakVolumePerDay)*1.1)))) %>%
    config(displayModeBar = F)
  p
})

output$leaktrend_info <- renderUI({
  ImageTrend <- ifelse(LeakInformation_DT$Trend=='Decreasing','Down.png',
                ifelse(LeakInformation_DT$Trend=='Increasing','Up.png','Equal.png'))
  fluidRow(column(12,
                  fluidRow(column(2,HTML(paste('<font size = "4"><b>',LeakInformation_DT$Values[1],'</b></font>')), align = 'right'),
                           column(7,HTML(paste('<font size = "4">',"open leaks",'</font>')), align = 'left'),
                           column(3,img(src=ImageTrend[1],width = '25%',height = '25%'))),
                  fluidRow(column(2,HTML(paste('<font size = "4"><b>',LeakInformation_DT$Values[2],'</b></font>')), align = 'right'),
                           column(7,HTML(paste('<font size = "4">',"litres/day of leaks",'</font>')), align = 'left'),
                           column(3,img(src=ImageTrend[2],width = '25%',height = '25%'))),
                  fluidRow(column(2,HTML(paste('<font size = "4"><b>',LeakInformation_DT$Values[3],'</b></font>')), align = 'right'),
                           column(7,HTML(paste('<font size = "4">',"days (average leak duration/meter)",'</font>')), align = 'left'),
                           column(3,img(src=ImageTrend[3],width = '25%',height = '25%')))
  ))
})

output$OverconsumptionAlarm_plot<- renderPlotly({
  p <- plot_ly(x = overconsumption_alarm_past10weeks$Count, y = overconsumption_alarm_past10weeks$week, type = 'bar', orientation = 'h') %>%
    layout(title = "Overconsumption Alarms"#,
         #  xaxis = list(title = "Number of Overconsumption Alarm"), yaxis = list (title = "Year_Weeknumber")
         ) %>%
    config(displayModeBar = F)
  p
})

output$netconsumption_info <- renderUI({
 # if('1' %in% input$site_show){
    ImageColorDirect <- ifelse(PunggolNetConsumptionOutput_DirectIndirect$Direct=='Green','green.png',
                        ifelse(PunggolNetConsumptionOutput_DirectIndirect$Direct=='Orange','orange.png',
                        ifelse(PunggolNetConsumptionOutput_DirectIndirect$Direct=='Red','red.png',
                        ifelse(PunggolNetConsumptionOutput_DirectIndirect$Direct=='Grey','grey.png',0))))
    ImageColorIndirect <- ifelse(PunggolNetConsumptionOutput_DirectIndirect$Indirect=='Green','green.png',
                          ifelse(PunggolNetConsumptionOutput_DirectIndirect$Indirect=='Orange','orange.png',
                          ifelse(PunggolNetConsumptionOutput_DirectIndirect$Indirect=='Red','red.png',
                          ifelse(PunggolNetConsumptionOutput_DirectIndirect$Indirect=='Grey','grey.png',0))))
    fluidRow(column(12,
                    fluidRow(column(12,HTML(paste('<font size = "4">',"Net Consumption",'</font>')), align = 'center')),
                    fluidRow(column(4,HTML(paste('<font size = "4"><b>',"Block",'</b></font>')), align = 'center'),
                             column(4,HTML(paste('<font size = "4"><b>',"Direct",'</b></font>')), align = 'center'),
                             column(4,HTML(paste('<font size = "4"><b>',"Indirect",'</b></font>')), align = 'center')),
                    fluidRow(column(4,HTML(paste('<font size = "4">',PunggolNetConsumptionOutput_DirectIndirect$Block[1],'</font>')),align='center'),
                             column(4,img(src=ImageColorDirect[1],width = '20%',height = '20%'),align='center'),
                             column(4,img(src=ImageColorIndirect[1],width = '20%',height = '20%'),align='center')),
                    fluidRow(column(4,HTML(paste('<font size = "4">',PunggolNetConsumptionOutput_DirectIndirect$Block[2],'</font>')),align='center'),
                             column(4,img(src=ImageColorDirect[2],width = '20%',height = '20%'),align='center'),
                             column(4,img(src=ImageColorIndirect[2],width = '20%',height = '20%'),align='center')),
                    fluidRow(column(4,HTML(paste('<font size = "4">',PunggolNetConsumptionOutput_DirectIndirect$Block[3],'</font>')),align='center'),
                             column(4,img(src=ImageColorDirect[3],width = '20%',height = '20%'),align='center'),
                             column(4,img(src=ImageColorIndirect[3],width = '20%',height = '20%'),align='center')),
                    fluidRow(column(4,HTML(paste('<font size = "4">',PunggolNetConsumptionOutput_DirectIndirect$Block[4],'</font>')),align='center'),
                             column(4,img(src=ImageColorDirect[4],width = '20%',height = '20%'),align='center'),
                             column(4,img(src=ImageColorIndirect[4],width = '20%',height = '20%'),align='center')),
                    fluidRow(column(4,HTML(paste('<font size = "4">',PunggolNetConsumptionOutput_DirectIndirect$Block[5],'</font>')),align='center'),
                             column(4,img(src=ImageColorDirect[5],width = '20%',height = '20%'),align='center'),
                             column(4,img(src=ImageColorIndirect[5],width = '20%',height = '20%'),align='center'))
    ))
  # } else {
  #   ImageColorDirect <- ifelse(YuhuaNetConsumptionOutput_DirectIndirect$Direct=='Green' ,'green.png',
  #                       ifelse(YuhuaNetConsumptionOutput_DirectIndirect$Direct=='Orange','orange.png',
  #                       ifelse(YuhuaNetConsumptionOutput_DirectIndirect$Direct=='Red','red.png',
  #                       ifelse(YuhuaNetConsumptionOutput_DirectIndirect$Direct=='Grey','grey.png',0))))
  #   ImageColorIndirect <- ifelse(YuhuaNetConsumptionOutput_DirectIndirect$Indirect=='Green','green.png',
  #                         ifelse(YuhuaNetConsumptionOutput_DirectIndirect$Indirect=='Orange','orange.png',
  #                         ifelse(YuhuaNetConsumptionOutput_DirectIndirect$Indirect=='Red','red.png',
  #                         ifelse(YuhuaNetConsumptionOutput_DirectIndirect$Indirect=='Grey','grey.png',0))))
    
    # fluidRow(column(12,
    #                 fluidRow(column(12,HTML(paste('<font size = "4">',"Net Consumption",'</font>')), align = 'center')),
    #                 fluidRow(column(4,HTML(paste('<font size = "4"><b>',"Block",'</b></font>')), align = 'center'),
    #                          column(4,HTML(paste('<font size = "4"><b>',"Direct",'</b></font>')), align = 'center'),
    #                          column(4,HTML(paste('<font size = "4"><b>',"Indirect",'</b></font>')), align = 'center')),
    #                 fluidRow(column(4,HTML(paste('<font size = "3">',YuhuaNetConsumptionOutput_DirectIndirect$Block[1],'</font>')),align='center'),
    #                          column(4,img(src=ImageColorDirect[1],width = '20%',height = '20%'),align='center'),
    #                          column(4,img(src=ImageColorIndirect[1],width = '20%',height = '20%'),align='center')),
    #                 fluidRow(column(4,HTML(paste('<font size = "3">',YuhuaNetConsumptionOutput_DirectIndirect$Block[2],'</font>')),align='center'),
    #                          column(4,img(src=ImageColorDirect[2],width = '20%',height = '20%'),align='center'),
    #                          column(4,img(src=ImageColorIndirect[2],width = '20%',height = '20%'),align='center')),
    #                 fluidRow(column(4,HTML(paste('<font size = "3">',YuhuaNetConsumptionOutput_DirectIndirect$Block[3],'</font>')),align='center'),
    #                          column(4,img(src=ImageColorDirect[3],width = '20%',height = '20%'),align='center'),
    #                          column(4,img(src=ImageColorIndirect[3],width = '20%',height = '20%'),align='center')),
    #                 fluidRow(column(4,HTML(paste('<font size = "3">',YuhuaNetConsumptionOutput_DirectIndirect$Block[4],'</font>')),align='center'),
    #                          column(4,img(src=ImageColorDirect[4],width = '20%',height = '20%'),align='center'),
    #                          column(4,img(src=ImageColorIndirect[4],width = '20%',height = '20%'),align='center')),
    #                 fluidRow(column(4,HTML(paste('<font size = "3">',YuhuaNetConsumptionOutput_DirectIndirect$Block[5],'</font>')),align='center'),
    #                          column(4,img(src=ImageColorDirect[5],width = '20%',height = '20%'),align='center'),
    #                          column(4,img(src=ImageColorIndirect[5],width = '20%',height = '20%'),align='center')),
    #                 fluidRow(column(4,HTML(paste('<font size = "3">',YuhuaNetConsumptionOutput_DirectIndirect$Block[6],'</font>')),align='center'),
    #                          column(4,img(src=ImageColorDirect[6],width = '20%',height = '20%'),align='center'),
    #                          column(4,img(src=ImageColorIndirect[6],width = '20%',height = '20%'),align='center')),
    #                 fluidRow(column(4,HTML(paste('<font size = "3">',YuhuaNetConsumptionOutput_DirectIndirect$Block[7],'</font>')),align='center'),
    #                          column(4,img(src=ImageColorDirect[7],width = '20%',height = '20%'),align='center'),
    #                          column(4,img(src=ImageColorIndirect[7],width = '20%',height = '20%'),align='center'))
    #))
  #}
})

## -------- Water Savings ------------- ##
output$WSAlarm_val <- renderUI({
  # HTML('<font size = "5"><b>',comma(round(Water_Saved/1000)),'m<sup>3</sup></b></font>')
  #HTML('<font size = "5"><b>',comma(WaterSaved_LeakAlarm),'m<sup>3</sup></b><span style="font-size: 20px;"> (',comma(WaterSaved_LeakAlarm_Percent),'%)</span></font>')
  HTML('<font size = "5"><b>',4.98,'LPCD</b><span style="font-size: 20px;"> (',3.7,'%)</span></font>')
})

output$WSGame_val <- renderUI({
  #HTML('<font size = "5"><b>',comma(WaterSaved_Gamification),'m<sup>3</sup></b><span style="font-size: 20px;"> (',comma(WaterSaved_Gamification_Percent),'%)</span></font>')
  #HTML('<font size = "5"><b>',6.78,'LPCD</b><span style="font-size: 20px;"> (',5.3,'%)</span></font>')
  HTML('<font size = "5"><b>',2.76,'LPCD</b><span style="font-size: 20px;"> (',3.5,'%)</span></font>')
})

## -------- Customer Engagement ------------- ##
titlestr <- paste('Weekly LPCD of Customers (Online=',length(unique(OnlineCustomers$service_point_sn)),',Offline=',length(unique(OfflineCustomers$service_point_sn)),")",sep="")
output$CustomerWeeklyLPCD_plot<- renderPlotly({
  p <- plot_ly(WeeklyLPCD_OnOffline, x = ~YearWeek, y = ~WeeklyLPCD_Online, type='scatter',mode='lines',name='Online',
               marker = list(color = "green"),line = list(color = "green")) %>%
    add_trace(WeeklyLPCD_OnOffline, x = ~YearWeek, y = ~WeeklyLPCD_Offline, type='scatter',mode='lines',name = 'Offline',
              marker = list(color = "orange"),line = list(color = "orange")) %>%
    layout(title = titlestr,
           xaxis = list(title = 'Year_Weeknumber',tickangle = -17),
           yaxis = list(title = 'Weekly LPCD'),showlegend = FALSE) %>%
    config(displayModeBar = F)
  p
})

output$OnlineCustomers <- renderValueBox({
  value <- length(unique(OnlineCustomers$service_point_sn))
  boxContent <- div(class = paste0("small-box bg-", 'green'), 
                    div(class = "inner",p('Online Customers'), h3(value)), 
                    div(class = "icon-large", icon("home")))
  width <- 16
  div(class = if (!is.null(width)) 
    paste0("col-sm-", width), boxContent)
})

output$OfflineCustomers <- renderValueBox({
  value <- length(unique(OfflineCustomers$service_point_sn))
  boxContent <- div(class = paste0("small-box bg-", 'orange'), 
                    div(class = "inner",p('Offline Customers'), h3(value)), 
                    div(class = "icon-large", icon("home")))
  width <- 16
  div(class = if (!is.null(width)) 
    paste0("col-sm-", width), boxContent)
})

## -------- Overall Summary ------------- ##
output$OverallQuantities_table <- DT::renderDataTable({DT::datatable(OverallQuantities,
                                                    options = list(dom='t', paging=FALSE,ordering=FALSE),
                                                    rownames = FALSE,escape=FALSE)
                                                   })

## -------- Data Quality ------------- ##
output$GaugePunggol <- renderGauge({
  gauge(round(as.numeric(AverageHourlyReadingRate_Punggol)), min = 0, max = 100, symbol = '%', gaugeSectors(
    success = c(80, 100), warning = c(40, 79), danger = c(0, 39)
  ))
})

output$GaugeYuhua <- renderGauge({
  gauge(round(as.numeric(AverageHourlyReadingRate_Yuhua)), min = 0, max = 100, symbol = '%', gaugeSectors(
    success = c(80, 100), warning = c(40, 79), danger = c(0, 39)
  ))
})

output$Maintenance <- renderValueBox({
  color.maint <- ifelse(QuantityNoDataMeter==0,'green','red')
  boxContent <- div(class = paste0("small-box bg-", color.maint),
                    div(class = "inner",
                        #h4('Maintenance'),
                        h3(QuantityNoDataMeter),
                        p('Meters with no data for yesterday'),
                        div(class = "icon-large", icon("wrench"))))
  width <- 16
  div(class = if (!is.null(width))
    paste0("col-sm-", width), boxContent)
})

output$signalquality_vs_distance <- renderPlotly({
  pal <- c("blue","green","brown")
  p <- plot_ly(data = PunggolYuhuaTuas_DistanceReadingRate,x = ~Distance,y = ~ReadingRate,
               type='scatter',mode = 'markers',
               text = ~customer,
               color = ~site,colors = pal) %>%
    layout(title = 'Quality of Signal per Transmitter vs Distance',
           xaxis = list(title = 'Distance from the nearest receiver (m)'),
           yaxis = list(title = 'Av. Hourly Reading rate (%)',range=c(35,102)),
           showlegend = TRUE) %>%
    config(displayModeBar = F)
  p
})

## -------- General Information ------------- ##
output$nbBlkBox <- renderValueBox({
  nbB <- length(unique(X$block))
  boxContent <- div(class = paste0("small-box bg-", 'teal'), 
                    div(class = "inner",p('Blocks'), h3(nbB)), 
                    div(class = "icon-large", icon("home")))
  width <- 16
  div(class = if (!is.null(width)) 
    paste0("col-sm-", width), boxContent)
})

output$AMR_First <- renderValueBox({
  boxContent <- div(class = paste0("small-box bg-", 'purple'), 
                    div(class = "inner",p('first date reception'), h3("2016-02-24")), #h3(as.character(min(X$Date)))), 
                    div(class = "icon-large", icon("calendar")))
  width <- 16
  div(class = if (!is.null(width)) 
    paste0("col-sm-", width), boxContent)
})

output$AMR_Last <- renderValueBox({
  boxContent <- div(class = paste0("small-box bg-", 'purple'), 
                    div(class = "inner",p('last date reception'), h3(as.character(max(X$Date)))), 
                    div(class = "icon-large", icon("calendar")))
  width <- 16
  div(class = if (!is.null(width)) 
    paste0("col-sm-", width), boxContent)
})

output$lastUpdated_Dashboard <- renderText({Updated_DateTime_Dashboard})