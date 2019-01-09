local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

#library(latticeExtra)
load(paste0(path,'data/CustomerSummary.RData'))
CustomerAgeRangeCSV_PG <- read.csv2(paste0(path,'data/CustomerAgeRange_PG.csv'),header = TRUE,sep=",")
CustomerAgeRangeCSV_YH <- read.csv2(paste0(path,'data/CustomerAgeRange_YH.csv'),header = TRUE,sep=",")
CustomerLoginFrequencyCSV_PG <- read.csv2(paste0(path,'data/CustomerLoginFrequency_PG.csv'),header = TRUE,sep=",")
CustomerLoginFrequencyCSV_YH <- read.csv2(paste0(path,'data/CustomerLoginFrequency_YH.csv'),header = TRUE,sep=",")
VisitorsPerDayCSV_PG <- read.csv2(paste0(path,'data/VisitorsPerDay_PG.csv'),header = TRUE,sep=",")
VisitorsPerDayCSV_YH <- read.csv2(paste0(path,'data/VisitorsPerDay_YH.csv'),header = TRUE,sep=",")
BlockOnlineOfflineCSV <- read.csv2(paste0(path,'data/BlockOnlineOffline.csv'),header = TRUE,sep=",")
CustomerSignUpCSV <- read.csv2(paste0(path,'data/CustomerSignUp.csv'),header = TRUE,sep=",")
CustomerAccumulativePointsCSV_PG <- read.csv2(paste0(path,'data/CustomerAccumulativePoints_PG.csv'),header = TRUE,sep=",")
CustomerAccumulativePointsCSV_YH <- read.csv2(paste0(path,'data/CustomerAccumulativePoints_YH.csv'),header = TRUE,sep=",")
CustomerAccumulatedPointsCSV_PG <- read.csv2(paste0(path,'data/CustomerAccumulatedPoints_PG.csv'),header = TRUE,sep=",")
CustomerAccumulatedPointsCSV_YH <- read.csv2(paste0(path,'data/CustomerAccumulatedPoints_YH.csv'),header = TRUE,sep=",")

output$CustomerAgeRange_plot<- renderPlotly({
  if (input$CA_Summary_site=='0'){
    CustomerAgeRange <- CustomerAgeRange_PG
  }else{
    CustomerAgeRange <- CustomerAgeRange_YH
  }
  colors <- c('rgb(211,94,96)', 'rgb(128,133,133)', 'rgb(144,103,167)', 'rgb(171,104,87)')
  p <- plot_ly(CustomerAgeRange, labels = ~age_range, values = ~Percent, type = 'pie',sort = FALSE,direction = "clockwise",
               textposition = 'inside',
               textinfo = 'label+percent',
               insidetextfont = list(color = '#FFFFFF'),
               hoverinfo = 'text',
               marker = list(colors = colors,
                             line = list(color = '#FFFFFF', width = 1)),
               #The 'pull' attribute can also be used to create space between the sectors
               showlegend = FALSE) %>%
    layout(title = 'Customer Age Range Breakdown',
           xaxis = list(showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE),
           yaxis = list(showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE)) %>%
    config(displayModeBar = F)
 p
})

output$downloadCustomerAgeRangeData <- downloadHandler(
  filename = function() {
    if (input$CA_Summary_site=='0'){
      paste('CustomerAgeRange_Punggol', '.csv', sep='')
    }
    else if(input$CA_Summary_site=='1'){
      paste('CustomerAgeRange_Yuhua', '.csv', sep='')
    }
  },
  content = function(file) {
    if (input$CA_Summary_site=='0'){
      write.csv(CustomerAgeRangeCSV_PG, file, row.names = FALSE)
    }
    else if(input$CA_Summary_site=='1'){
      write.csv(CustomerAgeRangeCSV_YH, file, row.names = FALSE)
    }
  }
)

output$CustomerLoginFreq_plot <- renderPlotly({
  if (input$CA_Summary_site=='0'){
    CustomerLoginFrequency_Percent <- CustomerLoginFrequency_Percent_PG
  }else{
    CustomerLoginFrequency_Percent <- CustomerLoginFrequency_Percent_YH
  }
  colors <- c('rgb(211,94,96)', 'rgb(128,133,133)', 'rgb(144,103,167)', 'rgb(171,104,87)')
  p <- plot_ly(CustomerLoginFrequency_Percent, labels = ~Category, values = ~Category_Percent, type = 'pie',sort = FALSE,direction = "clockwise",
               textposition = 'inside',
               textinfo = 'label+percent',
               insidetextfont = list(color = '#FFFFFF'),
               hoverinfo = 'text',
               marker = list(colors = colors,
                             line = list(color = '#FFFFFF', width = 1)),
               #The 'pull' attribute can also be used to create space between the sectors
               showlegend = FALSE) %>%
    layout(title = 'Customer Login Frequency',
           xaxis = list(showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE),
           yaxis = list(showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE)) %>%
    config(displayModeBar = F)
  p
})

output$downloadCustomerLoginFrequencyData <- downloadHandler(
  filename = function() {
    if (input$CA_Summary_site=='0'){
      paste('CustomerLoginFrequency_Punggol', '.csv', sep='')
    }
    else if(input$CA_Summary_site=='1'){
      paste('CustomerLoginFrequency_Yuhua', '.csv', sep='')
    }
  },
  content = function(file) {
    if (input$CA_Summary_site=='0'){
      write.csv(CustomerLoginFrequencyCSV_PG, file, row.names = FALSE)
    }
    else if(input$CA_Summary_site=='1'){
      write.csv(CustomerLoginFrequencyCSV_YH, file, row.names = FALSE)
    }
  }
)

output$VisitorsPerDay_plot <- renderPlotly({
  if (input$CA_Summary_site=='0'){
    VisitorsPerDay_Wide <- VisitorsPerDayPG_Wide
  }else{
    VisitorsPerDay_Wide <- VisitorsPerDayYH_Wide
  }
  p <- plot_ly(VisitorsPerDay_Wide, x = ~Date, y = ~Low, type='scatter',mode='lines',name='Low',
               line = list(color = "red")) %>%
    add_trace(VisitorsPerDay_Wide, x = ~Date, y = ~Unique, type='scatter',mode='lines',name = 'Unique',
              line = list(color = "orange")) %>%
    add_trace(VisitorsPerDay_Wide, x = ~Date, y = ~Regular, type='scatter',mode='lines',name = 'Regular',
              line = list(color = "green")) %>%
    add_trace(VisitorsPerDay_Wide, x = ~Date, y = ~Moderate, type='scatter',mode='lines',name = 'Moderate',
              line = list(color = "blue")) %>%
    add_trace(VisitorsPerDay_Wide, x = ~Date, y = ~Total, type='scatter',mode='lines',name='Total',
              line=list(color="black")) %>%
    layout(title = 'Visitors Per Day',
           xaxis = list(title = 'Date'),
           yaxis = list(title = 'Number of Visitors'),showlegend = TRUE) %>%
    config(displayModeBar = F)
  p
})

output$downloadVisitorsPerDayData <- downloadHandler(
   filename = function() {
    if (input$CA_Summary_site=='0'){
      paste('VisitorsPerDay_Punggol', '.csv', sep='')
    }
    else if(input$CA_Summary_site=='1'){
      paste('VisitorsPerDay_Yuhua', '.csv', sep='')
    }
  },
  content = function(file) {
    if (input$CA_Summary_site=='0'){
      write.csv(VisitorsPerDayCSV_PG, file, row.names = FALSE)
    }
    else if(input$CA_Summary_site=='1'){
      write.csv(VisitorsPerDayCSV_YH, file, row.names = FALSE)
    }
  }
)

output$BlockOnlineOffline_plot<- renderPlotly({
p <- plot_ly(BlockOnlineOffline, x = ~block_online_percent, y = ~y, type = 'bar', orientation = 'h', name = 'Online',
             marker = list(color = "green"),line = list(color = "green",width = 3)) %>%
     add_trace(x = ~block_offline_percent, name = 'Offline',
            marker = list(color = "orange"),line = list(color = "orange",width = 3)) %>%
     layout(barmode = 'stack',
         title = "Percentage of Online / Offline Households Per Block",
         xaxis = list(title = ""),
         yaxis = list(title ="")) %>%  config(displayModeBar = F)
p
})

output$downloadBlockOnlineOfflineData <- downloadHandler(
  filename = function() { paste('BlockOnlineOffline', '.csv', sep='') },
  content = function(file) {
    write.csv(BlockOnlineOfflineCSV, file, row.names = FALSE)
  }
)

output$CustomerSignUp_plot<- renderPlotly({
  p <- plot_ly(CustomerSignUp, x = ~Date, y = ~SignUp, type='scatter',mode='lines',name='SignUp',
               line = list(color = "blue")) %>%
    add_trace(CustomerSignUp, x = ~Date, y = ~PerCent, type='scatter',mode='lines',name = 'PerCent',
              line = list(color = "green"), yaxis = "y2") %>%
    layout(title = 'Cumulative Sum & Percentage of Online Customers',
           xaxis = list(title = 'Date'),
           yaxis = list(title = 'Online Customers'),showlegend = TRUE) %>%
    layout(yaxis2 = list(overlaying ="y",side = "right",title="Percentage"))%>%
    layout(legend = list(x = 1.08, y = 1.0)) %>%
    config(displayModeBar = F)
  p
})

output$downloadCustomerSignUpData <- downloadHandler(
  filename = function() { paste('CustomerSignUp', '.csv', sep='') },
  content = function(file) {
    write.csv(CustomerSignUpCSV, file, row.names = FALSE)
  }
)

output$CustomerAccumulativePoints_plot<- renderPlotly({
  if (input$CA_Summary_site=='0'){
    CustomerAccumulativePoints <- CustomerAccumulativePoints_PG
  }else{
    CustomerAccumulativePoints <- CustomerAccumulativePoints_YH
  }
  p <- plot_ly(CustomerAccumulativePoints, x = ~Date, y = ~AccumulativePoints,type = 'scatter', mode = 'lines',
               line = list( width = 0.5)) %>%
    layout(title = "Accumulative Points",
           xaxis = list(title = "Date",tickangle = -15),
           yaxis = list (title = "Points")) %>%
    config(displayModeBar = F)
  p
})

output$downloadCustomerAccumulativePointsData <- downloadHandler(
  filename = function() {
    if (input$CA_Summary_site=='0'){
      paste('CustomerAccumulativePoints_Punggol', '.csv', sep='')
    }
    else if(input$CA_Summary_site=='1'){
      paste('CustomerAccumulativePoints_Yuhua', '.csv', sep='')
    }
  },
  content = function(file) {
    if (input$CA_Summary_site=='0'){
      write.csv(CustomerAccumulativePointsCSV_PG, file, row.names = FALSE)
    }
    else if(input$CA_Summary_site=='1'){
      write.csv(CustomerAccumulativePointsCSV_YH, file, row.names = FALSE)
    }
  }
)

output$CustomerAccumulatedPoints_Histogram<- renderPlotly({
  if (input$CA_Summary_site=='0'){
    CustomerAccumulatedPoints_Users <- CustomerAccumulatedPoints_Users_PG
  }else{
    CustomerAccumulatedPoints_Users <- CustomerAccumulatedPoints_Users_YH
  }
  
  h <- hist(CustomerAccumulatedPoints_Users$AccumulativePoints)
  
  p <- plot_ly(x = h$mids, y = h$counts) %>% 
       add_bars() %>% 
       layout(title = "Histogram for Customers' Accumulated Points",
              xaxis = list(title = "Accumulated Points"),
              yaxis = list (title = "Number of Users")) %>%
       config(displayModeBar = F)
  p
})

output$downloadCustomerAccumulatedPointsData <- downloadHandler(
  filename = function() {
    if (input$CA_Summary_site=='0'){
      paste('CustomerAccumulatedPoints_Punggol', '.csv', sep='')
    }
    else if(input$CA_Summary_site=='1'){
      paste('CustomerAccumulatedPoints_Yuhua', '.csv', sep='')
    }
  },
  content = function(file) {
    if (input$CA_Summary_site=='0'){
      write.csv(CustomerAccumulatedPointsCSV_PG, file, row.names = FALSE)
    }
    else if(input$CA_Summary_site=='1'){
      write.csv(CustomerAccumulatedPointsCSV_YH, file, row.names = FALSE)
    }
  }
)

output$lastUpdated_CustomerSummary <- renderText(Updated_DateTime_CustomerSummary)
