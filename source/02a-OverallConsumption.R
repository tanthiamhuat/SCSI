if (input$so_site=='0'){
  X <- Punggol_SUB
}else{
  X <- Yuhua_SUB
}

output$so_customperiod <- renderUI({
  if(input$so_period=='0'){
     dateRangeInput('dateRange',
                    label = h3('Date range'),
                    start = max(X$Date)-7, end = max(X$Date),separator = " - ", format = "dd/mm/yyyy"
    )
    # cat(as.character(input$dateRange))
  }else{
    NULL
  }
})

start_so <- reactive({
  #this_day <- max(X$Date) # => should be replaced by Sys.Date()
  this_day <- Sys.Date()
  temp <- switch(input$so_period,
                  "0" = as.Date(as.character(input$dateRange[1]),tz="Asia/Singapore"),
                  "1" = this_day,
                  "2" = this_day-1,
                  "3" = as.Date(Week.date$beg[match(X$week[match(this_day,X$Date)[1]],Week.date$week)]),
                  "4" = as.Date(Week.date$beg[match(X$week[match(this_day,X$Date)[1]],Week.date$week)-1]),
                  "5" = this_day-7,
                  "6" = floor_date(this_day,unit='month'),
                   #as.Date(format(this_day,'%Y-%m-01'),tz='Asia/Singapore'),
                  "7" = floor_date(this_day,unit='month')-months(1), 
                   #as.Date(format(as.Date(format(this_day,'%Y-%m-01'),tz='Asia/Singapore')-1,'%Y-%m-01'),tz='Asia/Singapore'),
                  "8" = this_day-30,
                  "9" = this_day-90
                  #"9" = floor_date(this_day,unit='year'), 
                   #as.Date(format(this_day,'%Y-01-01 00:00:00'),tz='Asia/Singapore'),
                  #"10"= floor_date(this_day,unit='year')-years(1)
                    #as.Date(paste0(as.numeric(format(this_day,'%Y'))-1,"-01-01 00:00:00"),tz="Asia/Singapore")
  )
  as.character(temp)
})

end_so <- reactive({
  #this_day <- max(X$Date)
  this_day <- Sys.Date()
  temp <- switch(input$so_period,
                "0"=as.Date(input$dateRange[2]+1,tz='Asia/Singapore'),
                "1"=this_day+1,
                "2"=this_day,
                "3"=this_day+1,
                "4"=as.Date(Week.date$beg[match(X$week[match(this_day,X$Date)[1]],Week.date$week)])+1,
                "5"=this_day+1,
                "6"=this_day+1,
                "7"=floor_date(this_day,unit='month'),
                  #as.Date(format(this_day,'%Y-%m-01'),tz='Asia/Singapore'),
                "8"=this_day+1,
                "9"=this_day+1
                #"10"=floor_date(this_day,unit='year')
                  #as.Date(format(this_day,'%Y-01-01'),tz='GMT')
  )
  as.character(temp)
})

Public.Holidays$V1 <- as.character(Public.Holidays$V1)

output$so_plot <- renderDygraph({
  if (input$so_site=='0'){
    so_df_ini <- Punggol_SUB %>% filter(Date >= ymd(start_so()) & Date < ymd(end_so()))
  }else{
    so_df_ini <- Yuhua_SUB %>% filter(Date >= ymd(start_so()) & Date < ymd(end_so()))  
  }
  
  if(input$so_timestep=='0'){
    so_df_ini$Time <- so_df_ini$Date.Time # Hourly
  }else{
    so_df_ini$Time <- so_df_ini$Date  # Daily
  }
  so_df <- so_df_ini %>% dplyr::group_by(Time,Date,wd,week)%>% dplyr::summarise(vol = sum(Consumption,na.rm=TRUE))
  so_df$vol_plot <- so_df$vol/1000
  
  dotted.date <- max(X$Date.Time)-hours(4)
  if(input$so_timestep=='1'){dotted.date <- ymd(as.character(floor_date(dotted.date,unit = 'day')))-1}
  
  weekend.plot <- so_df %>% filter(wd %in% c('Saturday','Sunday')) %>% group_by(week) %>% dplyr::summarise(beg=min(Time),end=max(Time))
  # holidays.plot <- so_df %>% filter(!is.na(Holiday)) %>% dplyr::group_by(Holiday) %>% dplyr::summarise(beg=min(Time),end=max(Time))
  # Consumption <- xts(so_df$vol_plot,so_df$Time);
  # colnames(Consumption) <- c('Volume')

  if(nrow(so_df)>1){ 
    if(max(so_df$Date) == max(X$Date)){
      if(min(so_df$Time) >= dotted.date){
        Consumption <- xts(so_df$vol_plot,so_df$Time);
        maxC <- max(Consumption)
        name <- c('Volume.temp')
        colours <- c("rgb(3,15,64)")
      }else{
        so_df$type <- ifelse(so_df$Time<=dotted.date,"solid","dotted")
        so_df_plot <- so_df %>% spread(type,vol_plot)
        so_df_plot$dotted[match(dotted.date,so_df_plot$Time)] <- so_df_plot$solid[match(dotted.date,so_df_plot$Time)]
        Consumption <- xts(so_df_plot[,c("solid","dotted")],so_df$Time);
        name <- c('Volume','Volume.temp')
        maxC <- max(Consumption)
        colours <- rep("rgb(3,15,64)",2)
      }
    }else{
      Consumption <- xts(so_df$vol_plot,so_df$Time);
      colnames(Consumption) <- c('Volume')
      name <- c('Volume')
      maxC <- max(Consumption)
      colours <- c("rgb(3,15,64)")
    }

   if('3'%in%input$so_show & input$so_timestep=='1'){
     subWeather <- as.data.frame(Weather %>% filter(Date >= max(ymd(start_so()),min(X$Date)) & Date < ymd(end_so())))
     ## problem occurs if some dates don't correspond
     #Tmax <- subWeather$Tmax[match(so_df$Date,subWeather$Date)]
     Tavg <- subWeather$Tavg[match(so_df$Date,subWeather$Date)]
     
     Consumption <- cbind(Consumption,Tavg)
     name <- c(name, 'Avg.Temp')
     colours <- c(colours,"rgb(170,220,20)")
   }
    # indexTZ(Consumption) <- 'Asia/Singapore'
    colnames(Consumption) <- name
  if(max(so_df$Date) == max(X$Date) & min(so_df$Time) >= dotted.date){
    graph <-  dygraph(Consumption) %>%
      dySeries('Volume.temp',strokePattern = "dashed") %>%
      dyRangeSelector() %>% dyLegend(width=350,show='onmouseover') %>% 
      dyAxis("y",label=HTML('Consumption in m<sup>3</sup>'), valueRange = c(0, maxC*1.2)) %>% 
      dyOptions(useDataTimezone = TRUE,colors = colours)
  }else{
    graph <-  dygraph(Consumption) %>%
      dySeries('Volume') %>%
      dyRangeSelector() %>% dyLegend(width=350,show='onmouseover') %>% 
      dyAxis("y",label=HTML('Consumption in m<sup>3</sup>'), valueRange = c(0, maxC*1.2)) %>% 
      dyOptions(useDataTimezone = TRUE,colors = colours)
    
    if(max(so_df$Date) == max(X$Date)){
      graph <- graph %>% dySeries('Volume.temp',strokePattern = "dashed")
    }
  } 

   if('3'%in%input$so_show & input$so_timestep=='1'){
     graph <-  graph %>%
       # dySeries("Max.Temp", axis = "y2") %>%
       # dyAxis("y2",label=HTML('Maximal temperature &deg;C'), valueRange = c(0, max(Consumption[,ncol(Consumption)])*1.2))
       dySeries("Avg.Temp", axis = "y2") %>%
       dyAxis("y2",label=HTML('Average temperature &deg;C'), valueRange = c(0, max(Consumption[,ncol(Consumption)])*1.2))
     
   }     
   
   if("1" %in% input$so_show){
     for (i in 1:nrow(weekend.plot)){
       graph <- graph%>%dyShading(from = weekend.plot$beg[i], to = weekend.plot$end[i], color = "#fdd2c1")
     }}
   if("2" %in% input$so_show){
     for (i in 1:nrow(Public.Holidays)){
       if(Public.Holidays$Duration[i]>0){
         graph <- graph%>%dyShading(from = Public.Holidays$Start[i], to = Public.Holidays$End[i], color = "#b58c20")
       }else{
       graph <- graph%>%dyEvent(Public.Holidays$Start[i], Public.Holidays$V1[i],labelLoc = 'bottom')}
       
     }}

  }else{ 
      so_df <- data.frame(Time=so_df$Time[1]+c(0,1,-1),vol_plot=c(so_df$vol_plot[1],NA,NA))
      Consumption <- xts(so_df$vol_plot,so_df$Time);
      colnames(Consumption) <- "Volume"
      
    # attr(Consumption,'tzone') <- 'Asia/Singapore'
    # attr(Consumption,'.indexTZ') <- 'Asia/Singapore'
    # indexTZ(Consumption) <- 'Asia/Singapore'
    graph <- dygraph(Consumption) %>% 
      dyAxis("y",label=HTML('Consumption in m<sup>3</sup>'), valueRange = c(0, max(Consumption)*1.2)) %>% 
      dyOptions(useDataTimezone = TRUE, plotter = 
        "function barChartPlotter(e) {
            var ctx = e.drawingContext;
            var points = e.points;
            var y_bottom = e.dygraph.toDomYCoord(0);  // see     http://dygraphs.com/jsdoc/symbols/Dygraph.html#toDomYCoord
            
            // This should really be based on the minimum gap
            var bar_width = 2/3 * (points[1].canvasx - points[0].canvasx);
            ctx.fillStyle = e.color;
            
            // Do the actual plotting.
            for (var i = 0; i < points.length; i++) {
            var p = points[i];
            var center_x = p.canvasx;  // center of the bar
            
            ctx.fillRect(center_x - bar_width / 2, p.canvasy,
            bar_width, y_bottom - p.canvasy);
            ctx.strokeRect(center_x - bar_width / 2, p.canvasy,
            bar_width, y_bottom - p.canvasy);
            }
        }",colors="rgb(3,15,64)")%>% 
      dyLegend() %>% dyRangeSelector()
    
  }
  graph
})

output$so_legend_dg<- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <!--<tr>
                      <td style="width:50px;"> <img src="information.png" heigth="80%" width="80%"> </td>
                      <td>  </td>
                      <td> </td>
                      <td> Consumptions </td>
                   </tr>-->
                   <tr> <b> Legend </b> </td>
                   <tr>
                   <td> <font color="#030f40"> ___ </font> </td>
                   <td> &nbsp;</td> 
                   <td> Consumptions </td>
                   </tr>  
                   <tr>
                      <td> <font color="#030f40"> ---- </font> </td>
                      <td> &nbsp;</td> 
                      <td> Incomplete data </td>
                   </tr>    
                   <tr>
                      <td> <font color="#aadc14"> ___ </font> </td>
                      <td> &nbsp;</td>      
                      <td> Weather information </td>
                   </tr>  
                   <tr>
                      <td BGCOLOR="#fdd2c1">  </td>
                      <td> &nbsp;</td>    
                      <td> Week-end </td>
                   </tr>  
                   <tr>
                      <td BGCOLOR="#b58c20"> </td>
                      <td> &nbsp;</td>    
                      <td> School Holidays </td>
                   </tr>  
                   </table>'))
    )
})

output$lastUpdated_OverallCons <- renderText(Updated_DateTime_HourlyCons)