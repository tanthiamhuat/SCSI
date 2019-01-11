local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

# because of non-interpolated, with minutes values, which need to round off the hour
X <- X %>% dplyr::mutate(Date.Time=round_date(ymd_hms(date_consumption),"hour")) 

Xfull <- X %>% dplyr::filter(Date!=max(X$Date))
full.week.days <- c('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
Xfull$wd <- factor(Xfull$wd,levels = full.week.days)

#Total <- Xfull %>% group_by(Date.Time,Date,H,M,wd, week) %>% dplyr::summarise(Volume=sum(interpolated_consumption)/1000)
Total <- Xfull %>% group_by(Date.Time,Date,H,M,wd, week) %>% dplyr::summarise(Volume=sum(adjusted_consumption)/1000)
# ggplotly(Total %>% ggplot(aes(x=Date.Time,y=Volume,group=1,colour=1)) + geom_line()+ theme(legend.position="none"))
#Day.Time <- read_excel('source/Day Period.xlsx')
Day.Time <- read_excel(paste0(path,'data/Day Period.xlsx'))
### Option 1 => global trends ####
## Hour
Global.Peak.Hour <- function(Total){
  Avg.Hourly <- Total %>% group_by(H) %>% dplyr::summarise(Volume = median(Volume))
  Avg.Hourly$Type <- 'Avg'
  plot.avg <-Avg.Hourly %>% ggplot(aes(x=as.factor(H),y=Volume,group=1)) + 
    geom_line(colour = '#AADC14') + geom_point(colour = '#030F40') + 
    xlab('Hour') + 
    ylab(HTML('Consumption (m<sup>3</sup>/h)')) +
    scale_x_discrete(breaks=seq(0,23,by=2),labels=Day.Time$Hour[seq(0,23,by=2)+1]) 
  
  ## peak periods
  Peak <- Avg.Hourly[Avg.Hourly$Volume>=quantile(Avg.Hourly$Volume,0.85),]
  Peak$Period <- Day.Time$Day.Period[match(Peak$H,Day.Time$H)]
  Peak$Hour <- Day.Time$Hour[match(Peak$H,Day.Time$H)]
  nb.period <- sum(diff(Peak$H)>1) + 1
  
  
  deb.peak <- Peak$H[c(1,which(diff(Peak$H)>1)+1)]
  end.peak <- rev(rev(Peak$H)[c(1,which(diff(rev(Peak$H))< -1)+1)])
  # message.peak<- paste0('Peak period',ifelse(nb.period>1,'s',''),' occure',ifelse(nb.period>1,'','s'),' during:\n')
  message.peak<- paste0('Peak period',ifelse(nb.period>1,'s',''),' occur',ifelse(nb.period>1,'','s'),' during:<br /><ul>')
  for (i in 1:nb.period){
    if(deb.peak[i]==end.peak[i]){
      temp.peak <- Peak[Peak$H==deb.peak[i],]
      # temp.mess <- paste0(temp.peak$Period,' at ',temp.peak$Hour,' (',round(temp.peak$Volume,1),' m3/h)')
      temp.mess <- paste0(temp.peak$Period,' at ',temp.peak$Hour,' (',round(temp.peak$Volume,1),' m<sup>3</sup>/h)')
    }else{
      temp.peak <- Peak[Peak$H>=deb.peak[i] & Peak$H<=end.peak[i],]
      if(unique(temp.peak$Period)==1){
        # temp.mess <- paste0(temp.peak$Period[1],' between ',temp.peak$Hour[which.min(temp.peak$H)],' and ',temp.peak$Hour[which.max(temp.peak$H)],' (',round(mean(temp.peak$Volume,1)),' m3/h)')
        temp.mess <- paste0(temp.peak$Period[1],' between ',temp.peak$Hour[which.min(temp.peak$H)],' and ',temp.peak$Hour[which.max(temp.peak$H)],' (',round(mean(temp.peak$Volume,1)),' m<sup>3</sup>/h)')
      }else{
        # temp.mess <- paste0(paste0(unique(temp.peak$Period),collapse='/'),
        #                     ' between ',temp.peak$Hour[which.min(temp.peak$H)],' and ',temp.peak$Hour[which.max(temp.peak$H)],' (',round(mean(temp.peak$Volume,1)),' m3/h)')
        temp.mess <- paste0(paste0(unique(temp.peak$Period),collapse='/'),
                            ' between ',temp.peak$Hour[which.min(temp.peak$H)],' and ',temp.peak$Hour[which.max(temp.peak$H)],' (',round(mean(temp.peak$Volume,1)),' m<sup>3</sup>/h)')
        
      }
    }
    message.peak<- paste0(message.peak,'<li>',temp.mess)
  }
  message.peak <- paste0(message.peak,'</ul>')
  
  res.peak <- list(Peak = Peak, message = message.peak)  
  
  ## drop consumption periods
  Drop <- Avg.Hourly[Avg.Hourly$Volume<=quantile(Avg.Hourly$Volume,0.1),]
  Drop$Period <- Day.Time$Day.Period[match(Drop$H,Day.Time$H)]
  Drop$Hour <- Day.Time$Hour[match(Drop$H,Day.Time$H)]
  nb.period <- sum(diff(Drop$H)>1) + 1
  
  
  deb.drop <- Drop$H[c(1,which(diff(Drop$H)>1)+1)]
  end.drop <- rev(rev(Drop$H)[c(1,which(diff(rev(Drop$H))< -1)+1)])
  message.drop <- paste0('Drop period',ifelse(nb.period>1,'s',''),' occur',ifelse(nb.period>1,'','s'),' during:<br /><ul>')
  for (i in 1:nb.period){
    if(deb.drop[i]==end.drop[i]){
      temp.drop <- Drop[Drop$H==deb.drop[i],]
      temp.mess <- paste0(temp.drop$Period,' at ',temp.drop$Hour,' (',round(temp.drop$Volume,1),' m<sup>3</sup>/h)')
    }else{
      temp.drop <- Drop[Drop$H>=deb.drop[i] & Drop$H<=end.drop[i],]
      if(unique(temp.drop$Period)==1){
        temp.mess <- paste0(temp.drop$Period[1],' between ',temp.drop$Hour[which.min(temp.drop$H)],' and ',temp.drop$Hour[which.max(temp.drop$H)],' (',round(mean(temp.drop$Volume,1)),' m<sup>3</sup>/h)')
      }else{
        temp.mess <- paste0(paste0(unique(temp.drop$Period),collapse='/'),
                            ' between ',temp.drop$Hour[which.min(temp.drop$H)],' and ',temp.drop$Hour[which.max(temp.drop$H)],' (',round(mean(temp.drop$Volume,1)),' m<sup>3</sup>/h)')
      }
    }
    message.drop <- paste0(message.drop,'<li>',temp.mess)
  }
  message.drop <- paste0(message.drop,'</ul>')
  res.drop <- list(Drop = Drop, message = message.drop)  
  
  result <- list(data=Avg.Hourly, plot= plot.avg, peak=res.peak,drop=res.drop)
  return(result)
}

GPH <- Global.Peak.Hour(Total)

## Day
Total.day <- Total %>% group_by(wd,Date,M,week) %>% dplyr::summarise(Volume=sum(Volume))
Total.day <- Total.day %>% dplyr::filter(Date >= as.Date('2016-03-04'))
# ggplotly(Total.day %>% ggplot(aes(x=Date,y=Volume,group=1,colour=1)) + geom_line()+ theme(legend.position="none"))

Global.Peak.Day <- function(Total.day){
  Avg.Daily <- Total.day %>% group_by(wd) %>% dplyr::summarise(Volume = median(Volume))
  full.week.days <- c('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
  Avg.Daily$Day <- factor(full.week.days[as.numeric(Avg.Daily$wd)],levels = full.week.days)
  Avg.Daily$Type <- 'Avg'
  plot.avg <- Avg.Daily %>% ggplot(aes(x=Day,y=Volume,group=1)) + geom_line(colour = '#AADC14') + geom_point(colour = '#030F40') + xlab('Day') + ylab(HTML('Consumption (m<sup>3</sup>/day)'))
  
  ## peak periods
  Peak <- Avg.Daily[Avg.Daily$Volume>=quantile(Avg.Daily$Volume,0.9),]
  nb.period <- sum(diff(as.numeric(Peak$wd))>1) + 1
  
  deb.peak <- Peak$wd[c(1,which(diff(as.numeric(Peak$wd))>1)+1)]
  end.peak <- rev(rev(Peak$wd)[c(1,which(diff(rev(as.numeric(Peak$wd)))< -1)+1)])
  message.peak<- paste0('Peak period',ifelse(nb.period>1,'s',''),' occur',ifelse(nb.period>1,'','s'),':<br /><ul>')
  for (i in 1:nb.period){
    if(deb.peak[i]==end.peak[i]){
      temp.peak <- Peak[Peak$wd==deb.peak[i],]
      temp.mess <- paste0('on ',temp.peak$Day,' (',round(temp.peak$Volume,1),' m<sup>3</sup>/day)')
    }else{
      temp.peak <- Peak[as.numeric(Peak$Day)>=as.numeric(deb.peak)[i] & as.numeric(Peak$Day)<=as.numeric(end.peak[i]),]
      temp.mess <- paste0('between ',temp.peak$Day[which.min(as.numeric(Peak$wd))],' and ',temp.peak$Day[which.max(as.numeric(Peak$wd))],' (',round(mean(temp.peak$Volume,1)),' m<sup>3</sup>/day)')
    }
    message.peak<- paste0(message.peak,'<li>',temp.mess)
  }
  message.peak <- paste0(message.peak,'</ul>')
  
  res.peak <- list(Peak = Peak, message = message.peak)  
  
  ## drop consumption periods
  Drop <- Avg.Daily[Avg.Daily$Volume<=quantile(Avg.Daily$Volume,0.1),]
  nb.period <- sum(diff(as.numeric(Drop$wd))>1) + 1
  
  deb.drop <- Drop$wd[c(1,which(diff(as.numeric(Drop$wd))>1)+1)]
  end.drop <- rev(rev(Drop$wd)[c(1,which(diff(rev(as.numeric(Drop$wd)))< -1)+1)])
  message.drop <- paste0('Drop period',ifelse(nb.period>1,'s',''),' occur',ifelse(nb.period>1,'','s'),':<br /><ul>')
  for (i in 1:nb.period){
    if(deb.peak[i]==end.peak[i]){
      temp.drop <- Drop[Drop$wd==deb.drop[i],]
      temp.mess <- paste0('on ',temp.drop$Day,' (',round(temp.drop$Volume,1),' m<sup>3</sup>/day)')
    }else{
      temp.drop <- Drop[as.numeric(Drop$Day)>=as.numeric(deb.drop)[i] & as.numeric(Drop$Day)<=as.numeric(end.drop[i]),]
      temp.mess <- paste0('between ',temp.drop$Day[which.min(as.numeric(Drop$wd))],' and ',temp.drop$Day[which.max(as.numeric(Drop$wd))],' (',round(mean(temp.drop$Volume,1)),' m<sup>3</sup>/day)')
    }
    message.drop <- paste0(message.drop,'<li>',temp.mess)
  }
  message.drop <- paste0(message.drop,'</ul>')
  res.drop <- list(Drop = Drop, message = message.drop)  
  
  result <- list(data=Avg.Daily, plot= plot.avg, peak=res.peak,drop=res.drop)
  return(result)
}
GPD <- Global.Peak.Day(Total.day)

output$so_graphGlobal_CP <- renderPlotly({
  pl.glob <- switch(input$so_stepConsPattern,
                    "0" = GPH$plot,
                    "1" = GPD$plot
  )
  ggplotly(pl.glob,tooltip=c('y','x'))
})

output$so_textGlobal_CP <- renderText({
  messagePeak <- switch (input$so_stepConsPattern,
                         '0' = GPH$peak$message,
                         '1' = GPD$peak$message
  )
  messageDrop <- switch (input$so_stepConsPattern,
                         '0' = GPH$drop$message,
                         '1' = GPD$drop$message
  )
  HTML(messagePeak,'<br />',messageDrop)
})


#### Consumption pattern on selected dates
output$so_cp_seleted.dates <- renderUI({
  end.selected.dates <- max(X$Date)
  if(length(unique(X$H[X$Date==max(X$Date)]))!=24){end.selected.dates <- end.selected.dates-1}
  beg.selected.dates <- switch (input$so_stepConsPattern,
                                '0' = end.selected.dates-6,
                                '1' = end.selected.dates-30)
  dateRangeInput('so_cp_dateRange',
                 label = 'Select the period',
                 start = beg.selected.dates, end = end.selected.dates,separator = " to ", format = "dd/mm/yyyy")
})

detect.max <- function(x,timestep){
  r <- which.max(x)
  r <- ifelse(timestep=='hour',r-1,r)
  return(r)
}
detect.min <- function(x,timestep){
  r <- which.min(x)
  r <- ifelse(timestep=='hour',r-1,r)
  return(r)
}

CP_hour_period <- function(beg,end,data=Total){
  Total.focus <- data %>% dplyr::filter(Date>=beg & Date<=end)
  Total.focus <- Total.focus %>% dplyr::arrange(Date,H)
  Avg.vol.per <- Total.focus %>% group_by(H) %>% dplyr::summarise(avg=mean(Volume))
  
  message <- paste0("On the period ",gsub('-','/',as.character(beg)),"-",gsub('-','/',as.character(end)),"<br /> <ul>")
  
  Peak.hour <- Total.focus %>% group_by(Date) %>% dplyr::summarise(x=detect.max(Volume,'hour'))
  
  temp.peak <- sort(table(Peak.hour$x),decreasing=TRUE)
  peak <- data.frame(H=names(temp.peak),freq = as.numeric(temp.peak))
  peak$Avg.h <- Avg.vol.per$avg[match(peak$H,Avg.vol.per$H)]
  peak$peak.intensity <- peak$Avg.h/mean(Avg.vol.per$avg)
  # peak$Period <- Day.Time$Day.Period[match(peak$H,Day.Time$H)]
  peak$Hour <- Day.Time$Hour[match(peak$H,Day.Time$H)]
  
  if(max(peak$freq)/nrow(Peak.hour)>0.2){
    message <- paste0(message,'<li> A peak is frequently observed at <b>',peak$Hour[1],'</b>')
    message <- paste0(message,' with an average volume of <b>',round(peak$Avg.h[1],1),'</b> m<sup>3</sup>/h (<i>+',percent(peak$peak.intensity[1]-1),' compared to average volume on this period</i>)<br />.')
  }else{
    message <- paste0(message,'<li> There is no proved peak consumption.')
  }
  
  
  Drop.hour <- Total.focus %>% group_by(Date) %>% dplyr::summarise(x=detect.min(Volume,'hour'))
  
  temp.drop <- sort(table(Drop.hour$x),decreasing=TRUE)
  drop <- data.frame(H=names(temp.drop),freq = as.numeric(temp.drop))
  drop$Avg.h <- Avg.vol.per$avg[match(drop$H,Avg.vol.per$H)]
  drop$drop.intensity <- drop$Avg.h/mean(Avg.vol.per$avg)
  drop$Hour <- Day.Time$Hour[match(drop$H,Day.Time$H)]
  
  if(max(drop$freq)/nrow(Drop.hour)>0.2){
    message <- paste0(message,'<li> The lowest consumption is frequently observed at <b>',drop$Hour[1],'</b>')
    message <- paste0(message,' with an average volume of <b>',round(drop$Avg.h[1],1),'</b> m<sup>3</sup>/h (<i>',percent(drop$drop.intensity[1]-1),' compared to average volume on this period</i>)<br />.')
  }else{
    message <- paste0(message,'<li> There is no proved peak consumption.')
  }
  message <- paste0(message,'</ul>')
  result <- list(data=Total.focus, message =message)
  return(result)
}

CP_day_period <- function(beg,end,data=Total.day){
  Total.focus <- data %>% dplyr::filter(Date>=beg & Date<=end)
  Total.focus <- Total.focus %>% dplyr::arrange(Date)
  Avg.vol.per <- Total.focus %>% group_by(wd) %>% dplyr::summarise(avg=mean(Volume))
  message <- paste0("On the period ",gsub('-','/',as.character(beg)),"-",gsub('-','/',as.character(end)),"<br /> <ul>")
  
  Peak.day <- Total.focus %>% group_by(week) %>% dplyr::summarise(x=detect.max(Volume,'day'))
  
  temp.peak <- sort(table(Peak.day$x),decreasing=TRUE)
  peak <- data.frame(wd=levels(Total.focus$wd)[as.numeric(names(temp.peak))],freq = as.numeric(temp.peak))
  full.week.days <- c('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
  peak$Day <- full.week.days[as.numeric(names(temp.peak))]
  peak$Avg.d <- Avg.vol.per$avg[match(peak$wd,Avg.vol.per$wd)]
  peak$peak.intensity <- peak$Avg.d/mean(Avg.vol.per$avg)
  
  if(max(peak$freq)/nrow(Peak.day)>0.2){
    message <- paste0(message,'<li> A peak is frequently observed on <b>',peak$Day[1],'</b>')
    message <- paste0(message,' with an average volume of <b>',round(peak$Avg.d[1],1),'</b> m<sup>3</sup>/h (<i>+',percent(peak$peak.intensity[1]-1),' compared to average volume on this period</i>).')
  }else{
    message <- paste0(message,'<li> There is no proved peak consumption.')
  }
  
  
  Drop.day <- Total.focus %>% group_by(week) %>% dplyr::summarise(x=detect.min(Volume,'day'))
  
  temp.drop <- sort(table(Drop.day$x),decreasing=TRUE)
  
  drop <- data.frame(wd=levels(Total.focus$wd)[as.numeric(names(temp.drop))],freq = as.numeric(temp.drop))
  drop$Day <- full.week.days[as.numeric(names(temp.drop))]
  drop$Avg.d <- Avg.vol.per$avg[match(drop$wd,Avg.vol.per$wd)]
  drop$drop.intensity <- drop$Avg.d/mean(Avg.vol.per$avg)
  
  if(max(drop$freq)/nrow(Drop.day)>0.2){
    message <- paste0(message,'<li> The lowest consumption is frequently observed on <b>',drop$Day[1],'</b>')
    message <- paste0(message,' with an average volume of <b>',round(drop$Avg.d[1],1),'</b> m<sup>3</sup>/h (<i>',percent(drop$drop.intensity[1]-1),' compared to average volume on this period</i>).')
  }else{
    message <- paste0(message,'<li> There is no proved peak consumption.')
  }
  message <- paste0(message,'</ul>')
  result <- list(data=Total.focus, message =message)
  return(result)
}

output$so_cp_seleted.plot <- renderPlotly({
  dates.selected <- as.Date(as.character(input$so_cp_dateRange))
  if(input$boxplot_consPatt=="1"){
    temp <- switch(input$so_stepConsPattern,
                   "0" = CP_hour_period(dates.selected[1],dates.selected[2],Total) ,
                   "1" =  CP_day_period(dates.selected[1],dates.selected[2],Total.day) 
    )
    pl.selected.plot <- switch(input$so_stepConsPattern,
                               "0" = temp$data %>% ggplot(aes(x=Date.Time,y=Volume)) ,
                               "1" = temp$data %>% ggplot(aes(x=Date,y=Volume))
    )
    
    pl.selected.plot <- pl.selected.plot + 
      geom_line(colour = '#AADC14') + 
      geom_point(colour = '#030F40') + 
      xlab('') + ylab(HTML(paste0('Consumption (m<sup>3</sup>/',ifelse(input$so_stepConsPattern=='0','h','day'),')')))
  }else{
    temp <- filter(Xfull,Date>=dates.selected[1] & Date <= dates.selected[2])
    pl.selected.plot <- switch(input$so_stepConsPattern,
                               "0" = temp %>% ggplot(aes(Date.Time, Consumption,group=Date.Time)) ,
                               "1" = temp %>% group_by(service_point_sn,Date) %>% dplyr::summarise(Consumption=sum(Consumption)) %>% ggplot(aes(Date, Consumption,group=Date)) 
    )
    pl.selected.plot <- pl.selected.plot + geom_boxplot(outlier.shape=NA, fill = rgb(170,220,20,maxColorValue = 255), colour = rgb(3,15,64,maxColorValue = 255))
  }
  ggplotly(pl.selected.plot)
})

output$so_cp_seleted.message <- renderText({
  dates.selected <- as.Date(as.character(input$so_cp_dateRange))
  temp <- switch(input$so_stepConsPattern,
                 "0" = CP_hour_period(dates.selected[1],dates.selected[2],Total) ,
                 "1" =  CP_day_period(dates.selected[1],dates.selected[2],Total.day) 
  )
  HTML(temp$message)
})

output$so_stepConsPattern_tmp <- renderUI({
  if(input$so_stepConsPattern==0){
    radioButtons("boxplot_consPatt", label = "Display", choices=list("Average values"=1),selected = 1,inline = TRUE)
  }else{
    radioButtons("boxplot_consPatt", label = "Display", choices=list("Average values"=1,"Detailed values"=2),selected = 1,inline = TRUE)
  }
  })
