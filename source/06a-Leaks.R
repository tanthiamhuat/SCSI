local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

# https://github.com/ThomasSiegmund/D3TableFilter
# https://thomassiegmund.shinyapps.io/interaction/
# install.packages("devtools")
# devtools::install_github("ThomasSiegmund/D3TableFilter")
library(D3TableFilter)

load(paste0(path,'data/leak_alarm_PunggolYuhua.RData'))
leak_alarm_PunggolYuhua$min5flow_ave <- sapply(leak_alarm_PunggolYuhua$min5flow_ave, gsub, pattern = ",", replacement= ".")
leak_alarm_PunggolYuhua$min5flow_ave <- as.numeric(leak_alarm_PunggolYuhua$min5flow_ave)
leak_alarm_PunggolYuhua$start_date <- as.Date(leak_alarm_PunggolYuhua$start_date)
leak_alarm_PunggolYuhua$end_date <- as.Date(leak_alarm_PunggolYuhua$end_date)
leak_alarm_PunggolYuhua$family_id <- as.numeric(leak_alarm_PunggolYuhua$family_id)
leak_alarm_PunggolYuhua$duration <- as.numeric(leak_alarm_PunggolYuhua$duration)

EditDF <- reactiveValues()
EditDF$df.leaks <- leak_alarm_PunggolYuhua
EditDF$rowEdit <- c()
EditDF$colEdit <- c()
EditDF$valEdit <- c()

# path <- '/srv/shiny-server/DataAnalyticsPortal/data/LeakAlarmWaterFall.csv'
# get_waterfall <- function(path) {
#   colors <- c("#ff0000", "#32CD32")
#   path <- '/srv/shiny-server/DataAnalyticsPortal/data/LeakAlarmWaterFall.csv'
#   df <- read.csv(path, stringsAsFactors = FALSE)
#   df <- df %>% mutate(name = paste(MonthYear, status),
#                       color = rep(colors, length.out = nrow(df)),
#                       y = value) %>%
#     select(name, y, color)
#   highchart() %>%
#     hc_title(text = 'Waterfall plot of number of Open and Close leaks for each month.', style = list(fontSize = "15px")) %>%
#     hc_chart(type = 'waterfall') %>%
#     hc_xAxis(categories = df$name) %>%
#     hc_add_series(df,name = 'Leaks',showInLegend = FALSE)
# }

output$LeakAlarm_D3table <- renderD3tf({
  # Define table properties. See http://tablefilter.free.fr/doc.php
  # for a complete reference
  tableProps <- list(
    btn_reset = TRUE,
    grid_enable_cols_resizer = TRUE,
    paging = TRUE, 
    paging_length = 15,
    col_types = c("Number",rep("String",4),rep("Date",2),rep("Number",2),rep("String",3)),
   # results_per_page = JS("['Rows per page',[15,10,5]]") 
   results_per_page = JS("['Rows per page',[10,5,5]]") 
  );

  d3tf(leak_alarm_PunggolYuhua, #values$df.leaks,
       tableProps = tableProps,
       edit=c("col_11"),
       enableTf = TRUE,
       extensions = list(
         list(name = "sort")
       ),
       #extensions = list('Sort', 'ColsVisibility', 'ColumnsResizer', 'FiltersRowVisibility'),
       showRowNames = FALSE,
       tableStyle = "table table-bordered");
})

observeEvent(input$LeakAlarm_D3table_edit,{
  # each time an edit is done, we keep the modification in memory
  # values$df.leaks[input$LeakAlarm_D3table_edit$row, input$LeakAlarm_D3table_edit$col] <- (input$LeakAlarm_D3table_edit$val);
  EditDF$rowEdit <- c(EditDF$rowEdit,input$LeakAlarm_D3table_edit$row)
  EditDF$colEdit <- c(EditDF$colEdit,input$LeakAlarm_D3table_edit$col)
  EditDF$valEdit <- c(EditDF$valEdit,input$LeakAlarm_D3table_edit$val)
})

observeEvent(input$save_leak,{
  if(length(EditDF$valEdit)>0){
    for(i in 1:length(EditDF$valEdit)){
      EditDF$df.leaks[EditDF$rowEdit[i],EditDF$colEdit[i]] <- EditDF$valEdit[i]
    }
    write.csv2(EditDF$df.leaks,file=paste0(path,'data/leak_alarm_PunggolYuhua.csv'),row.names=FALSE, sep = ",")
    # once all the modifications have been done, we clear the row/col/val
    EditDF$rowEdit <- c()
    EditDF$colEdit <- c()
    EditDF$valEdit <- c()
  }
  #leak_alarm_PunggolYuhua <- read.csv2("/srv/shiny-server/DataAnalyticsPortal/data/leak_alarm_PunggolYuhua.csv", header = TRUE, sep = ",",fill = TRUE,stringsAsFactors=FALSE)
  leak_alarm_PunggolYuhua <- fread(paste0(path,'data/leak_alarm_PunggolYuhua.csv'),showProgress = T)
  save(leak_alarm_PunggolYuhua,file=paste0(path,'data/leak_alarm_PunggolYuhua.RData'))
})

output$downloadLeakSeverityData <- downloadHandler(
  filename = function() { paste('LeakAlarmPunggolYuhua', '.csv', sep='') },
  content = function(file) {
    LeakAlarmPunggolYuhuaCSV <- read.csv2(paste0(path,'data/leak_alarm_PunggolYuhua.csv'),header = TRUE,sep=",")
    #write.csv2(EditDF$df.leaks,file=file,row.names=FALSE,sep=",")
    write.csv(LeakAlarmPunggolYuhuaCSV, file, row.names = FALSE)
    }
)

x <- c("Moderate", "Severe", "Critical")
leak_alarm_PunggolYuhua_Extracted <- leak_alarm_PunggolYuhua %>%
  dplyr::select_("service_point_sn","site","status","severity") %>%
  dplyr::group_by(site,status,severity) %>%
  dplyr::summarise(Count=n()) %>%
  slice(match(x, severity)) %>%
  dplyr::mutate(site_status=ifelse(site=="Punggol" & status=="Open","Punggol_Open",
                            ifelse(site=="Punggol" & status=="Close","Punggol_Close",
                            ifelse(site=="Yuhua" & status=="Open","Yuhua_Open",
                            ifelse(site=="Yuhua" & status=="Close","Yuhua_Close"))))) %>%
  as.data.frame()

leak_alarm_PunggolYuhua_Extracted$severity <- as.factor(leak_alarm_PunggolYuhua_Extracted$severity)

output$LeakInfo_plot <- renderPlotly({
  pal <- c("#66B2FF", "#0000CC", "#00FF00","#006600")
  p <- plot_ly(leak_alarm_PunggolYuhua_Extracted, x = ~Count, y = ~severity, color = ~site_status, orientation = 'h', colors = pal) %>%
    layout(title = "Leak Severity in Punggol and Yuhua", 
           xaxis = list(title = "Number of Records"),
           yaxis = list(title = "Severity",tickangle = -90)) %>%
    config(displayModeBar = F)
  p
  
})

output$LeakSeverity_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">
                   The table above displays one row per leak case, which can be sorted by clicking on the column name. <br>
                   The leak status is: <br>
                   - Open if the Min5_flow of the last 3 days is above 0 <br>
                   - Close if the Min5_flow of the last 3 days is = 0 <br>
                     The column "min5flow_ave" is the average of the min flow 5 over the last 3 days from:<br>
                   - now if the leak is opened.<br>
                   - the end_date if the leak is closed.<br>

                   The duration is either the number of days between: <br>
                   - start_date and end_date (inclusively), for Close status. <br>
                   - start_date and yesterday (inclusively), for Open status. <br>
                   Severity of leak is based on its duration and min5flow_ave.<br>
                   a)	Moderate: duration < 5 consecutive days & min5flow_ave < 10 litres per hour.<br>
                   b)	Severe: duration < 5 consecutive days & min5flow_ave >= 10 litres per hour OR 
                              duration >= 5 consecutive days & min5flow_ave < 10 litres per hour.<br>
                   c) Critical: duration >= 5 consecutive days & min5flow_ave >= 10 litres per hour.<br>

                   The column online_status = ACTIVE refers to online customers whereas online_status = INACTIVE
                   refers to offline customers.<br>
                   If start_date is < AccountActivatedDate, the online_status is INACTIVE.<br>
                   Take note here for site = Yuhua cases, min5flow_ave refers to the minimum consumption above zero for 3 consecutive days.

                   </td>
                   </tr>
                   </table>'))
    )
})

output$lastUpdated_LeakSeverity <- renderText({Updated_DateTime_LeakAlarm})

# output$QtyLeaksOpenClose_plot <- renderHighchart({
#   get_waterfall(path)
# })

output$QtyLeaksOpenClose_plot <- renderPlotly({
  data <- read.csv(paste0(path,'data/LeakAlarmWaterFall.csv'))
  data$MonthYearLeak <- paste(data$MonthYear,data$leak)
  data$MonthYearLeak <- factor(data$MonthYearLeak,levels = unique(data$MonthYearLeak))
  data$end <- cumsum(data$value)
  data$start <- c(0, head(data$end, -1))
  data$X <- seq(1:nrow(data))
  colors <- rep(c("#ff0000", "#32CD32"),nrow(data)/2)
  p <- ggplot(data, aes(MonthYearLeak,text = paste("Leak:", data$value))) +
       geom_rect(aes(x = MonthYearLeak, xmin = X - 0.35, xmax = X + 0.35, 
                 ymin = end, ymax = start), fill = colors) +
    theme(axis.text.x = element_text(angle=45,size=8),
          axis.title.x=element_blank()
          ) + theme(plot.margin=unit(c(0.01,1,1.2,1),"cm")) +  # top,right,bottom,left
    ggtitle('Waterfall plot of number of Open (red) and Close (green) leaks for each month.') +
    scale_y_continuous(breaks=seq(0,max(data$end)+2,1)) 
  p
  ggplotly(p) %>% config(displayModeBar = F)
})

output$QtyLeaksOpenClose_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">
                   The waterfall plot above shows the number of Leak Alarm which is Open and Close per month.
                   For example, there are 9 Leak Alarms which are Open for the month of Nov in year 2017, and
                   7 Leak Alarms which are Close for the month of Nov in year 2017. <br>
                   In June 2018, the number of Open Leaks is significantly higher at 34 due to the Yuhua leaks.
                   </td>
                   </tr>
                   </table>'))
    )
})