local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/block_forecast.RData'))

WeekNumber <- max(block_forecast_result$week_number)
block_forecast_result <- tail(block_forecast_result,12) %>% as.data.frame()
block_forecast_result <- block_forecast_result %>% 
                         dplyr::mutate(TotalActualConsumption=TotalActualConsumption/1000,
                                       TotalForecastedConsumption=TotalForecastedConsumption/1000)
block_forecast_result[1] <- NULL

block_forecast_result_long <- gather(block_forecast_result,Consumption,Values,TotalActualConsumption,TotalForecastedConsumption,factor_key=TRUE)
  
output$Block_Forecast_plot <- renderPlot({
  title_str <- paste("Block Consumption (Actual and Forecasted) of Previous Week (",WeekNumber,")",sep="")
  p <-ggplot(data=block_forecast_result_long, aes(x=block, y=Values, fill=Consumption)) + 
    geom_bar(stat="identity", position=position_dodge()) +
    geom_text(aes(label=Values), position=position_dodge(width=0.9),hjust=0.5,vjust=1.1,size=5.0) +
    xlab("Block") +
    ylab(bquote('Consumption ('*m^3*')')) +
    scale_fill_manual(values=c("#FF7F00", "#00CC00")) + 
    ggtitle(title_str) + theme_grey(base_size = 18) + theme(legend.position = "bottom")
  p
})

output$block_forecast_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td style="width:50px;"> </td>
                   <td> <p align="justify">The above plot would display previous two weeks data on each Monday before 11.40am,
                   as the weekly consumption data is not updated yet. Hence the updated plot for the previous week would be available after 11.40am on Monday.</p></td>
                   </tr>
                   </table>'))
  )
})

output$Overall_Forecast_plot <- renderDygraph({
  graph <- dygraph(TotalActualForecastedConsumption_xts, main = "Total Consumption (Actual vs Forecasted)") %>%
    dyRangeSelector() %>%
    dyAxis("y",label=HTML('Consumption in m<sup>3</sup>'), valueRange = c(1200, 4200)) %>%
    dyOptions(colors = c("#FF7F00", "#00CC00"))
  graph
})

output$lastUpdated_BlockForecast <- renderText({Updated_DateTime_BlockForecast})