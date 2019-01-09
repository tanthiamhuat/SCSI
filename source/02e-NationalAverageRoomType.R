local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'data/NationalAverageRoomType.RData'))

thisyear <- year(today())
lastyear <- thisyear-1

CombinedSPGroupPunggol_lastyear_long <- gather(CombinedSPGroupPunggol_lastyear,Month,Consumption,Jan:Dec,factor_key=TRUE)
CombinedSPGroupPunggol_thisyear_long <- gather(CombinedSPGroupPunggol_thisyear,Month,Consumption,Jan:Dec,factor_key=TRUE)
 
output$NationalAverageRoomType_table <- DT::renderDataTable(DT::datatable({
  if (input$Year==thisyear) {
    data <- CombinedSPGroupPunggol_thisyear
  } else if (input$Year==lastyear) {
    data <- CombinedSPGroupPunggol_lastyear
  }
  
  if (input$RoomType=="All"){
    data_selected <- data
  } else {
    data_selected <- data[data$room_type == input$RoomType,]
  }
},options = list(pageLength = 10, dom = 'tip'), rownames = FALSE
))

output$NationalAverageRoomType_plot <- renderPlot({
  if (input$RoomType!="All"){
    if (input$Year==thisyear) {
      data_filtered <- CombinedSPGroupPunggol_thisyear_long[CombinedSPGroupPunggol_thisyear_long$room_type == input$RoomType,]
    } else if (input$Year==lastyear) {
      data_filtered <- CombinedSPGroupPunggol_lastyear_long[CombinedSPGroupPunggol_lastyear_long$room_type == input$RoomType,]
    }
    title_string <- paste("National Average (SPGroup) versus Punggol Consumption for Room Type ",input$RoomType," in year ",input$Year,sep="")
  }
 
  p <-ggplot(data=data_filtered, aes(x=Month, y=Consumption, fill=Source)) +
    geom_bar(stat="identity", position=position_dodge()) +
    geom_text(aes(label=Consumption), position=position_dodge(width=0.9),hjust=0.5,vjust=1.1,size=5.0) +
    xlab("Month") +
    ylab(bquote('Consumption ('*~ m^3*' Per Household)')) +
    scale_fill_manual(values=c("#FF7F00", "#00CC00")) +
    ggtitle(title_string) +
    theme_grey(base_size = 18) + theme(legend.position = "bottom")
  p
})

output$NationalAverageRoomType_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">The SPGroup data is downloaded from https://www.spgroup.com.sg/what-we-do/billing.
                   </td>
                   </tr>
                   </table>'))
    )
}) 

output$lastUpdated_NationalAverageRoomType <- renderText(Updated_DateTime_NationalAverageRoomType)