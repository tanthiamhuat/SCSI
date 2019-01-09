local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'Profiling_V2/Output/01-Punggol_Indicators.RData'))
load(paste0(path,'Profiling_V2/Output/04-Final_Profile.RData'))

Customers <- inner_join(Y,indicator, by=c("service_point_sn"="ID","block")) %>% 
             dplyr::mutate(AverageMonthlyConsumption=round(amc)) %>%
             dplyr::select_("service_point_sn","block","room_type","num_house_member","AverageMonthlyConsumption","AverageMonthlyOccupancy","Profile") 

Customers_Details <- inner_join(Customers,family_details,by=c("service_point_sn","num_house_member")) %>%
                     dplyr::select_("service_point_sn","block","room_type","num_house_member","online_status","AverageMonthlyConsumption",
                                    "AverageMonthlyOccupancy","Profile") 

Conso_LPCD_extracted <- Conso_LPCD %>% dplyr::select_("Date","Profile","LPCD")
Conso_LPCD_wide <- spread(Conso_LPCD_extracted,Profile,LPCD)  
Conso_LPCD_xts <- xts(Conso_LPCD_wide[2:5],order.by = Conso_LPCD_wide$Date)

output$CustomerSegmentation_plot<- renderDygraph({
  graph <- dygraph(Conso_LPCD_xts,main="Average LPCD Per Profile") %>%
    dyRangeSelector() %>%
    dyAxis("y",label=HTML('Average LPCD (litres/capita/day)')) %>%
    dyOptions(colors=c("#e41a1c","#377eb8","#4daf4a","#984ea3"))
  graph
})

output$CustomerSegmentation_info <- renderUI({
   fluidRow(
    column(5,HTML('<table>
                   <tr>
                   <td style="width:270px;"> <img src="CustomerSegmentation.png" heigth="170%" width="170%"> </td>
                   <td>
                   </td>
                   </tr>
                   </table>')),
    column(7,HTML('<table>
                    <tr>
                    <td> <align="justify">Customer profiles are grouped based on:<br>
                                         (a) Consumption Level (L1,L2) and	<br>
                                         (b) Consumption Variation (G1,G2,G3,G4). <br>
                                         Hence, we have 4 Profiles (P1,P2,P3 and P4), where <br>
                                         P1 = (L1G1,L1G4), <br>
                                         P2 = (L1G2,L1G3), <br>
                                         P3 = (L2G1,L2G4) and <br>
                                         P4 = (L2G2,L2G3).
                    </td>
                    </tr>
                    </table>'))
   )
})

output$Customers_table <-  DT::renderDataTable(Customers_Details,
                                               filter='bottom',options = list(scrollX=TRUE,pageLength=10)
)

output$lastUpdated_CustomerSegmentation <- renderText(Updated_DateTime_CustomerSegmentation)