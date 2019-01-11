load("/srv/shiny-server/DataAnalyticsPortal/data/CustomerProfileWaterSavings_Weekly.RData")

output$CustomerProfile_WaterSavings_plot <- renderDygraph({
  graph <- dygraph(CustomerProfileWaterSavings_Weekly, main = "Water Savings Per Profile (Weekly)") %>%
    dyRangeSelector() %>%
    dyAxis("y",label=HTML('Water Savings (litres)')) %>%
    dyOptions(colors=c("#e41a1c","#377eb8","#4daf4a","#984ea3"))
  graph
})

output$lastUpdated_CustomerProfileWaterSavings <- renderText(Updated_DateTime_CustomerProfileWaterSavings)

