local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'data/CustomerProfileBenchmark.RData'))

output$CustomerProfileBenchmark_table <- DT::renderDataTable(Benchmark_Final, 
                                                              filter='bottom',options = list(scrollX=TRUE,pageLength=25)
)

output$lastUpdated_CustomerProfileBenchmark <- renderText(Updated_DateTime_CustomerProfileBenchmark)