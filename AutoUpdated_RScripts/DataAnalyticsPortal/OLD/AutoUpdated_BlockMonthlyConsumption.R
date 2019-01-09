rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate)

load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")

BlockMonthlyConsumption <- Punggol_All %>% 
                      dplyr::filter(meter_type=="SUB") %>%
                      group_by(Y,M,block) %>%
                      dplyr::summarise(MonthlyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

BlockMonthlyConsumption <- BlockMonthlyConsumption[-c(1,2),] # remove first two rows

BlockMonthlyConsumption_wide <- spread(BlockMonthlyConsumption,Y,MonthlyConsumption)
colnames(BlockMonthlyConsumption_wide)[3] <-"BlockMonthlyConsumption_2016"
colnames(BlockMonthlyConsumption_wide)[4] <-"BlockMonthlyConsumption_2017"

BlockMonthlyConsumption_wide$M <- factor(month.abb[BlockMonthlyConsumption_wide$M],
                                        levels = month.abb)

save(BlockMonthlyConsumption_wide,file="/srv/shiny-server/DataAnalyticsPortal/data/BlockMonthlyConsumption.RData")
write.csv(BlockMonthlyConsumption_wide,file="/srv/shiny-server/DataAnalyticsPortal/data/BlockMonthlyConsumption.csv")
# https://plot.ly/r/bar-charts/
Months <- as.character(unique(BlockMonthlyConsumption_wide$M))
PG_B1 <- BlockMonthlyConsumption_wide %>% dplyr::filter(block=="PG_B1") %>% 
         dplyr::select_("BlockMonthlyConsumption_2016","BlockMonthlyConsumption_2017")
PG_B2 <- BlockMonthlyConsumption_wide %>% dplyr::filter(block=="PG_B2") %>% 
  dplyr::select_("BlockMonthlyConsumption_2016","BlockMonthlyConsumption_2017")
PG_B3 <- BlockMonthlyConsumption_wide %>% dplyr::filter(block=="PG_B3") %>% 
  dplyr::select_("BlockMonthlyConsumption_2016","BlockMonthlyConsumption_2017")
PG_B4 <- BlockMonthlyConsumption_wide %>% dplyr::filter(block=="PG_B4") %>% 
  dplyr::select_("BlockMonthlyConsumption_2016","BlockMonthlyConsumption_2017")
PG_B5 <- BlockMonthlyConsumption_wide %>% dplyr::filter(block=="PG_B5") %>% 
  dplyr::select_("BlockMonthlyConsumption_2016","BlockMonthlyConsumption_2017")
PG_B1 <- PG_B1[,-1]
PG_B2 <- PG_B2[,-1]
PG_B3 <- PG_B3[,-1]
PG_B4 <- PG_B4[,-1]
PG_B5 <- PG_B5[,-1]
data <- data.frame(Months,PG_B1,PG_B2,PG_B3,PG_B4,PG_B5)


p <- plot_ly(BlockMonthlyConsumption_wide, x = ~M, y = ~BlockMonthlyConsumption_2016/1000, type = 'bar', name = 'Year 2016',
             marker = list(color = "green")) %>%
  add_trace(BlockMonthlyConsumption_wide, x = ~M, y = ~BlockMonthlyConsumption_2017/1000, type = 'bar', name = 'Year 2017',
            marker = list(color = "orange")) %>%
  layout(xaxis = list(title = 'Month'),
         yaxis = list(title = 'Block Consumption (m<sup>3</sup>)'),  barmode = 'dodge',
         legend = list(x = 0, y = -0.30))
p

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MonthlyConsumption successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)