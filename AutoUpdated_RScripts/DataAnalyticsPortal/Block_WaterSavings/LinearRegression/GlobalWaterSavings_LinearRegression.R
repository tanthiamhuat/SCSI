rm(list=ls())  # remove all variables
memory.limit(1e+13)
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

require(pacman)
pacman::p_load(plyr,dplyr,lubridate,stringr,readxl,xlsx,tidyr,ggplot2,forecast,TSA,zoo,xts,corrplot)

AMR_Data <- read.xlsx("/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DataAnalyticsPortal/Block_WaterSavings/LinearRegression/amr-data_20180301.xlsx","Sheet1")
names(AMR_Data)
explanatory.var <- c("daily.temp","daily.rainfall","day_of_wk","Public.holiday","School.holiday","price.hike","app","leak.alarm","month")
corrplot(cor(AMR_Data[,explanatory.var]),type = 'upper',diag = FALSE)

## Relative Importance
library(relaimpo)
lmMod_All <- lm(overall.LPCD ~ daily.temp+Public.holiday+price.hike+leak.alarm+month,data = AMR_Data)  # fit lm() model
relImportance <- calc.relimp(lmMod_All, type = "lmg", rela = TRUE)  # calculate relative importance scaled to 100
sort(relImportance$lmg, decreasing=TRUE)  # relative importance

lm1 <- lm(overall.LPCD ~ daily.temp+Public.holiday+price.hike+leak.alarm+month,data = AMR_Data) 
summary(lm1)
lm2 <- lm(overall.LPCD ~ daily.temp+Public.holiday+app+leak.alarm+month,data = AMR_Data) 
summary(lm2)
lm3 <- lm(overall.LPCD ~ daily.temp+Public.holiday+price.hike+app+leak.alarm+month,data = AMR_Data) 
summary(lm3)