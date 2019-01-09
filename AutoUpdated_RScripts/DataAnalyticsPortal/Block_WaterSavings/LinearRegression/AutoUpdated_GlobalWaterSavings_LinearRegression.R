rm(list=ls())  # remove all variables
memory.limit(1e+13)
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

require(pacman)
pacman::p_load(plyr,dplyr,lubridate,stringr,readxl,xlsx,tidyr,ggplot2,forecast,TSA,zoo,xts,corrplot)

library(chron)
WaterSavings_DF <- data.frame(Date=seq.Date(as.Date("2016-03-14"),today()-1,by='days'))
WaterSavings_DF$weekday <- as.numeric(format(WaterSavings_DF$Date, "%w"))
WaterSavings_DF$weekday[WaterSavings_DF$weekday == 0] <- 7 # replace 0 with 7
WaterSavings_DF$weekend <- (format(WaterSavings_DF$Date, "%u") %in% c(6, 7))*1
WaterSavings_DF$month <- month(WaterSavings_DF$Date)

holidays <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/Holidays_2014_2017.csv",sep = ";")
colnames(holidays)[2] <- "Date"
holidays$Date <- dmy(holidays$Date)
WaterSavings_DF$Public.holiday <- (WaterSavings_DF$Date %in% holidays$Date)*1

load("/srv/shiny-server/DataAnalyticsPortal/data/Weather.RData")
Weather <- Weather %>% dplyr::select_("Date","Tavg","Rainf")

WaterSavings_DF <- inner_join(WaterSavings_DF,Weather,by="Date")
WaterSavings_DF$price.hike <- (WaterSavings_DF$Date >= as.Date("2017-07-01"))*1
WaterSavings_DF$app <- (WaterSavings_DF$Date >= as.Date("2017-06-10"))*1
WaterSavings_DF$leak.alarm <- (WaterSavings_DF$Date >= as.Date("2016-04-20"))*1

DailyLPCD <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.csv",sep = ",")
DailyLPCD$date <- as.Date(DailyLPCD$date)
WaterSavings_DF <- inner_join(WaterSavings_DF,DailyLPCD,by=c("Date"="date"))
WaterSavings_DF <- na.omit(WaterSavings_DF)

explanatory.var <- c("Tavg","Rainf","weekend","Public.holiday","price.hike","app","leak.alarm","weekday","month")
corrplot(cor(WaterSavings_DF[,explanatory.var]),type = 'upper',diag = FALSE)

# WaterSavings_DF$weekday <- as.factor(WaterSavings_DF$weekday)
# WaterSavings_DF$weekend <- as.factor(WaterSavings_DF$weekend)
# WaterSavings_DF$month <- as.factor(month(WaterSavings_DF$Date))
# WaterSavings_DF$Public.holiday <- as.factor(WaterSavings_DF$Public.holiday)
# WaterSavings_DF$price.hike <- as.factor(WaterSavings_DF$price.hike)
# WaterSavings_DF$app <- as.factor(WaterSavings_DF$app)
# WaterSavings_DF$leak.alarm <- as.factor(WaterSavings_DF$leak.alarm)
var2explain <- 'DailyLPCD'

### 2 couples of variables are highly correlated (weekday+weekend,price.hike+app)
### Linear regression to select the best set of variable
lm.var <- function(variable,var2explain,Intercept = TRUE){
  fml.char <- paste(var2explain,paste(variable,collapse = ' + '),sep=' ~ ')
  if(!Intercept) fml.char <- paste0(fml.char,' -1')
  mod.out <- lm(formula = as.formula(fml.char),data = WaterSavings_DF)
  return(mod.out)
}
# model w/ price hike & weekend
var1 <- c("Tavg","Public.holiday","leak.alarm","month","weekday","price.hike")
mod1 <- lm.var(var1,var2explain)

# model w/ price hike & weekday
var2 <- c("Tavg","Public.holiday","leak.alarm","month","weekday","app")
mod2 <- lm.var(var2,var2explain)

# model w/ app & weekend
var3 <- c("Tavg","Public.holiday","leak.alarm","month","weekend","price.hike")
mod3 <- lm.var(var3,var2explain)

# model w/ app & weekday
var4 <- c("Tavg","Public.holiday","leak.alarm","month","weekend","app")
mod4 <- lm.var(var4,var2explain)

ComparMod <- data.frame(Model = c('PriceHike+Weekend','PriceHike+TimeEffect','APP+Weekend','APP+TimeEffect'),
                        R2=c(summary(mod1)$adj.r.squared,summary(mod2)$adj.r.squared,summary(mod3)$adj.r.squared,summary(mod4)$adj.r.squared),
                        AIC=c(AIC(mod1),AIC(mod2),AIC(mod3),AIC(mod4)),stringsAsFactors = FALSE)
cat('Best model w/ combination - AIC:',ComparMod$Model[which.min(ComparMod$AIC)],'\n')
cat('Best model w/ combination - R2:',ComparMod$Model[which.max(ComparMod$R2)],'\n')

## Relative Importance
library(relaimpo)
lmMod_All <- lm(DailyLPCD ~ Tavg + Rainf + Public.holiday + price.hike + app + leak.alarm + weekday + month,
                data = WaterSavings_DF)  # fit lm() model
relImportance <- calc.relimp(lmMod_All, type = "lmg", rela = TRUE)  # calculate relative importance scaled to 100
sort(relImportance$lmg, decreasing=TRUE)  # relative importance

######
tslm.var <- function(variable,var2explain,Intercept = TRUE, trend =TRUE, season=TRUE){
  fml.char <- paste(var2explain,paste(variable,collapse = ' + '),sep=' ~ ')
  if(trend){fml.char <- paste0(fml.char,' + trend')}
  if(season){fml.char <- paste0(fml.char,' + season')}
  if(!Intercept) fml.char <- paste0(fml.char,' -1')

  mod.out <- tslm(formula = as.formula(fml.char),data = WaterSavings_DF)
  return(mod.out)
}

y7 <- ts(WaterSavings_DF$DailyLPCD,frequency = 7)

varF <-  c("Tavg","Public.holiday","leak.alarm","month","app","price.hike") #list of the variables used in the final model
## previously does not include price.hike

WaterSavings_DF$weekend <- NULL
tslm.mod_Trend <-  tslm.var(variable = varF, var2explain = 'y7', Intercept = FALSE)
tslm.mod_NoTrend <- tslm.var(variable = varF, var2explain = 'y7', Intercept = FALSE,trend = F)  
## trend == time series trend
## season == day

AIC(tslm.mod_Trend) < AIC(tslm.mod_NoTrend) # compare model w/ or w/o trend

Coef <- data.frame(Variable = names(tslm.mod_NoTrend$coefficients), All = tslm.mod_NoTrend$coefficients)
Coef1 <- data.frame(Variable = names(tslm.mod_Trend$coefficients), All = tslm.mod_Trend$coefficients)

summary(tslm.mod_NoTrend)