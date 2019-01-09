rm(list=ls())  # remove all variables
memory.limit(1e+13)
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

require(pacman)
pacman::p_load(plyr,dplyr,lubridate,stringr,readxl,xlsx,tidyr,ggplot2,forecast,TSA,zoo,xts,corrplot)

setwd("/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DataAnalyticsPortal/Block_WaterSavings/LinearRegression")

# Files sent by Rolland to extract the explanatory variables
saveX <- read_excel('water savings from Suez project (March 2016 to Dec 2017).xlsx')[,1:11]
saveX$date <- as.Date(saveX$date)
colnames(saveX) <- str_replace_all(colnames(saveX),pattern = '[ ]',replacement = '.')
saveX <- na.omit(saveX)

X <- read.csv('/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD_Block.csv')
X_Block <- spread(X,block,DailyLPCD)
X_Block <- na.omit(X_Block)

X_Block$date <- as.Date(X_Block$date)
which_block <- X_Block[,1:2]

X_Block <- inner_join(which_block,saveX[,c(1,4:ncol(saveX))],by = c('date')) 
explanatory.var <- c("daily.temp","daily.rainfall","weekend","Public.holiday","price.hike","app","leak.alarm","time.effect")
corrplot(cor(X_Block[,explanatory.var]),type = 'upper',diag = FALSE)

var2explain <- names(X_Block)[2]

### 2 couples of variables are highly correlated 
### Linear regression to select the best set of variable
lm.var <- function(variable,var2explain,Intercept = TRUE){
  fml.char <- paste(var2explain,paste(variable,collapse = ' + '),sep=' ~ ')
  if(!Intercept) fml.char <- paste0(fml.char,' -1')
  mod.out <- lm(formula = as.formula(fml.char),data = X_Block)
  return(mod.out)
}
# model w/ price hike & weekend
var1 <- c("daily.temp","weekend","Public.holiday","price.hike","leak.alarm")
mod1 <- lm.var(var1,var2explain)

# model w/ price hike & time effect
var2 <- c("daily.temp","Public.holiday","price.hike","leak.alarm","time.effect")
mod2 <- lm.var(var2,var2explain)

# model w/ app & weekend
var3 <- c("daily.temp","weekend","Public.holiday","app","leak.alarm")
mod3 <- lm.var(var3,var2explain)

# model w/ app & time effect
var4 <- c("daily.temp","Public.holiday","app","leak.alarm","time.effect")
mod4 <- lm.var(var4,var2explain)

ComparMod <- data.frame(Model = c('PriceHike+Weekend','PriceHike+TimeEffect','APP+Weekend','APP+TimeEffect'),
                        R2=c(summary(mod1)$adj.r.squared,summary(mod2)$adj.r.squared,summary(mod3)$adj.r.squared,summary(mod4)$adj.r.squared),
                        AIC=c(AIC(mod1),AIC(mod2),AIC(mod3),AIC(mod4)),stringsAsFactors = FALSE)
cat('Best model w/ combination - AIC:',ComparMod$Model[which.min(ComparMod$AIC)],'\n')
cat('Best model w/ combination - R2:',ComparMod$Model[which.max(ComparMod$R2)],'\n')

######
tslm.var <- function(variable,var2explain,Intercept = TRUE, trend =TRUE, season=TRUE){
  fml.char <- paste(var2explain,paste(variable,collapse = ' + '),sep=' ~ ')
  if(trend){fml.char <- paste0(fml.char,' + trend')}
  if(season){fml.char <- paste0(fml.char,' + season')}
  if(!Intercept) fml.char <- paste0(fml.char,' -1')

  mod.out <- tslm(formula = as.formula(fml.char),data = X_Block)
  return(mod.out)
}

y7 <- ts(X_Block$PG_B1,frequency = 7)

varF <-  c("daily.temp","Public.holiday","leak.alarm","price.hike","weekend") #list of the variables used in the final model

tslm.mod_Trend <-  tslm.var(variable = varF, var2explain = 'y7', Intercept = FALSE)
tslm.mod_NoTrend <- tslm.var(variable = varF, var2explain = 'y7', Intercept = FALSE,trend = F)
AIC(tslm.mod_Trend) < AIC(tslm.mod_NoTrend) # compare model w/ or w/o trend

Coef <- data.frame(Variable = names(tslm.mod_NoTrend$coefficients), All = tslm.mod_NoTrend$coefficients)
