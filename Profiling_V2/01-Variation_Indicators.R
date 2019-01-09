rm(list=ls(all=TRUE));gc()
if(length(dev.list())>0) dev.off()

ptm <- proc.time()

if(!'pacman' %in% installed.packages()[,1]){install.packages('pacman')}
require(pacman)
pacman::p_load(plyr,dplyr,tidyr,lubridate,readxl,DBI,RPostgreSQL,fst)

cat('\014')
setwd("/srv/shiny-server/DataAnalyticsPortal/Profiling_V2")

Punggol_2016 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2016.fst")
Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst")
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")
Punggol_All <- rbind(rbind(Punggol_2016,Punggol_2017)[,1:12],Punggol_thisyear)

gc()
X <- Punggol_All %>% dplyr::filter(Date >= ymd('2016-03-01'))
X$Date.Time <- X$date_consumption-lubridate::hours(1)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))) %>%
          group_by(id_service_point) %>%
          dplyr::filter(move_in_date==max(move_in_date))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(!service_point_sn %in% c("3100507837M","3100507837B","3100660792")))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

family_details <- family_servicepoint %>% select_("service_point_sn","num_house_member","online_status")

daily_occupancy <- as.data.frame(tbl(con,"daily_occupancy"))
weekly_occupancy <- as.data.frame(tbl(con,"weekly_occupancy"))
monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy"))

X <- X %>% dplyr::filter(room_type !='HDBCD' & !(is.na(unit)))

X$ID <- as.character(X$service_point_sn);
X$Consumption <- X$adjusted_consumption
X$H <- hour(X$Date.Time)
X$D <- day(X$Date.Time)
X$M <- month(X$Date.Time)
X$Y <- year(X$Date.Time)
X$wd <- weekdays(X$Date.Time)

if(X$H[which.max(X$Date.Time)]!=23){X <- X %>% dplyr::filter(Date < max(X$Date))}

DailyCons <- X %>% group_by(ID,block,Date) %>% dplyr::summarise(vol=sum(Consumption,na.rm=TRUE))
MonthlyCons <- X %>% group_by(ID,block,Y,M) %>% dplyr::summarise(vol=sum(Consumption,na.rm=TRUE))

ADC <- DailyCons %>% group_by(ID,block) %>% dplyr::summarise(adc=mean(vol))# average daily consumption
AMC <- MonthlyCons %>% group_by(ID,block) %>% dplyr::summarise(amc=mean(vol))# average monthly consumption
AHC <- X %>% group_by(ID,block) %>% dplyr::summarise(ahc=mean(Consumption,na.rm = TRUE))# average hourly consumption

Avg <- merge(ADC,AHC,by=c('ID','block'));
Avg <- merge(Avg,AMC,by=c('ID','block'));
#Avg <- Avg %>% dplyr::filter(adc != 0)
Y <- Avg;

nb_mat <- nrow(Avg);
cat("# customers = ",nb_mat,"\n");
X <- subset(X,ID %in% Avg$ID)
save_X <- X

cat("End of pre-process at ",as.character(Sys.time()),"\n")

######################################################################################################################################################################################################
################################################################################## Codification des variables ########################################################################################
######################################################################################################################################################################################################

##################################### Hour #################################################
X$hour <- paste0("H",sprintf('%02d',hour(X$Date.Time)))
Hour.Period <- read_excel('/srv/shiny-server/DataAnalyticsPortal/data/Day Period.xlsx')                 

# X$Year <- format(X$IndexTime,format='%Y')
# Hour.Y <- X %>% group_by(Year,hour) %>% summarise(x=mean(vol))
# Hour.Y$Year <- as.factor(Hour.Y$Year)
# ggplot(Hour.Y, aes(x = hour, y = x*1000,fill=Year)) + geom_bar(position="dodge",stat="identity")+
#   ylab('Volume (litre/hour)')
X$HourPeriod <- Hour.Period$Day.Period[match(X$H,Hour.Period$H)]

Hour <- X %>% group_by(ID,HourPeriod) %>% dplyr::summarise(x=mean(Consumption,na.rm=TRUE))
temp <- Hour %>% spread(HourPeriod,x)
Y <- merge(Y,temp,by=c("ID"));

### Add in 3 slots of hours for peak consumption
X_early <- X %>% dplyr::filter(H >=6 & H <=12)
Test <- data.frame(H = 6:11, Riser = c(rep('Early',2),rep('Transit',2),rep('Late',2)))
Riser <- inner_join(Test,X_early) %>%
         dplyr::group_by(ID,Riser) %>%
         dplyr::summarise(x=mean(Consumption,na.rm=TRUE))
temp <- Riser %>% spread(Riser,x)
Y$keep.riser <- rowMeans(temp[,-1]) ## will be used to normalized the 3 variables at the end of the script
Y <- merge(Y,temp,by=c("ID"));

####################################### Week days ##########################################
DailyCons$day <- paste0('D',as.numeric(format(DailyCons$Date,format="%w")))
JS <- DailyCons %>% group_by(ID,day) %>% dplyr::summarise(x=mean(vol))

temp <- JS %>% spread(day,x)
Y <- merge(Y,temp,by=c("ID"))

########################################## Days off  ##########################################
doff <- read.table("/srv/shiny-server/DataAnalyticsPortal/data/Holidays_2014_2017.csv",sep=";",dec=".",header=FALSE,as.is=TRUE,quote="\"");
colnames(doff) <- c("namedoff","do");
doff$do <- dmy(doff$do)
DailyCons$doff <- doff$namedoff[match(DailyCons$Date,doff$do,nomatch=NA)]
DailyCons$public.holiday <- ifelse(is.na(DailyCons$doff),"working.day","public.hol")

temp <- DailyCons %>% group_by(ID,public.holiday) %>% dplyr::summarise(x=mean(vol,na.rm=TRUE))
temp <- temp %>% spread(public.holiday,x)

Y <- merge(Y,temp,by=c("ID"));

cat("Ending at ",as.character(Sys.time()),"\n")

########################################### mois #####################################################
# DailyCons$month <- paste0("M",sprintf('%02d',month(DailyCons$Date)));   # no disimilarity among the months
# Month <- DailyCons %>% group_by(ID,month) %>% dplyr::summarise(x=mean(vol,na.rm=TRUE))
# temp <- Month %>% spread(month,x)
# Y <- merge(Y,temp,by=c("ID"))
# 
# cat("Ending at ",as.character(Sys.time()),"\n")

##################################### climat ################################################
load('/srv/shiny-server/DataAnalyticsPortal/data/Weather.RData')
temp.climat <- DailyCons
temp.climat$T_Max <- Weather$Tmax[match(temp.climat$Date,Weather$Date)]
temp.climat$Rainf <- Weather$Rainf[match(temp.climat$Date,Weather$Date)]

temp <- ddply(temp.climat,.(ID),function(x) cor(x$vol,x$T_Max),.progress="text");
colnames(temp) <- c("ID","corTC");
temp$corTC[which(is.na(temp$corTC))] <- 0
Y <- merge(Y,temp,by="ID");

temp <- ddply(temp.climat,.(ID),function(x) cor(x$vol,x$Rainf),.progress="text");
colnames(temp) <- c("ID","corRF");
temp$corRF[which(is.na(temp$corRF))] <- 0
Y <- merge(Y,temp,by="ID");

# Y$corTC[which(is.na(Y$corTC))] <- 0;
# Y$corRF[which(is.na(Y$corRF))] <- 0;
cat("Ending at ",as.character(Sys.time()),"\n")
 
##################################### variation ################################################
VarConso <- function(vect) {
    x1 <- sum(vect[which(vect<0.75*mean(vect))])/sum(vect);
    x2 <- sum(vect[which(vect>=0.75*mean(vect) & vect<1.5*mean(vect))])/sum(vect);
    x3 <- sum(vect[which(vect>=1.5*mean(vect))])/sum(vect);
    return(data.frame(CLo = x1, CNo = x2, CHi = x3))
}

temp <- ddply(DailyCons,.(ID),function(x) VarConso(x$vol),.progress="text");

Y <- merge(Y,temp,by=c("ID"));
cat("Ending at ",as.character(Sys.time()),"\n")

##################################### Vacancy #####################################
Vacancy <- DailyCons %>% group_by(ID) %>% dplyr::summarise(Vacancy = sum(vol==0,na.rm=TRUE)/length(vol))
Y <- merge(Y,Vacancy,by=c("ID"));

##################################### Coefficient of variation #####################################
cv <- function(vect){return(sd(vect)/mean(vect))}

# between days
CDays <- DailyCons %>% group_by(ID) %>% dplyr::summarise(CVday=cv(vol))
# between weeks
DailyCons$week <- format(DailyCons$Date,format="%W")
Week <- DailyCons %>% group_by(ID,week) %>% dplyr::summarise(x=mean(vol))
CWeek <- Week %>% group_by(ID) %>% dplyr::summarise(CVwe=cv(x))
# between months
DailyCons$month <- month(DailyCons$Date)
Month <- DailyCons %>% group_by(ID,month) %>% dplyr::summarise(x=mean(vol))
CMonth <- Month %>% group_by(ID) %>% dplyr::summarise(CVmo=cv(x))

Y <- merge(Y,CDays,by=c("ID"));
Y <- merge(Y,CWeek,by=c("ID"));
Y <- merge(Y,CMonth,by=c("ID"));

##################################### Household Size #####################################
HHSize <- family_servicepoint %>% 
          group_by(service_point_sn) %>%
          dplyr::filter(move_in_date==max(move_in_date)) %>%
          dplyr::select_("service_point_sn","num_house_member") %>%
          dplyr::filter(service_point_sn %in% Y$ID) %>%
          dplyr::rename(ID=service_point_sn)

Y <- merge(Y,HHSize,by='ID')
cat("Ending at ",as.character(Sys.time()),"\n")

##################################### Occupancy #####################################
AverageDailyOccupancy <- daily_occupancy %>% 
                         dplyr::filter(service_point_sn %in% Y$ID) %>%
                         group_by(service_point_sn) %>%
                         dplyr::summarise(AverageDailyOccupancy=round(mean(occupancy_rate),2)) %>%
                         dplyr::rename(ID=service_point_sn)

AverageWeeklyOccupancy <- weekly_occupancy %>% 
                          dplyr::filter(service_point_sn %in% Y$ID) %>%
                          group_by(service_point_sn) %>%
                          dplyr::summarise(AverageWeeklyOccupancy=round(mean(occupancy_rate),2)) %>%
                          dplyr::rename(ID=service_point_sn)

AverageMonthlyOccupancy <- monthly_occupancy %>% 
                           dplyr::filter(service_point_sn %in% Y$ID) %>%
                           group_by(service_point_sn) %>%
                           dplyr::summarise(AverageMonthlyOccupancy=round(mean(occupancy_rate),2)) %>%
                           dplyr::rename(ID=service_point_sn)

Y <- merge(Y,AverageDailyOccupancy,by='ID')
Y <- merge(Y,AverageWeeklyOccupancy,by='ID')
Y <- merge(Y,AverageMonthlyOccupancy,by='ID')

### indicator are 
save.Y <- Y
# hourly indicators
ind.h <- sort(match(unique(Hour.Period$Day.Period),names(Y)))
Y[,ind.h] <- Y[,ind.h]/Y$ahc  # scaling
# Riser
ind.r <- sort(match(c('Early','Transit','Late'),names(Y)))
Y[,ind.r] <- Y[,ind.r]/Y$keep.riser  # scaling
# daily indicators
ind.d <- (max(ind.r)+1):(which(names(Y)=="corTC")-1)
Y[,ind.d] <- Y[,ind.d]/Y$adc  # scaling

Y$keep.riser <- NULL
indicator <- Y
save(indicator,family_details,DailyCons,file = "Output/01-Punggol_Indicators.RData")

time_taken <- proc.time() - ptm
ans <- paste("01-Variation_Indicators successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
