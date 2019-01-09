###########################################################################################################
#########################################  Customer segmentation  #########################################
##########################################  on 1D (level)  ################################################
#########################################  or 2D (level + var)   ##########################################
###########################################################################################################

rm(list=ls(all=TRUE));invisible(gc());
cat('\014')

ptm <- proc.time()

# import packages
if(!'pacman' %in% installed.packages()[,1]){install.packages('pacman')}
require(pacman)
pacman::p_load(plyr,dplyr,tidyr,ggplot2,lubridate,fst)

setwd("/srv/shiny-server/DataAnalyticsPortal/Profiling_V2")
load("/srv/shiny-server/DataAnalyticsPortal/Profiling_V2/Output/01-Punggol_Indicators.RData")

Punggol_2016 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2016.fst")[,1:12]
Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst")[,1:12]
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")
Punggol_All <- rbind(rbind(Punggol_2016,Punggol_2017),Punggol_thisyear)

X <- Punggol_All %>% dplyr::filter(Date >= ymd('2016-03-01'))
X <- X %>% dplyr::filter(room_type != 'HDBCD' & !(is.na(unit)))

X$Consumption <- X$adjusted_consumption
X$H <- hour(X$date_consumption)
X$M <- month(X$date_consumption)
X$Y <- year(X$date_consumption)
X$wd <- weekdays(X$date_consumption)

DailyCons <- X %>% group_by(service_point_sn,block,Date,room_type) %>% dplyr::summarise(vol=sum(Consumption,na.rm = TRUE)/1000)

Y <- DailyCons %>% group_by(service_point_sn,block,room_type) %>% dplyr::summarise(adc=mean(vol))# average daily consumption

# load("Output/02- Punggol - Groups variation_2Grp.RData")
Group <- read.table('Output/02-Customers Groups.csv', header =TRUE, sep =';',dec='.', as.is = TRUE)
load('Output/03 - Punggol - Stratification.RData')

Y$Grp <- Group$grp[match(Y$service_point_sn,Group$ID)]
Y$Grp[which(is.na(Y$Grp))] <- 0
Y$Lev <- Clust$str[match(Y$service_point_sn,Clust$ID)]
Y$Lev[which(is.na(Y$Lev))] <- 0
Y$Prof <- paste0('L',Y$Lev,'G',Y$Grp)
Y <- Y %>% filter(Lev !=0 & Grp !=0)

Y %>% group_by(Lev,Grp) %>% dplyr::tally() %>% spread(Lev,n)
X$Prof <- Y$Prof[match(X$service_point_sn,Y$service_point_sn)]

#Xd <- X %>% group_by(Date,M,Y,wd,week,block,floor,unit,room_type,service_point_sn,Prof) %>% dplyr::summarise(x=sum(Consumption))
Xd <- X %>% group_by(Date,M,Y,wd,block,floor,unit,room_type,service_point_sn,Prof) %>% dplyr::summarise(x=sum(Consumption))

Avg.Ind.Dai.Cons <- Xd %>% group_by(service_point_sn,wd) %>% dplyr::summarise(x=mean(x,na.rm=TRUE))
Avg.Ind.Dai.Cons <- Avg.Ind.Dai.Cons %>% spread(wd,x,fill=TRUE)
Y <- merge(Y,Avg.Ind.Dai.Cons)
Avg.Ind.Hou.Cons <- X %>% group_by(service_point_sn,H) %>% dplyr::summarise(x=mean(Consumption,na.rm=TRUE))
Avg.Ind.Hou.Cons$H <- paste0('H',sprintf('%02d',Avg.Ind.Hou.Cons$H))
Avg.Ind.Hou.Cons <- Avg.Ind.Hou.Cons %>% spread(H,x,fill=0)
Y <- merge(Y,Avg.Ind.Hou.Cons)

write.table(Y,paste0("Output/04_Individual Avg Consumption.csv"),quote=F,row.names=F,sep=";",dec=".",col.names=T);

FinalProfile <- data.frame(Prof = c(paste0('L1G',c(1,4,2,3)),paste0('L2G',c(1,4,2,3))), Profile=c(paste0('P',c(1,1,2,2,3,3,4,4))))

Y <- inner_join(Y,FinalProfile)
Xd$Profile <- Y$Profile[match(Xd$service_point_sn,Y$service_point_sn)]

Xd$num_house_member <- family_details$num_house_member[match(Xd$service_point_sn,family_details$service_point_sn)]

Xd <- Xd %>% filter(!is.na(Profile))

Conso <- Xd %>% group_by(Date,Profile) %>% dplyr::summarise(Conso.avg=mean(x,na.rm=TRUE))
Conso <- Conso %>% filter(Date < max(Conso$Date))

Conso_LPCD <- Xd %>% group_by(Date,Profile) %>% dplyr::summarise(Conso.avg=mean(x,na.rm=TRUE),
                                                                 num_house_member.avg=mean(num_house_member,na.rm=TRUE),
                                                                 LPCD=round(Conso.avg/num_house_member.avg))
Conso_LPCD <- Conso_LPCD %>% filter(Date < max(Conso$Date))

pdf(paste0("Output/04-Average Daily consumption by profile.pdf"),width = 14,height = 7)
ggplot(Conso, aes(x=Date, y=Conso.avg)) + 
  geom_line(aes(colour=Profile)) +
  # facet_grid(Prof ~ ., scales = "free_y") + 
  ylab (expression("Average Consumption (litres/day)")) 
dev.off()

Avg.Daily <- Xd %>% group_by(wd,Profile) %>% dplyr::summarise(x=mean(x,na.rm=TRUE))
Avg.Daily <- Avg.Daily %>% spread(Profile,x)

X$Profile <- Y$Profile[match(X$service_point_sn,Y$service_point_sn)]
X <- X %>% filter(!is.na(Profile))
Avg.hourly <- X %>% group_by(H,Profile) %>% dplyr::summarise(x=mean(Consumption,na.rm = TRUE))
# Avg.hourly <- Avg.hourly %>% spread(Profile,x)
pdf(paste0("Output/04-DifferentProfiles.pdf"),width = 14,height = 7)
ggplot(Avg.hourly, aes(x=H, y=x)) + 
  geom_line(aes(colour=Profile)) +
  # facet_grid(Prof ~ ., scales = "free_y") + 
  ylab (expression("Average Consumption (litres/hour)")) 
dev.off()

Updated_DateTime_CustomerSegmentation <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(Conso,Y,Conso_LPCD,Updated_DateTime_CustomerSegmentation,file = "Output/04-Final_Profile.RData")

time_taken <- proc.time() - ptm
ans <- paste("04-Profile successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)

