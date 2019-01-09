###########################################################################################################
#########################################  Customer segmentation  #########################################
##########################################  on 1D (level)  ################################################
#########################################  or 2D (level + var)   ##########################################
###########################################################################################################

rm(list=ls(all=TRUE));invisible(gc());
cat('\014')

# import packages
if(!'pacman' %in% installed.packages()[,1]){install.packages('pacman')}
require(pacman)
pacman::p_load(plyr,dplyr,tidyr,FactoMineR,ggplot2,mclust)

setwd("/srv/shiny-server/DataAnalyticsPortal/Profiling_V2")
load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")

X <- Punggol_All %>% dplyr::filter(Date >= ymd('2016-03-01'))

X$ID <- as.character(X$service_point_sn);
DailyCons <- X %>% group_by(ID,block,Date,room_type,unit) %>% summarise(vol=sum(Consumption,na.rm = TRUE)/1000)
X <- DailyCons %>% group_by(ID,block,room_type,unit) %>% summarise(adc=mean(vol))# average daily consumption
rm(DailyCons,Punggol_All,Week.date)

X <- X %>% dplyr::filter(room_type !='HDBCD' & !(is.na(unit)))
X <- X %>% dplyr::filter(adc != 0)
# we use as variable for stratification the average annual consumption 
X$var.str <- X$adc*365  
## replace above with LPCD  --> Thiam Huat to work something here...

X <- X[order(X$var.str),];

######## Selection of boundary of the upper stratum ########
#' optimization function to find the bound of the last stratum
#' last stratum should gather a minimal number of individuals while explaining as much variance as possible
#' @param bound: lower boudary of the last stratum
#' @param weight: vector of weights to the two criteria to minimize (if NULL, take the log of the variance within-stratum)

# optim.upper.bound <- function(bound,weight=NULL){
#   X$str <- 1
#   X$str[which(X$var.str>=bound)] <- 2
#   
#   res <- data.frame(str=1:2)
#   res$Nh <- aggregate(X$ID,subset(X,select="str"),length)$x;
#   res$Wh <- res$Nh / sum(res$Nh)
#   res$S2h <- aggregate(X$var.str,subset(X,select="str"),var)$x;
#   res$S2h[which(is.na(res$S2h))] <- 0;
#   res$vh <- res$S2h*res$Nh/sum(res$Nh)
#   
#   # transformation of the variable because of the difference of magnitude between Nh and vh => unbalanced proportion
#   if(is.null(weight)){
#     res$vh <- log(pmax(res$vh,1))
#     weight <- c(1,1)
#   }else{
#     weight <- weight/sum(weight)
#   }
#   
#   res$vW <- res$vh/sum(res$vh)
#   
#   # minimization of the number of individual & maximization of the variance in the stratum
#   # <=> minimization of the variance in the 1st stratum
#   return(weight[1]*res$Wh[2]+weight[2]*res$vW[1])
# }
# 
# res.bound <- optimize(optim.upper.bound,interval=quantile(X$var.str,c(1,99)/100))
# 
# last_str <- trunc(res.bound$minimum)
# 
# nb.pie <- trunc(sum(X$var.str>=last_str)*100/nrow(X));
# nb.pie <- c(nb.pie,100-nb.pie[1]);
# 
# label.pie <- c(paste0("Big Consumpt. (",nb.pie[1],"%)"),paste0("Cust. to segment (",nb.pie[2],"%)"))
# 
# df <- data.frame(
#   variable = label.pie,
#   value = nb.pie
# )
# pdf("03-Preliminary segmentation - Big consumers.pdf")
# ggplot(df, aes(x = "", y = value, fill = variable)) +
#   geom_bar(width = 1, stat = "identity") +
#   coord_polar("y", start = pi / 3) +
#   labs(title = "Preliminary segmentation")
# dev.off()

######## => Segmentation
# W <- subset(X,var.str<last_str)
W <- X

### function that cut the empirical distribution of a function according to a fixed number of strata
find.bound <- function(l,vect = var.str){
    dc <- 1;
    up.var.str <- max(vect);
    bound <- data.frame(borne = seq(0,(up.var.str-dc),by=dc));
    count <- function(b) sum(vect >=b & vect < b+dc)
    bound$f <- sapply(bound$borne,count);
    ## cumulative square root of frequencies
    bound$csf <- cumsum((bound$f)^(1/2));
    inter.size <- max(bound$csf)/l * 0:(l-1);
    bound$str <- NA;
    
    for (i in 1:nrow(bound))
    {
      bound$str[i] <- max(which(inter.size <= bound$csf[i]))
    }
    binf <- aggregate(bound$borne,subset(bound,select="str"),min);colnames(binf) <- c("str","binf");
    bstr <- data.frame(str=1:(nrow(binf)),bstr=c(binf$binf));
    return(bstr)
}
  
# calculate some statistics on strata according to a fixed nb of strata
Variance.strata <- function(nb, data = W){
  var.str <- data$var.str
  bstr <- find.bound(nb,var.str)
  data$str <- NA;
  
  for (i in 1:nrow(bstr))
  {
    data$str[which(data$var.str>=bstr$bstr[i])] <- i;
  }
  
  #' some statistics on the strata
  Str_temp <- bstr
  Str_temp$Nh <-  aggregate(data$var.str,subset(data,select="str"),length)$x;
  Str_temp$Wh <-  Str_temp$Nh/sum(Str_temp$Nh);
  Str_temp$S2h <- aggregate(data$var.str,subset(data,select="str"),var)$x;
  Str_temp$Vw <- (Str_temp$Nh/sum(Str_temp$Nh))*Str_temp$S2h;
  return(sum(Str_temp$Vw))
}

var.str <- W$var.str
nb.min <- 2
nb.max <- 10

varW <- data.frame(nb=nb.min:nb.max,V=NA,d=NA)
varW$V <- sapply(nb.min:nb.max,Variance.strata)
varW$d <- c(NA,diff(varW$V)/varW$V[-nrow(varW)])
varW$d[1] <- (varW$V[1]-var(W$var.str))/var(W$var.str)

lev <- 1/3
analyse <- min(which(varW$d > -lev))-1
L <- varW$nb[analyse]

# pdf("Output/03-Number of strata.pdf",width=10,height=6)
  plot(varW$nb,varW$V,type="h",lwd=3,col="skyblue",main="Evolution of the total variance within strata",xlab="# of cluster",ylab="")
  lines(varW$nb,varW$V,col=rgb(248,118,109, maxColorValue = 255),lty="dotted")
  points(L,varW$V[which(varW$nb==L)],type="h",col=rgb(248,118,109, maxColorValue = 255),lwd=3)
  x_axis <- (nb.min:(nb.max-1))+0.5
  y_axis <- ((varW$V[-1]+varW$V[-nrow(varW)])/2)*1.2
  text(x_axis,y_axis,paste(round(varW$d[-1]*100),"%",sep=""),cex=0.65,col="blue");
# dev.off()
  
L <- 2 # low VS high volumes  
bstr <- find.bound(L,var.str)


X$str <- NA;
for (i in 1:nrow(bstr))
{
  X$str[which(X$var.str>=bstr$bstr[i])] <- i;
}

#' some statistics on the strata
ST <- bstr
ST.temp <- X %>% group_by(str) %>% dplyr::summarise(Nh=length(ID),Wh = length(ID)/nrow(X),Yh=mean(var.str),S2h=var(var.str))
ST <- merge(ST,ST.temp,by='str')

X %>% group_by(str,room_type) %>% tally() %>% spread(str,n)
 
pdf("Output/03-Stratification.pdf") 
  data.plot <- data.frame(var.clust=sort(var.str))
  data.plot$freq <- (1:nrow(data.plot))/nrow(data.plot)
  op = par(bg = "#EFEFEF")
  plot(data.plot$var.clust,data.plot$freq,type="l",col="#4ECDC4",xlab="Clustering variable",ylab="Frequency",axes = FALSE,cex.axis=0.8)
  abline(v=c(ST$bstr,max(X$var.str)),lty="dashed",col="#FF6B6B")
  axis(1,at=c(ST$bstr,max(X$var.str)),labels=c(ST$bstr,round(max(X$var.str))),cex.axis=0.8)
  axis(2,cex.axis=0.8)
dev.off()

Clust <- X
save(Clust,ST,file = 'Output/03 - Punggol - Stratification.RData')


# write.table(ST,paste0("Output/03-Strata_definition.csv"),quote=F,row.names=F,sep=";",dec=".",col.names=T);
# 
# 
# write.table(X,paste0("Output/03-Customers+strata.csv"),quote=F,row.names=F,sep=";",dec=".",col.names=T)

ST$Strate <- paste0("Str",ST$str)
ST$Strate <- factor(c('Low Volume','High Volume')[ST$str],levels = c('Low Volume','High Volume'))
ST$volume <- ST$Yh*ST$Nh
require(ggplot2)
pos.x.1 <- (cumsum(ST$Nh)+c(0,cumsum(ST$Nh)[-nrow(ST)]))/2
lab.y.1 <- rev(percent(ST$Nh/sum(ST$Nh)))

pos.x.2 <-  (cumsum(rev(ST$volume))+c(0,cumsum(rev(ST$volume))[-nrow(ST)]))/2
lab.y.2 <- rev(percent(ST$volume/sum(ST$volume)))

pdf(paste0("Output/03-Strata_Allocation.pdf"))
ggplot(ST, aes(x = "", y = Nh, fill = Strate)) +
  geom_bar(width = 1, stat = "identity") +
  coord_polar("y", start = pi / 3,direction = -1) +
  geom_text(aes(y = pos.x.1,label = lab.y.1)) + 
  labs(title = "Allocation of customers")

ggplot(ST, aes(x = "", y = volume, fill = Strate)) +
  geom_bar(width = 1, stat = "identity") +
  coord_polar("y", start = pi / 3,direction = -1) +
  geom_text(aes(y = pos.x.2,label = lab.y.2)) + 
  labs(title = "Allocation of consumptions")
dev.off()

max.x <- max(X$var.str)
x.coord.avg <- (ST$bstr + c(ST$bstr[-1],max.x))/2
pdf(paste0("Output/03-Strata_Histogram.pdf"),width = 12,height = 7)  
ggplot(data=Clust, aes(var.str)) + 
  geom_histogram(breaks=c(ST$bstr,max.x),col=rgb(248,118,109,255,maxColorValue = 255), aes(fill=..count..))+
  xlab("Average yearly consumption") + ylab("# of customers") +annotate("text",x = x.coord.avg ,y = (ST$Nh+pmin(0.1*ST$Nh,500)) ,angle=0,label=as.character(ST$Nh),colour=rgb(248,118,109,255,maxColorValue = 255))+
  theme(legend.position="none")
dev.off()


