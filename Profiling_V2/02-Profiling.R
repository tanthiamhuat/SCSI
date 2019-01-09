# http://www.sthda.com/english/wiki/ade4-and-factoextra-principal-component-analysis-r-software-and-data-mining
# http://www.sthda.com/english/wiki/factominer-and-factoextra-principal-component-analysis-visualization-r-software-and-data-mining
# https://www.analyticsvidhya.com/blog/2016/03/practical-guide-principal-component-analysis-python/
# https://cos.name/cn/topic/12803/

rm(list=ls(all=TRUE));gc()
if(length(dev.list())>0) dev.off()
if(!'pacman' %in% installed.packages()[,1]){install.packages('pacman')}
require(pacman)
pacman::p_load(FactoMineR,ClustOfVar,plyr,dplyr,tidyr,ggplot2,e1071,plot3D,scales,FNN)

cat('\014')

setwd("/srv/shiny-server/DataAnalyticsPortal/Profiling_V2")
load("Output/01-Punggol_Indicators.RData")
indicator[is.na(indicator)] <- 0

Avg <- indicator[,1:5]; ## average daily consumption per customer

Y <- indicator[,6:ncol(indicator)];
rownames(Y) <- Avg$ID;

### Hierarchical clustering on variable
Z <- scale(Y)
CAH_var <- ClustOfVar::hclustvar(Z)
plot(CAH_var)

# choice nb of initial group
lev <- 6/100 # to be adapted
crit <- rev(CAH_var$height);
k <- min(which(diff(crit)/crit[-length(crit)]> -lev))
nbmax <- pmin(length(crit),20) # limit the plot to 20 groups

plot(1:nbmax,crit[1:nbmax],type="b",xlab="Nb of variables cluster",ylab="Inter-class inertia")
points(k,crit[k],col="red",pch=16)
x_axis <- 2:length(crit)
y_axis <- ((crit[-1]+crit[-length(crit)])/2)*1.05
text(x_axis[1:nbmax],y_axis[1:nbmax],paste(round(diff(crit)*100/crit[-length(crit)]),"%",sep="")[1:nbmax],cex=0.65,col="blue");

#k <- 4
# Gathering variables
Gath_V <- data.frame(var=names(cutree(CAH_var,k)),grp=cutree(CAH_var,k));
Gath_V$var <- as.character(Gath_V$var)
temp <- Gath_V[CAH_var$order,]

source("00-Plot_ACP.R")
#### New groups are created by suppressing the variables that are not correctly projected on the first factor plane (or by creating a new group)
Gath_V$new_gpe <- NA;
list.gpe <- which(table(Gath_V$grp)>1)

for (i in list.gpe){
  rang <- 1
  var.k <- subset(Gath_V,grp==i)
  Z <- subset(Y,select=as.character(var.k$var))
  acp <- PCA(Z,graph=F)
  eig <- acp$eig[2,1]
  cos2 <- min(acp$var$cos2[,1])
  z <- Z
  suppr <- NULL
  #' Case 1, val. Clean> 1 => can one sum up? A synthetic variable
  #' It is necessary to remove the least well represented variables
  #' And we look at whether all the variables are well projected
  if(eig > 1 | cos2 < 0.45){
    while((eig > 1 | cos2 < 0.45) & class(z)!='numeric'){
      suppr <- which.min(acp$var$cos2[,1])
      z <- z[,-suppr]
      acp <- PCA(z,graph = FALSE)
      if(class(z)!='numeric'){eig <- acp$eig[2,1] }else as.numeric(eig <- acp$eig[2])
      if(class(z)!='numeric'){cos2 <-min(acp$var$cos2[,1])}else cos2 <- min(acp$var$cos2)
    }
    if(class(z)!='numeric'){Gath_V$new_gpe[match(names(z),Gath_V$var)] <- paste0(i,letters[rang]);rang <- rang+1}
    
    #' We try to see if the delete variables can not be combined
    z_suppr <- Z[,setdiff(names(Z),names(z))]
    if(class(z_suppr)!='numeric'){
      acp_suppr <- PCA(z_suppr,graph = FALSE)
      eig <- acp_suppr$eig[2,1]
      cos2 <- min(acp_suppr$var$cos2[,1])
      if(eig >1 | cos2 < 0.45 ){
        while((eig > 1 | cos2 < 0.45) & class(z_suppr)!='numeric'){
          suppr <- which.min(acp_suppr$var$cos2[,1])
          z_suppr <- z_suppr[,-suppr]
          acp_suppr <- PCA(z_suppr,graph = FALSE)
          if(class(z_suppr)!='numeric'){eig <- acp_suppr$eig[2,1] }else eig <- acp_suppr$eig[2]
          if(class(z_suppr)!='numeric'){cos2 <-min(acp_suppr$var$cos2[,1])}else cos2 <- min(acp_suppr$var$cos2)
        }
      }
      if(class(z_suppr)!='numeric'){Gath_V$new_gpe[match(names(z_suppr),Gath_V$var)] <- paste0(i,letters[rang]);rang <- rang+1}
    }
  }else{
    if(class(z)!='numeric'){Gath_V$new_gpe[match(names(z),Gath_V$var)] <- paste0(i,letters[rang]);rang <- rang+1}
  }
}

new.gpe <- names(table(Gath_V$new_gpe))
arret <- sum(is.na(Gath_V$new_gpe)) == 0
tt <- 1
while(!arret)
{
  NA.var <- subset(Gath_V,is.na(new_gpe))
  n.dep <- nrow(NA.var);
  non.attr <- 0;# on compte le nombre de groupes o? l'on n'a pas pu r?-attribuer de variables
  eig <- matrix(0,nrow(NA.var),length(new.gpe));dimnames(eig) <- list(as.character(NA.var$var),new.gpe);
  cos2 <- matrix(0,nrow(NA.var),length(new.gpe));dimnames(cos2) <- list(as.character(NA.var$var),new.gpe);
  for (j in 1:length(new.gpe))
  {
    var.k <- subset(Gath_V,new_gpe==new.gpe[j])
    #res.acp <- data.frame(var=NA.var$var,eig=NA,cos2=NA)
    for (i in 1:nrow(NA.var))
    {
      new.var <- subset(Y,select=as.character(NA.var$var[i]))
      Z <- subset(Y,select=as.character(var.k$var))
      Z <- cbind(Z,new.var)
      acp <- PCA(Z,graph=F)
      eig[i,j] <- acp$eig[2,1]
      cos2[i,j] <- ifelse(sum(acp$var$cos2[,1]<0.4)==0,acp$var$cos2[ncol(Z),1],0)
      #           res.acp$eig[i] <- acp$eig[2,1]
      #           res.acp$cos2[i] <- acp$var$cos2[ncol(Z)]
    }
  }
  eig[which(eig>1)] <- NA
  cos2[which(cos2<0.4)] <- NA
  reattr <- order(cos2,decreasing=T)
  reattr.order <- 1;
  reattr.ok <- F
  while(!(reattr.ok))
  {
    reattr.obj <- reattr[reattr.order]
    if(sum(cos2[reattr.obj],na.rm=T)>0 & !(is.na(eig[reattr.obj])))
    {
      nl <- reattr.obj%%nrow(cos2);
      nc <- (reattr.obj%/%nrow(cos2))+1
      if(nl==0){nl <- nrow(cos2);nc <- nc-1}
      
      Gath_V$new_gpe[which(Gath_V$var==rownames(cos2)[nl])] <- colnames(cos2)[nc]
      cat("Variable reattribute ",rownames(cos2)[nl]," - ",colnames(cos2)[nc],"\n")
      reattr.ok <- T
    }else
    {
      if(sum(cos2[reattr[-(1:reattr.order)]],na.rm=T)==0){arret <- TRUE; cat("No more variables to reattribute \n");reattr.ok <- TRUE
      }else{reattr.order <- reattr.order+1}
    }
  }
  if (sum(is.na(Gath_V$new_gpe))==0){
    arret <- TRUE
  }
}

k.temp <- length(unique(unique(Gath_V$new_gpe[which(!(is.na(Gath_V$new_gpe)))])))
Gath_V[which(is.na(Gath_V$new_gpe)),]
# if some variables can be set together do it manually
# check if in acp1$eig only the 1st value is >=1
# and if acp1$var$cos2 are all >= 0.5

# var1 <- c('M06','M11','M12')
# temp1 <- subset(Y,select=var1)
# acp1 <- PCA(temp1)
# Gath_V$new_gpe[match(var1,Gath_V$var)] <- k.temp+1
# k.temp <- k.temp + 1
# 
# var1 <- c('Afternoon','Evening')
# temp1 <- subset(Y,select=var1)
# acp1 <- PCA(temp1)
# Gath_V$new_gpe[match(var1,Gath_V$var)] <- k.temp+1
# k.temp <- k.temp + 1

# var1 <- c('HDB01_02','HDB03')
# temp1 <- subset(Y,select=var1)
# acp1 <- PCA(temp1)
# Gath_V$new_gpe[match(var1,Gath_V$var)] <- k.temp+1
# k.temp <- k.temp + 1

# var1 <- c('M02','CLo')
# temp1 <- subset(Y,select=var1)
# acp1 <- PCA(temp1)
# Gath_V$new_gpe[match(var1,Gath_V$var)] <- k.temp+1
# k.temp <- k.temp + 1
# Y$HDB01_02_03 <- pmax(Y$HDB01_02+Y$HDB03,1)
# 
# # 
# var1 <- c('HDB01_02','HDB03')
# temp1 <- subset(Y,select=var1)
# acp1 <- PCA(temp1)
# Gath_V$new_gpe[match(var1,Gath_V$var)] <- k.temp+1
# k.temp <- k.temp + 1

# var1 <- c(paste0('D',c(1,4,5)))
# temp1 <- subset(Y,select=var1)
# acp1 <- PCA(temp1)
# Gath_V$new_gpe[match(var1,Gath_V$var)] <- k.temp+1
# k.temp <- k.temp + 1

# var1 <- c("corTC",'corRF')
# temp1 <- subset(Y,select=var1)
# acp1 <- PCA(temp1)
# Gath_V$new_gpe[match(var1,Gath_V$var)] <- k.temp+1
# k.temp <- k.temp + 1

# var1 <- c(paste0('M.',sprintf("%02d",c(7:8,12))))
# temp1 <- subset(Y,select=var1)
# acp1 <- PCA(temp1)
# Gath_V$new_grp[match(var1,Gath_V$var)] <- k.temp+1
# k.temp <- k.temp + 1

Gath_V <- subset(Gath_V,!is.na(new_gpe))  ## remove those with NA
new.grp <- unique(Gath_V$new_gpe[which(!(is.na(Gath_V$new_gpe)))])
Gath_V$final_grp <- LETTERS[match(Gath_V$new_gpe,new.grp)]
final.grp <- unique(Gath_V$final_grp[which(!(is.na(Gath_V$final_grp)))])

# plot of the different PCA + calculation of synthetic variable
new_data <- Avg
nom <- colnames(new_data)
k2 <- length(final.grp)
nbr <- 4
nbc <- 3

pdf("Output/02-Synthetic variables.pdf",width=16,height=12)
par(mfrow=c(nbr,nbc))
for(i in 1:k2)
{
  var.k <- subset(Gath_V,final_grp==final.grp[i])
  Z <- subset(Y,select=as.character(var.k$var))
  if(ncol(Z)>1)
  {
    acp <- PCA(Z,graph=F)
    plot.acp.var2(acp,final.grp[i])
    new_data$temp <- acp$ind$coord[,1];
  }else
  {
    plot(c(0,1),c(0,1),type="n",main=paste("Synth. var. grp ",final.grp[i],sep=""),xlab="",ylab="")
    text(0.5,0.5,colnames(Z))
    new_data$temp <- Z[,1]
  }
  colnames(new_data) <- c(nom,paste("y",final.grp[i],sep=""));
  nom <- colnames(new_data)
  cat("fin var.synth ",final.grp[i]," at ",as.character(Sys.time()),"\n")
}
dev.off()

write.table(Gath_V[order(Gath_V$final_grp),],"Output/02-Synthetic variables.csv",quote=F,row.names=F,sep=";",dec=".",col.names=T);

# new_data$yA <- NULL
variable <- match(paste0('y',LETTERS[1:k2]),names(new_data))
Yk <- new_data[,variable]
rownames(Yk) <- new_data$ID

# selection of the number of customer cluster

library(doSNOW);library(parallel);library(snow);library(foreach)

KM.function <- function(nb){
  temp.hclust <- hclust(dist(Yk))
  temp.tree <- cutree(temp.hclust,nb)
  cluster.centers <- foreach(i=1:nb, .combine = rbind) %do% colMeans(Yk[which(temp.tree==i),])
  # temp <- kmeans(Yk,nb,iter.max = 1000,nstart=1000)
  
  temp <- kmeans(Yk,cluster.centers,iter.max = 1000)
  return(temp$tot.withinss)
}
# nb.min <- 2
# nb.max <- 10
# nb_cores <- detectCores()-1
# cl <- makeSOCKcluster(rep("localhost", nb_cores)) 
# registerDoSNOW(cl)
# indice.coeur <- (nb.max-nb.min)%/%nb_cores
# n.start <- c(nb.min+(0:(nb_cores-1)*indice.coeur),nb.max+1)
# nb.km <- foreach(i=1:nb_cores,.combine=c,.packages='foreach') %dopar% {
#   vect <-n.start[i]:(n.start[i+1]-1)
#   sapply(vect, KM.function)
# }
# stopCluster(cl)        
# 
# reduc.var.clust <- diff(nb.km)/nb.km[-length(nb.km)]
# res.km <- data.frame(nb=nb.min:nb.max,vartot = nb.km,crit = c(NA,reduc.var.clust))
# K <- res.km$nb[min(which(res.km$crit > -0.05))-1]
K <- 15

# clustering : KMeans
temp.hclust <- hclust(dist(Yk))
temp.tree <- cutree(temp.hclust,K)
cluster.centers <- foreach(i=1:K, .combine = rbind) %do% colMeans(Yk[which(temp.tree==i),])
# temp <- kmeans(Yk,nb,iter.max = 1000,nstart=1000)

new_fit <- kmeans(Yk,cluster.centers,iter.max = 100000)
# print(new_fit$size)

new_data$grp <- new_fit$cluster

Weight <-  as.numeric(table(new_data$grp)/nrow(new_data));
print(percent(Weight))  # 15 groups
names.clust <- names(table(new_data$grp))

if(min(Weight)<0.2){
  rm.clust <- as.numeric(names.clust[which(Weight<0.15)])  # 15% of the population, need to be adapted
  change.clust <- which(new_fit$cluster %in% rm.clust)
  test <- Yk[-change.clust,]
  grp <- new_data$grp[-change.clust]
  for (i in 1:length(change.clust)){
    train <- Yk[change.clust[i],]
    temp <- knn(test,train,factor(rep(1,nrow(test))),k=1)
    new_data$grp[change.clust[i]] <- grp[attributes(temp)$nn.index[,1]]
  }
}
newWeight <-  as.numeric(table(new_data$grp)/nrow(new_data));
print(percent(newWeight))
K <- length(unique(new_data$grp))
new_data$grp <- match(new_data$grp,unique(new_data$grp))
# new_fit <- kmeans(Yk, K,nstart = 1000,iter.max=1000)

data_train <- subset(new_data,select=c('grp',names(Yk)))
data_train$grp <- as.factor(data_train$grp)
svm.model <- svm(grp ~ ., data=data_train)
svm.pred <- predict(svm.model, data_train)
percent(sum(svm.pred==new_data$grp)/nrow(new_data))

write.table(new_data,"Output/02-Customers Groups.csv",quote=F,row.names=F,sep=";",dec=".",col.names=T);

# temp <- PCA(Yk,graph = FALSE)
# plot(temp$ind$coord[,1],temp$ind$coord[,2],col=new_data$grp,pch=20)
# 
# color.aster <-  c("#FF6B6B", "#4ECDC4", "#556270","#9669FE","#57D53B")
# couleurs <- color.aster[1:K]
# 
# pdf("Output/02-ACP_cluster.pdf",width=12,height=6)
# par(mfrow=c(1,2))
# plot(temp$ind$coord[,1],temp$ind$coord[,2],
#      xlab=paste('Factorial Axis 1',sep=""),ylab=paste("Factorial Axis 2",sep=""),pch=20,col=couleurs[new_data$grp])
# abline(v=0,lty="dashed",col="gray")
# abline(h=0,lty="dashed",col="gray")
# legend("topright",paste("G",1:K,sep=""),col=couleurs,cex=0.8,pch=20)#,bty="n")
# 
# plot(0, type='n', xlim=c(-1,1), ylim=c(-1,1),axes=T, xlab='Factorial Axis 1', ylab='Factorial Axis 2')
# a <- pi/2 - 2*pi/200*0:200
# abline(v=0,lty="dashed")
# abline(h=0,lty="dashed")
# polygon( cos(a), sin(a) )
# arrows(0, 0, x1 = temp$var$coord[,1], y1 = temp$var$coord[,2], length = 0.15, angle = 15,code = 2)
# text(temp$var$coord[,1]*1.05,temp$var$coord[,2]*1.05,rownames(temp$var$coord),cex=0.8)
# dev.off()


#####################################
source("00-AFD_procedures_saracco.R")
nom.var <- names(Yk)
temp.data <- new_data
rownames(temp.data) <- temp.data$ID
VectNom <- sort(unique(new_data$grp))
listEch <-split(subset(temp.data,select=nom.var),temp.data$grp)

###
res<-AFDnum.prog(listEch,VectNom,AffichIndividu=FALSE)
AFD_coord <- as.data.frame(res$variableDiscr.ck)
AFD_coord$grp <- new_data$grp[match(rownames(AFD_coord),new_data$ID)]

# AFD_coord <- subset(AFD_coord,select=c("ident","gpe","c1","c2"))

color.aster <-c("#FF6B6B", "#4ECDC4", "#556270","#9669FE","#57D53B","#9932CC")

couleurs <- color.aster[1:K]

pdf(paste0("Output/02-AFD_groupe.pdf"),width=12,height=6)
par(mfrow=c(1,2))
plot(AFD_coord$c1,AFD_coord$c2,xlab=paste("C1 (Discr. power= ",round(res$pouv.discrim[1]*100),"%)",sep=""),ylab=paste("C2 (Discr. power = ",round(res$pouv.discrim[2]*100),"%)",sep=""),pch="*",col=couleurs[AFD_coord$grp])
abline(v=0,lty="dashed",col="gray")
abline(h=0,lty="dashed",col="gray")
legend("bottomleft",paste("G",1:K,sep=""),col=couleurs,cex=0.8,pch=20,bty="n")

plot(0, type='n', xlim=c(-1,1), ylim=c(-1,1),axes=T, xlab='Axe 1', ylab='Axe 2')
a <- pi/2 - 2*pi/200*0:200
abline(v=0,lty="dashed")
abline(h=0,lty="dashed")
polygon( cos(a), sin(a) )
arrows(0, 0, x1 = res$mat.correlations[,1], y1 = res$mat.correlations[,2], length = 0.15, angle = 15,code = 2)
text(res$mat.correlations[,1]*1.05,res$mat.correlations[,2]*1.05,colnames(res$X),cex=0.8)
# text(pmax(res$mat.correlations[,1]*0.9,-1),pmax(res$mat.correlations[,2]*1.1,-1),etiq,cex=0.8)
dev.off()

Group <- new_data
save(Group,file="Output/02- Punggol - Groups variation.RData")
