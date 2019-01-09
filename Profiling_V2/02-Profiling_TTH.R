# http://www.sthda.com/english/wiki/ade4-and-factoextra-principal-component-analysis-r-software-and-data-mining
# http://www.sthda.com/english/wiki/factominer-and-factoextra-principal-component-analysis-visualization-r-software-and-data-mining
# https://www.analyticsvidhya.com/blog/2016/03/practical-guide-principal-component-analysis-python/
# https://cos.name/cn/topic/12803/

rm(list=ls(all=TRUE));gc()
if(length(dev.list())>0) dev.off()

library(factoextra)
library(FactoMineR)
library(cluster)

load("Output/01-Punggol_Indicators.RData")
indicator[is.na(indicator)] <- 0

Avg <- indicator[,c(1,3:5)]; ## average daily consumption per customer
rownames(Avg) <- Avg[,1]
Avg <- Avg[,2:4]
df <- scale(Avg)

Y <- indicator[,6:ncol(indicator)];
rownames(Y) <- Avg$ID;
Z <- scale(Y)
rownames(Z) <- indicator[,1]
df <- Z

res.pca <- PCA(Z, graph = FALSE)
eig.val <- get_eigenvalue(res.pca)
fviz_eig(res.pca, addlabels = TRUE, ylim = c(0, 50))
fviz_pca_var(res.pca, col.var = "black")

var <- get_pca_var(res.pca)
library("corrplot")
corrplot(var$cos2, is.corr=FALSE)

fviz_cos2(res.pca, choice = "var", axes = 1:2)

km.res <- kmeans(df,4,nstart = 25)
print(km.res)


fviz_nbclust(df,kmeans,method = "wss") + 
  geom_vline(xintercept = 4,linetype=2)

aggregate(Avg,by=list(cluster=km.res$cluster),mean)
dd <- cbind(Avg,cluster=km.res$cluster)

fviz_cluster(km.res, data = df,
             palette = c("#2E9FDF","#00AFBB","#E7B800","#FC4E07"),
             ellipse.type = "euclid",
             star.plot = TRUE,
             repel = TRUE,
             ggtheme = theme_minimal()
             )

fviz_nbclust(df,pam,method = "silhouette") + theme_classic()

pam.res <- pam(df,2)
print(pam.res)
dd <- cbind(Avg,cluster=pam.res$cluster)
pam.res$medoids
head(pam.res$clustering)

fviz_cluster(pam.res, 
             palette = c("#00AFBB","#FC4E07"),
             ellipse.type = "t",
             star.plot = TRUE,
             repel = TRUE,
             ggtheme = theme_minimal()
)

fviz_nbclust(df,clara,method = "silhouette") + theme_classic()

clara.res <- clara(df,2,samples = 50,pamLike = TRUE)
print(clara.res)

fviz_cluster(clara.res, 
             palette = c("#00AFBB","#FC4E07"),
             ellipse.type = "t",
             geom = "point", pointsize =1,
             ggtheme = theme_minimal()
)

res.dist <- dist(df,method="euclidean")
res.hc <- hclust(d=res.dist,method="ward.D2")
fviz_dend(res.hc,cex = 0.5)

res.coph <- cophenetic(res.hc)
cor(res.dist,res.coph)

res.hc2 <- hclust(d=res.dist,method="average")
cor(res.dist,cophenetic(res.hc2))
fviz_dend(res.hc2,cex = 0.5)

grp <- cutree(res.hc2,k=5)
head(grp,10)
table(grp)
#rownames(df)[grp==2]

fviz_dend(res.hc2,k=5,
          cex=0.5,
          k_colors = c("#2E9FDF","#00AFBB","#E7B800","#FC4E07","#FC4E07"),
          color_labels_by_k = TRUE,
          rect = TRUE)

fviz_cluster(list(data=df,cluster=grp),
             palette=c("#2E9FDF","#00AFBB","#E7B800","#FC4E07","#FC4E07"),
             ellipse.type = "convex",
             repel = TRUE,
             show.clust.cent = FALSE, ggtheme = theme_minimal())

## assessing cluster tendency
library(clustertend)
set.seed(123)
hopkins(df,n=nrow(df)-1)  # value close to 0.5 means not clusterable

fviz_dist(dist(df),show_labels = FALSE) + labs(title="Customer Data")

# Elbow method
fviz_nbclust(df,kmeans,method="wss") + geom_vline(xintercept = 4,linetype=2)+ labs(subtitle="Elbow method")

# Silhouette method
fviz_nbclust(df,kmeans,method="silhouette")+ labs(subtitle="Silhouette method")

# Gap statistic
set.seed(123)
fviz_nbclust(df,kmeans,nstart=25,method="gap_stat",nboot=500)+labs(subtitle="Gap Statistics method")

library(NbClust)
nb <- NbClust(df,distance="euclidean",min.nc=2,max.nc=10,method="kmeans")
fviz_nbclust(nb)

# k-means clustering
km.res <- eclust(df,"kmeans",k=3,nstart=25,graph = FALSE)

# Visual k-means clusters
fviz_cluster(km.res,geom = "point", ellipse.type = "norm", palette="jco",ggtheme = theme_minimal())

# Hierarchical clustering
hc.res <- eclust(df,"hclust",k=3,hc_metric = "euclidean",hc_method = "ward.D2",graph = FALSE)

# Visualize dendrograms
fviz_dend(hc.res,show_labels = FALSE,palette = "jco",as.ggplot=TRUE)

## Cluster validation
fviz_silhouette(km.res,palette="jco",ggtheme=theme_classic())

# Silhouette information
silinfo <- km.res$silinfo
names(silinfo)
head(silinfo$widths[,1:3],10)
silinfo$clus.avg.widths
silinfo$avg.width
km.res$size

sil <- km.res$silinfo$widths[,1:3]
neg_sil_index <- which(sil[,'sil_width']<0)

# Choose the best clustering algorithms
library(clValid)
clmethods <- c("hierarchical","kmeans","pam")
intern <- clValid(df,nClust = 2:6,clMethods = clmethods,validation = "internal")
summary(intern)

stab <- clValid(df,nClust = 2:6,clMethods = clmethods,validation = "stability")
optimalScores(stab)

library(pvclust)
set.seed(123)
res.pv <- pvclust(df,method.dist = "cor",method.hclust = "average",nboot=10)
plot(res.pv,hang=-1,cex=0.5)
pvrect(res.pv)

res.hk <- hkmeans(df,3)
names(res.hk)
res.hk

fviz_dend(res.hk,cex=0.6,palette="jco",rect = TRUE,rect_border = "jco",rect_fill = TRUE)

## visualize the hkmeans final clusters
fviz_cluster(res.hk,palette="jco",repel = TRUE,ggtheme = theme_classic())
