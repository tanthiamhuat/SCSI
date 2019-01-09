######################################################################################
# Programmes d'AFD sous R : 
#            "AFDnum.prog" pour les sorties numeriques
#            "AFDgraph.prog" pour les sorties graphiques
######################################################################################

AFDnum.prog<-function(listEch,vect.NomGrp,AffichIndividu=T){
#
#----------------------------------------------------------------------------------
# En entree : - listeEch = objet de type liste contenant la liste des echantillons 
#                          de chaque groupe		
#             - vect.NomGrp = vecteur contenant le nom de chacun des groupes
#
# En sortie : - facteurDiscr.uk = matrice des facteurs discriminants 
#             - variableDiscr.ck = matrices des variables discriminantes
#             - X = matrice (n*p) des donnees initiales (avec tous les groupes) 
#             - mat.correlations = matrices des correlations entre les variables
#                                  initiales et les variables discriminantes
#             - vect.nk = vecteur des effectifs de chaque groupe
#             - K = le nombre de groupes
#----------------------------------------------------------------------------------
#
K<-length(listEch)
p<-ncol(listEch[[1]])
X<-listEch[[1]]
for (k in 2:K){
	X<-rbind(X,listEch[[k]])
}
n<-nrow(X)
V<-var(X)*(n-1)/n
list.Vk<-list()
list.gk<-list()
vect.nk<-rep(0,K)
for (k in 1:K){
	list.gk[[k]]<-matrix(apply(listEch[[k]],2,mean),ncol=1)
	vect.nk[k]<-nrow(listEch[[k]])
	list.Vk[[k]]<-var(listEch[[k]])*(vect.nk[k]-1)/vect.nk[k]
}
nom.X <- rownames(X)
X<-as.matrix(X)
dimnames(X)[[1]]<-rep(vect.NomGrp,vect.nk)
n<-sum(vect.nk)
vect.Pk<-vect.nk/n
g<-matrix(apply(X,2,mean),ncol=1)
Xcent<-X-matrix(rep(1,n),ncol=1)%*%t(g)
W<-matrix(0,nrow=p,ncol=p)
B<-matrix(0,nrow=p,ncol=p)
for (k in 1:K){
	W<-W+vect.Pk[k]*list.Vk[[k]]
	B<-B+vect.Pk[k]*(list.gk[[k]]-g)%*%t(list.gk[[k]]-g)
}

# Calcul du facteur discriminant :
#=================================
res<-eigen(solve(V)%*%B,symmetric=F)

cat("Liste des pouvoirs discriminants :",fill=T)
cat("----------------------------------",fill=T)
print(Re(res$values[1:c(K-1)]))

facteurDiscr.uk<-as.matrix(Re(res$vectors[,1:(K-1)]))
dimnames(facteurDiscr.uk)[[2]]<-paste("u",1:ncol(facteurDiscr.uk),sep="")

cat("Matrice des facteurs discriminants :",fill=T)
cat("------------------------------------",fill=T)
print(facteurDiscr.uk)

# Calcul des variables discriminantes :
#======================================
variableDiscr.ck<-as.matrix(Xcent%*%facteurDiscr.uk)
dimnames(variableDiscr.ck)[[2]]<-paste("c",1:ncol(variableDiscr.ck),sep="")
# dimnames(variableDiscr.ck)[[1]]<-c(1:n)
dimnames(variableDiscr.ck)[[1]]<- nom.X

if (AffichIndividu==T){
cat("Matrice des variables discriminantes :",fill=T)
cat("--------------------------------------",fill=T)
print(data.frame(variableDiscr.ck))
}

# Calcul des correlations (variables init, variables discr)
#==========================================================
mat.correlations<-matrix(0,nrow=p,ncol=K-1)
for (i in 1:p){
	for (k in 1:(K-1)){
		mat.correlations[i,k]<-cor(Xcent[,i],variableDiscr.ck[,k])
	}
}
dimnames(mat.correlations)[[2]]<-paste("c",1:ncol(mat.correlations),sep="")
dimnames(mat.correlations)[[1]]<-dimnames(X)[[2]]

cat("Matrice des correlations avec les variables discriminantes :",fill=T)
cat("------------------------------------------------------------",fill=T)
print(mat.correlations)

list(pouv.discrim=Re(res$values[1:c(K-1)]),facteurDiscr.uk=facteurDiscr.uk,variableDiscr.ck=variableDiscr.ck,X=X,mat.correlations=mat.correlations,vect.nk=vect.nk,K=K)
}


######################################################################################


AFDgraph.prog<-function(resAFD,x=1,y=2, taille.base =0, coef.base =1,param.cercle=3){
#
#----------------------------------------------------------------------------------
# En entree : - resAFD = sorties de la fonction "AFDnum.prog" ci-dessus
#             - x = numero de l'axe discriminant qui sera represente horizontalement (par defaut l'axe 1)
#             - y = numero de l'axe discriminant qui sera represente verticalement (par defaut l'axe 2)
#   NB : La taille du nom des variables est calculee de la maniere suivante :
#           taille.base+coef.base*(cor(variable, axe disc x)^2+cor(variable, axe disc y)^2)
#        avec :
#             - taille.base = taille de base du nom des variables representees sur le cercle des correlations 
#                             (par defaut 0)
#             - coef.base = coefficient multiplicateur de la qualite de representation des variables dans le plan considere
#                            (par defaut 1)       
#----------------------------------------------------------------------------------
#	
# Projection des individus
#==========================

#par(mfrow=c(1,2))
#windows()
par(pty="m")
min.x<-min(resAFD$variableDiscr.ck[,x])
max.x<-max(resAFD$variableDiscr.ck[,x])
min.y<-min(resAFD$variableDiscr.ck[,y])
max.y<-max(resAFD$variableDiscr.ck[,y])
vect.indice<-c(0,cumsum(resAFD$vect.nk))
for (k in 1:resAFD$K){
plot(resAFD$variableDiscr.ck[c((vect.indice[k]+1):vect.indice[k+1]),x],resAFD$variableDiscr.ck[c((vect.indice[k]+1):vect.indice[k+1]),y],pch=" ",
	xlab=paste("Axe",x),ylab=paste("Axe",y),
	xlim=c(min.x,max.x),ylim=c(min.y,max.y))
text(resAFD$variableDiscr.ck[c((vect.indice[k]+1):vect.indice[k+1]),x],resAFD$variableDiscr.ck[c((vect.indice[k]+1):vect.indice[k+1]),y],
	labels=as.character(dimnames(resAFD$X)[[1]][c((vect.indice[k]+1):vect.indice[k+1])]),col=k,
	cex= 1)
par(new=T)
}
par(new=F)

# Cercle des correlations
#=========================
x11()
#windows()
par(pin=c(param.cercle,param.cercle))
plot(resAFD$mat.correlations[,x],resAFD$mat.correlations[,y],pch=" ",
       xlab=paste("Axe",x),ylab=paste("Axe",y),xlim=c(-1,1),ylim=c(-1,1))
text(resAFD$mat.correlations[,x],resAFD$mat.correlations[,y],
         labels=as.character(dimnames(resAFD$X)[[2]]),
			cex=taille.base + coef.base * (resAFD$mat.correlations[, x]^2 + resAFD$mat.correlations[, y]^2))
segments(0,0,resAFD$mat.correlations[,x],resAFD$mat.correlations[,y])
abline(h=0,v=0)
xx<-seq(from=-1,to=1,length=1001)
yyPlus<-sqrt(1-xx^2)
yyMoins<--yyPlus
par(new=T)
plot(xx,yyPlus,pch=" ",xlim=c(-1,1),ylim=c(-1,1),xlab="Axe 1",ylab="Axe 2")
lines(xx,yyPlus)
par(new=T)
plot(xx,yyMoins,pch=" ",xlim=c(-1,1),ylim=c(-1,1),xlab="Axe 1",ylab="Axe 2")
lines(xx,yyMoins)
par(new=F)
par(pty="m")

}

