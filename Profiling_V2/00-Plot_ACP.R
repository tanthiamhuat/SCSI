plot.acp.var <- function(acp)
{

    cos2weak <- which(acp$var$cos2[,1]<0.45)
    plot(0, type='n', xlim=c(-1,1), ylim=c(-1,1),axes=T, xlab=paste('Axe 1 (eig=',round(acp$eig[1,1],2),' - ',round(acp$eig[1,2]),'%)',sep=""),
    ylab=paste('Axe 2 (eig=',round(acp$eig[2,1],2),' - ',round(acp$eig[2,2]),'%)',sep=""))
    a <- pi/2 - 2*pi/200*0:200
    abline(v=0,lty="dashed")
    abline(h=0,lty="dashed")
    polygon( cos(a), sin(a) )
    coul_flech <- rep(1,nrow(acp$var$coord));coul_flech[cos2weak] <- 2
    nom_etiq <- rep("",nrow(acp$var$coord)); nom_etiq[cos2weak] <- paste("(",round(acp$var$cos2[cos2weak],2),")",sep="")
    nom_etiq <- paste(rownames(acp$var$coord),nom_etiq,sep="");
    arrows(0, 0, x1 = acp$var$coord[,1], y1 = acp$var$coord[,2], length = 0.15, angle = 15,code = 2,col=coul_flech)
    text(acp$var$coord[,1]*1.05,acp$var$coord[,2]*1.05,nom_etiq,cex=0.8)
}


plot.acp.var2 <- function(acp,gpe)
{

    cos2weak <- which(acp$var$cos2[,1]<0.45)
    plot(0, type='n', xlim=c(-1,1), ylim=c(-1,1),axes=T, xlab=paste('Axe 1 (eig=',round(acp$eig[1,1],2),' - ',round(acp$eig[1,2]),'%)',sep=""),
    ylab=paste('Axe 2 (eig=',round(acp$eig[2,1],2),' - ',round(acp$eig[2,2]),'%)',sep=""),main=paste("Var. synth. gpe ",gpe,sep=""))
    a <- pi/2 - 2*pi/200*0:200
    abline(v=0,lty="dashed")
    abline(h=0,lty="dashed")
    polygon( cos(a), sin(a) )
    coul_flech <- rep(1,nrow(acp$var$coord));coul_flech[cos2weak] <- 2
    nom_etiq <- rep("",nrow(acp$var$coord)); nom_etiq[cos2weak] <- paste("(",round(acp$var$cos2[cos2weak],2),")",sep="")
    nom_etiq <- paste(rownames(acp$var$coord),nom_etiq,sep="");
    arrows(0, 0, x1 = acp$var$coord[,1], y1 = acp$var$coord[,2], length = 0.15, angle = 15,code = 2,col=coul_flech)
    text(acp$var$coord[,1]*1.05,acp$var$coord[,2]*1.05,nom_etiq,cex=0.8)
}

#var.synth <- function(X)
