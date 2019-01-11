# http://www.sthda.com/english/wiki/print.php?id=237

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'Profiling_V2/Output/01-Punggol_Indicators.RData'))
load(paste0(path,'Profiling_V2/Output/04-Final_Profile.RData'))

#install.packages("cluster")
#install.packages("dendextend")
library(cluster)
library(dendextend)
library(factoextra)

Customers <- inner_join(Y,indicator, by=c("service_point_sn"="ID","block")) %>% 
             dplyr::mutate(AverageMonthlyConsumption=round(amc)) %>%
             dplyr::select_("service_point_sn","block","room_type","num_house_member","AverageMonthlyConsumption","AverageMonthlyOccupancy","Profile") 

Customers_Details <- inner_join(Customers,family_details,by=c("service_point_sn","num_house_member")) %>%
                     dplyr::mutate(MonthlyLPCD=round(AverageMonthlyConsumption/num_house_member)) %>%
                     dplyr::select_("service_point_sn","block","room_type","AverageMonthlyConsumption","online_status","AverageMonthlyConsumption",
                                    "AverageMonthlyOccupancy","Profile","MonthlyLPCD","num_house_member") 

data1 <- Customers_Details %>% dplyr::select_("MonthlyLPCD","AverageMonthlyConsumption","num_house_member")
data <- Customers_Details %>% dplyr::select_("MonthlyLPCD","AverageMonthlyConsumption")

# Dissimilarity matrix
d <- dist(data, method = "euclidean")

# Hierarchical clustering using Ward's method
res.hc <- hclust(d, method = "ward.D2" )

# Plot the obtained dendrogram
plot(res.hc, cex = 0.6, hang = -1)

# Cut tree into 4 groups
grp <- cutree(res.hc, k = 4)
# Number of members in each cluster
table(grp)

plot(res.hc, cex = 0.6)
rect.hclust(res.hc, k = 4, border = 2:5)

library(factoextra)
fviz_cluster(list(data = data, cluster = grp))

data_cluster <- cbind(data1,grp)
data_cluster$service_point_sn <- Customers_Details$service_point_sn
data_cluster$index <- row.names(data_cluster)

data_cluster <- data_cluster %>% dplyr::select_("index","service_point_sn","AverageMonthlyConsumption","MonthlyLPCD","num_house_member","grp")
