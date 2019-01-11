load("/srv/shiny-server/DataAnalyticsPortal/data/NonDomestic.RData")
Receivers <- NonDomesticCustomersReceivers_LongLat %>% dplyr::filter(Type=="Receiver")

output$Coverage <- renderLeaflet({
  sector = input$sector
  NonDomesticCustomersReceivers_LongLat <- NonDomesticCustomersReceivers_LongLat %>%
                                           dplyr::filter(Sector %in% sector)
  NonDomesticCustomersReceivers_LongLat_Display <- rbind(NonDomesticCustomersReceivers_LongLat,Receivers)
  
  leafIcons <- icons(
    iconUrl = ifelse (NonDomesticCustomersReceivers_LongLat_Display$Type=="Receiver",
                     "http://52.74.103.158/icon/receiver.png",
              ifelse (NonDomesticCustomersReceivers_LongLat_Display$Sector=="Hotel",
                     "http://52.74.103.158/icon/transmitter_H.png", 
              ifelse (NonDomesticCustomersReceivers_LongLat_Display$Sector=="Retail",
                     "http://52.74.103.158/icon/transmitter_R.png",
              ifelse (NonDomesticCustomersReceivers_LongLat_Display$Sector=="Wafer Fabrication & Semiconductors",
                      "http://52.74.103.158/icon/transmitter_W.png", 
              ifelse (NonDomesticCustomersReceivers_LongLat_Display$Sector=="Laundry",
                      "http://52.74.103.158/icon/transmitter_L.png",
              ifelse (NonDomesticCustomersReceivers_LongLat_Display$Sector=="Biomedical Manufacturing",
                      "http://52.74.103.158/icon/transmitter_B.png",
              ifelse (NonDomesticCustomersReceivers_LongLat_Display$Sector=="Chemicals",
                      "http://52.74.103.158/icon/transmitter_C.png", 
              ifelse (NonDomesticCustomersReceivers_LongLat_Display$Sector=="Petrochemicals",
                      "http://52.74.103.158/icon/transmitter_P.png",NA)))))))),
  
    iconWidth = 20, iconHeight = 20
  )
  
  leaflet(data = NonDomesticCustomersReceivers_LongLat_Display) %>% addTiles() %>%
    addMarkers(~NonDomesticCustomersReceivers_LongLat_Display$Longitude, ~NonDomesticCustomersReceivers_LongLat_Display$Latitude, 
               popup=NonDomesticCustomersReceivers_LongLat_Display$CompanyName,
               icon = leafIcons) %>%
    addCircles(lng = 103.6503, lat = 1.332937, radius = 1380) %>% # Tuas, 1.38km
    addCircles(lng = 103.9025, lat = 1.403054, radius = 900) %>% # Punggol 274C, 0.9km
    addCircles(lng = 103.9059, lat = 1.400575, radius = 750) %>% # Punggol 199C, 0.75 km
    addCircles(lng = 103.9086, lat = 1.404082, radius = 800) %>% # Punggol 613C, 0.8 km
    addCircles(lng = 103.8527, lat = 1.371990, radius = 5000) %>% # Ang Mo Kio, 5 km
    addCircles(lng = 103.8861, lat = 1.379040, radius = 2500) %>% # Hougang: 2.5 km
    addCircles(lng = 103.7018, lat = 1.335610, radius = 8000) %>% # Jurong West: 8 km
    addCircles(lng = 103.7486, lat = 1.346500, radius = 2500) ## Bukit Batok: 2.5km
})

output$Receiver_info <- renderUI({
  HTML('<table>
                  <tr>
                  <td> <p align="justify">Receivers are shown with their area of signal coverage in their circles. Click on individual
                                          receivers and transmitters to see its details. Transmitters are abbreviated according to their sector.</p></td>
                  </tr>
                  </table>')
})





