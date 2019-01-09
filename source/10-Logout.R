cat('Logout\n')
output$Logout <- renderUI({
  session$reload() 
})

