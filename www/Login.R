#### Log in module ###
## https://gist.github.com/withr/9001831  ##

USER <- reactiveValues(Logged = Logged)

passwdInput <- function(inputId, label) {
  tagList(
    tags$label(label),
    tags$input(id = inputId, type="password", value="")
  )
}

output$uiLogin <- renderUI({
  if (USER$Logged == FALSE) {
    wellPanel(
      textInput("userName", "UserName:"),
      passwdInput("passwd", "Password:"),
      br(),
      actionButton("Login", "Log in")
      )
  }
})

output$pass <- renderText({  
  if (USER$Logged == FALSE) {
    if (!is.null(input$Login)) {
      if (input$Login > 0) {
        Username <- isolate(input$userName)
        Password <- isolate(input$passwd)
        
        Id.username1 <- which(PASSWORD1$UserName == Username)
        Id.password1 <- which(PASSWORD1$Password == Password)
        
        if (length(Id.username1) > 0 & length(Id.password1) > 0) {
          if (Id.username1 == Id.password1) {
            USER$Logged <- TRUE
          } 
        } else {
          "UserName or Password failed!"
        }
        
        Id.username2 <- which(PASSWORD2$UserName == Username)
        Id.password2 <- which(PASSWORD2$Password == Password)
        
        if (length(Id.username2) > 0 & length(Id.password2) > 0) {
          if (Id.username2 == Id.password2) {
            USER$Logged <- TRUE
          } 
        } else {
          "UserName or Password failed!"
        }
    
      }
    }
  }
})