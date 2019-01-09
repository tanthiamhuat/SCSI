## https://nsaunders.wordpress.com/2012/08/27/custom-css-for-html-generated-using-rstudio/
options(rstudio.markdownToHTML = 
          function(inputFile, outputFile) {      
            require(markdown)
            markdownToHTML(inputFile, outputFile, stylesheet='custom.css')   
          }
)