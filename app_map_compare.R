# Wed Mar 27 08:38:47 2024 ------------------------------


# -------------------- USER EDIT HERE -------------------------------------

# make sure your Google Drive is synced to your desktop
## instructions: https://support.google.com/drive/answer/10838124?hl=en

## you do not have to sync all folders, only the data in this folder:
## Shared drives\LIHI\2023 (v4.0)\1_Data\pins-inclusivity-maps

# Input ? What drive is your Google Drive mapped to ? Should be a capital letter.
inp_gdrive = "K"

# ------------------ DO NOT EDIT BELOW ------------------------------------

# this application creates a visualization of lihi4 and lihi5 community area
# maps and demographics. its purpose is for quality checks in the data.

# libraries ---------------------------------------------------------------

options(warn = -1)

if (!require(shiny)) install.packages('shiny')
library(shiny)

if (!require(leaflet)) install.packages('leaflet')
library(leaflet)

if (!require(pins)) install.packages('pins')
library(pins)

if (!require(dplyr)) install.packages('dplyr')
library(dplyr)

if (!require(sf)) install.packages('sf')
library(sf)

if (!require(mapview)) install.packages('mapview')
library(mapview)

# functions ---------------------------------------------------------------

f_load_data <- function(inp_pin,
                        inp_name){
  out_data <- pins::pin_read(board = inp_pin,
                             name = inp_name)
  return(out_data)
}

f_create_scale <- function(inp_values){
  scale_values <- c(0.05,0.1,1,5,10,50,100,500,1000,5000,10000,
                    50000,100000,10^6,50^7,10^8,50^8)
  max_v <- max(inp_values,na.rm = TRUE)
  choice_v <- which.min(abs(8-max_v/scale_values))
  choice_v <- scale_values[choice_v]
  max_scale <- round(max_v/choice_v)*choice_v
  return(seq(from = 0,
             to = ifelse(max_scale < max_v,
                         max_scale + choice_v,
                         max_scale),
             by = choice_v))
  
}

# load data ---------------------------------------------------------------

pin_path <- paste0(inp_gdrive,":/Shared drives/LIHI/2024 (v5.0)/1_Data/pins-inclusivity-maps")

pin_app <- pins::board_folder(path = pin_path)

dat_hosp <- f_load_data(pin_app,"hospital_compare")
dat_hosp_camp <- f_load_data(pin_app,"hospital_campus_compare")
list_hosp_isc <- f_load_data(pin_app, "hospital_isochrones_compare")
dat_hosp_zip <- f_load_data(pin_app, "hospital_zipcode_compare")
dat_zip <- f_load_data(pin_app, "zipcode_compare")

names_hosp_isc_lihi5 <- list_hosp_isc$names_lihi5
names_hosp_isc_lihi4 <- list_hosp_isc$names_lihi4


# app interface -----------------------------------------------------------

ui <- fluidPage(
  
  # Application title
  titlePanel(span("Explore LIHI 4.0 and LIHI 5.0 results")),
  
  sidebarLayout(
    sidebarPanel(textInput(inputId = "inputPrvdr",
                           label = "Provider number",
                           value = "010005"),
                 selectInput(inputId = "inputVar",
                             label = "Color variable",
                             choices = c(#"Patients (count per 1,000)",
                               "Population (65 and over)",
                               "Median income",
                               "Education",
                               "American Indian and Alaska Native (ZCTA population, %)",
                               "Asian (ZCTA population, %)",
                               "Black or African American (ZCTA population, %)",
                               "Hispanic or Latino (ZCTA population, %)",
                               "Other race/ethnicity (ZCTA population, %)",
                               "Two or more races (ZCTA population, %)",
                               "White (not Hispanic/Latino) (ZCTA population, %)",
                               "Persons of Color (non-white) (ZCTA population, %)",
                               "Total days of care",
                               "Total cases",
                               "Total charges (Medicare)"),
                             selected = "Total cases"),
                 selectInput(inputId = "colourType",
                             label = "Color fill",
                             choices = c("Reds",
                                         "Oranges",
                                         "Greens",
                                         "Blues",
                                         "Purples"),
                             selected = "Blues"),
                 checkboxInput(inputId = "inputBoundary",
                               label = "Boundary display on",
                               value = TRUE),
                 checkboxInput(inputId = "inputLegend",
                               label = "Legend display on",
                               value = TRUE)
    ),
    mainPanel(
      textOutput("printName"),
      br(),
      tableOutput("printResult"),
      br(),
      # map here
      fluidRow(
        splitLayout(cellWidths = c("50%", "50%"), 
                    p("LIHI 5 map"),
                    p("LIHI 4 map"))
      ),
      fluidRow(
        splitLayout(cellWidths = c("50%", "50%"), 
                    leafletOutput(outputId = "leafletMaplihi5"),
                    leafletOutput(outputId = "leafletMaplihi4"))
      ),
      br(),
      p("For Lown Institute internal use only. Note we may not have approval to 
        share graphics with `Patients (count per 1,000)` graphics. The `Total cases` 
        variable is sourced from a public data set and is preferable. Patient counts
        were not downloaded for LIHI 5.0 data.")
    )
  )
)




# app content -------------------------------------------------------------

server <- function(input, output, session){
  
  # update hospital data
  inp_reactive_hosp <- reactive({
    inp_hosp <- dat_hosp |>
      dplyr::filter(prvdr_num %in% input$inputPrvdr)
    
    return(inp_hosp)
  })
  
  # update variable selection (colour)
  inp_reactive_fill <- reactive({
    inp_fill <- switch(input$inputVar,
                       "Patients (count per 1,000)" = "patient_rate_1000",
                       "Population (65 and over)" = "acs_population_count",
                       "Median income" = "acs_income",
                       "Education" = "acs_education",
                       "American Indian and Alaska Native (ZCTA population, %)" = "acs_race_ain",
                       "Asian (ZCTA population, %)" = "acs_race_asian",
                       "Black or African American (ZCTA population, %)" = "acs_race_black",
                       "Hispanic or Latino (ZCTA population, %)" = "acs_race_hispanic",
                       "Other race/ethnicity (ZCTA population, %)" = "acs_race_other",
                       "Two or more races (ZCTA population, %)" = "acs_race_more",
                       "White (not Hispanic/Latino) (ZCTA population, %)" = "acs_race_white",
                       "Persons of Color (non-white) (ZCTA population, %)" = "acs_race_poc",
                       "Total days of care" = "total_days_of_care",
                       "Total cases" = "total_cases",
                       "Total charges (Medicare)" = "total_charges")
    return(inp_fill)
  })
  
  # campus data 
  inp_reactive_campus <- reactive({
    
    inp_campus <- dat_hosp_camp |> 
      dplyr::filter(prvdr_num %in% input$inputPrvdr)
    
    return(inp_campus)
    
  })
  
  # hospital isochrone
  inp_reactive_iso <- reactive({
    
    inp_iso_pos_lihi5 <- which(names_hosp_isc_lihi5 == input$inputPrvdr)
    inp_iso_lihi5 <- list_hosp_isc$lihi5[[inp_iso_pos_lihi5]]
    inp_iso_lihi5 <- st_cast(inp_iso_lihi5, "POLYGON")
    
    inp_iso_pos_lihi4 <- which(names_hosp_isc_lihi4 == input$inputPrvdr)
    if (length(inp_iso_pos_lihi4) != 0){
      inp_iso_lihi4 <- list_hosp_isc$lihi4[[inp_iso_pos_lihi4]]
      inp_iso_lihi4 <- st_cast(inp_iso_lihi4, "POLYGON")
    } else {
      inp_iso_lihi4 = integer(length = 0)
    }
    
    return(list(lihi5 = inp_iso_lihi5,
                lihi4 = inp_iso_lihi4))
    
  })
  
  # hospital zip codes 
  inp_reactive_zip <- reactive({
    
    inp_hosp_zip <- dat_hosp_zip |> 
      dplyr::filter(prvdr_num == input$inputPrvdr)
    
    # zip codes
    inp_zip <- dat_zip |> 
      inner_join(inp_hosp_zip,
                 by = "zcta")
    
    return(inp_zip)
    
  })
  
  # colour fill variable
  inp_reactive_fill_pal <- reactive({
    inp_zip <- inp_reactive_zip()
    inp_fill <- inp_reactive_fill()
    
    # base on lihi 5 counts & lihi 4 counts
    inp_fill_r <- paste0(inp_fill, "_lihi5")
    inp_fill_r2 <- paste0(inp_fill,"_lihi4")
    
    ## set palette
    values_fill <- c(inp_zip[[inp_fill_r]],
                     inp_zip[[inp_fill_r2]])
    values_fill <- values_fill[!is.nan(values_fill)]
    values_fill <- values_fill[!is.infinite(values_fill)]
    inp_values <- f_create_scale(values_fill)
    
    ## choose colour fill 
    if (grepl("acs_race",inp_fill)){
      max_pal = 1
    } else {
      max_pal = max(inp_values,na.rm = T)
    }
    pal_m <- colorNumeric(
      palette = input$colourType,
      domain = c(0,max_pal),
      na.color = "#DEDEDE"
    )
    
    ## create legend input
    
    m_colors = pal_m(c(NA,inp_values))
    m_labels = inp_values
    
    if (inp_fill == "total_charges"){
      m_labels <- c("Small count",
                    paste0('$',formatC(m_labels, digits = 0,
                                       big.mark=',', format = 'f')))
    } else if (inp_fill == "acs_income"){
      m_labels <- c("No data",
                    paste0('$',formatC(m_labels, digits = 0,
                                       big.mark=',', format = 'f')))
    } else if (grepl("acs_race",inp_fill)){
      m_labels <- c("No data", 
                    round(m_labels*100))
    } else if (inp_fill %in% c("total_days_of_care","total_cases")){
      m_labels <- c("Small count",
                    round(m_labels))
    } else {
      m_labels <- c("No data",
                    round(m_labels))
    }
    
    return(list(inp_palette = pal_m,
                inp_labels = m_labels,
                inp_colors = m_colors))
  })
  
  # create map output
  foundational.map_lihi5 <- reactive({
    leaf_fill <- inp_reactive_fill_pal()
    inp_fill <- inp_reactive_fill()
    inp_fill_r <- paste0(inp_fill, "_lihi5")
    inp_zip_data <- inp_reactive_zip()
    
    inp_camps <- inp_reactive_campus() |> 
      dplyr::filter(version == "LIHI 5")
    
    out_leaf_lihi5 <- leaflet(height = 600,
                              width = 600) |> 
      addProviderTiles("CartoDB.Positron",) |> 
      addMarkers(data = inp_camps,
                 label = inp_camps$campus_id) |> 
      addPolygons(data = inp_zip_data, 
                  fillColor = ~leaf_fill$inp_palette(inp_zip_data[[inp_fill_r]]),
                  fillOpacity = 0.8,
                  weight = 1, 
                  smoothFactor = 0.2,
                  color = "#808080") 
    
    
    if (input$inputBoundary == TRUE){
      out_leaf_lihi5 <- out_leaf_lihi5 |> 
        addPolygons(data = inp_reactive_iso()$lihi5,
                    fillColor = NA,
                    fillOpacity = 0)  
    }
    
    if (input$inputLegend == TRUE){
      out_leaf_lihi5 <- out_leaf_lihi5 |> 
        addLegend(colors = leaf_fill$inp_colors,
                  labels = leaf_fill$inp_labels)
    }
    
    out_leaf_lihi5
    
  })
  
  # create map output
  foundational.map_lihi4 <- reactive({
    leaf_fill <- inp_reactive_fill_pal()
    inp_fill <- inp_reactive_fill()
    inp_fill_r <- paste0(inp_fill, "_lihi4")
    inp_zip_data <- inp_reactive_zip()
    
    inp_iso <- inp_reactive_iso()$lihi4
    
    if (length(inp_iso) != 0){
      out_leaf_lihi4 <- leaflet(height = 600,
                                width = 600) |> 
        addProviderTiles("CartoDB.Positron",) |> 
        addMarkers(data = inp_reactive_campus() |> 
                     dplyr::filter(version == "LIHI 4")) |> 
        addPolygons(data = inp_zip_data, 
                    fillColor = ~leaf_fill$inp_palette(inp_zip_data[[inp_fill_r]]),
                    fillOpacity = 0.8,
                    weight = 1, 
                    smoothFactor = 0.2,
                    color = "#808080") 
      
      
      if (input$inputBoundary == TRUE){
        out_leaf_lihi4 <- out_leaf_lihi4 |> 
          addPolygons(data = inp_iso,
                      fillColor = NA,
                      fillOpacity = 0)  
      }
      
      if (input$inputLegend == TRUE){
        out_leaf_lihi4 <- out_leaf_lihi4 |> 
          addLegend(colors = leaf_fill$inp_colors,
                    labels = leaf_fill$inp_labels)
      }
    } else {
      out_leaf_lihi4 <- leaflet(height = 600,
                                width = 600)
    }
    
    out_leaf_lihi4
    
  })
  
  # print output
  output$leafletMaplihi5 <- renderLeaflet({
    
    foundational.map_lihi5()
    
  })
  
  output$leafletMaplihi4 <- renderLeaflet({
    
    foundational.map_lihi4()
    
  })
  
  
  # create hospital output
  output$printName <- renderText({
    
    inp_hosp <- inp_reactive_hosp() |> 
      dplyr::filter(version == "LIHI 5")
    
    return(c(inp_hosp$Name,
             " in ",
             inp_hosp$City,
             ",",
             inp_hosp$State))
    
  })
  
  output$printResult <- renderTable({
    
    inp_hosp <- inp_reactive_hosp()
    
    inp_hosp_lihi5 <- inp_hosp |> 
      dplyr::filter(version == "LIHI 5")
    
    inp_hosp_lihi4 <- inp_hosp |> 
      dplyr::filter(version == "LIHI 4")
    
    otp_tbl <- tibble(Result = c("Grade",
                                 "Income star",
                                 "Education star",
                                 "Racial inclusivity star"),
                      `LIHI 5` = c(inp_hosp_lihi5$TIER_3_GRADE_Inclusivity,
                                   inp_hosp_lihi5$TIER_4_STARS_Inclusivity_Income,
                                   inp_hosp_lihi5$TIER_4_STARS_Inclusivity_Education,
                                   inp_hosp_lihi5$TIER_4_STARS_Inclusivity_Race))
    
    if (nrow(inp_hosp_lihi4) > 0){
      otp_tbl <- otp_tbl |> 
        bind_cols(tibble(`LIHI 4` = c(inp_hosp_lihi4$TIER_3_GRADE_Inclusivity,
                                      inp_hosp_lihi4$TIER_4_STARS_Inclusivity_Income,
                                      inp_hosp_lihi4$TIER_4_STARS_Inclusivity_Education,
                                      inp_hosp_lihi4$TIER_4_STARS_Inclusivity_Race)))
    }
                      
    
    return(otp_tbl)
    
    
  },
  bordered = TRUE,
  colnames = TRUE)
  
}


# run the application -----------------------------------------------------

shinyApp(ui = ui, 
         server = server)

