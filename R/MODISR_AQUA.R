modisr_aqua_available_products <- c("MODIS AQUA L2 SST")

modisr_aqua_product_sort_name <- function(product){

  c("MODIS AQUA L2 SST" = "MODISA_L2_SST",
    "MODIS AQUA L3 Binned CHL" = "MODISA_L3b_CHL",
    "MODIS AQUA L3 Mapped CHL" = "MODISA_L3m_CHL"
    )[product]

}

#' @importFrom rlang %||%
modisr_aqua_list_files <- function(product = "MODIS AQUA L2 SST",  max_results = 20, temporal = NULL){



  url <- glue::glue("https://cmr.earthdata.nasa.gov/search/granules.umm_json?sort_key=short_name&sort_key=start_date&short_name={modisr_aqua_product_sort_name(product)}&provider=OB_DAAC")

  if(!is.null(temporal)){
    url <- glue::glue("{url}&temporal={temporal[1]},{temporal[2]}")
  }

  results_to_retrieve <- max_results

  if(is.null(results_to_retrieve)) {

    url_temp <- glue::glue("{url}&page_size=1")

    con <- curl::curl(url_temp)

    results_to_retrieve <- suppressWarnings(readLines(con)) %>% jsonlite::fromJSON() %>% purrr::pluck("hits")

    page_size <- 2000
  }else{


    page_size <- min(2000,max_results)
  }




  pages_to_retrieve <- seq(1,ceiling(results_to_retrieve / 2000))



  out <- pages_to_retrieve %>% purrr::map(\(page_num){


    url <- glue::glue("{url}&page_size={page_size}&page_num={page_num}")

    con <- curl::curl(url)

    result <- suppressWarnings(readLines(con)) %>% jsonlite::fromJSON()

    return(result$items)

  }) %>% purrr::list_rbind()




  out %<>% tidyr::unnest(c(meta, umm))


  return(out)
}


modisr_aqua_download_files <- function(files, dest, key) {

  files %>% purrr::walk(\(file){


  url <- sprintf("https://oceandata.sci.gsfc.nasa.gov/ob/getfile/%s?appkey=%s", file, key)

  download.file(url, dest)

})

}

#' @importFrom magrittr %>%
modisr_aqua_read_metadata <- function(con){

  global <- ncdf4::ncatt_get(con, 0)

  vars <- con$var %>% names()

  vars_metadata <- vars %>% purrr::map(\(var) ncdf4::ncatt_get(con,var)) %>% magrittr::set_names(vars)

  dimensions_metadata <- con$dim %>% names()

  out <- list(global = global, vars = vars_metadata, dimensions = dimensions_metadata)

  return(out)

}


modisr_aqua_read_vars <- function(con, vars){

out <- vars %>% purrr::map(\(var) ncdf4::ncvar_get(con, var)) %>% magrittr::set_names(vars)

return(out)

}
