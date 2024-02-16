modisr_aqua_available_products <- c("MODIS AQUA L2 SST")

modisr_aqua_product_sort_name <- function(product){

  c("MODIS AQUA L2 SST" = "MODISA_L2_SST",
    "MODIS AQUA L3 Binned SST" = "MODISA_L3b_SST",
    "MODIS AQUA L3 Binned CHL" = "MODISA_L3b_CHL",
    "MODIS AQUA L3 Mapped CHL" = "MODISA_L3m_CHL"
    )[product]

}

#' https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html
#' @importFrom rlang %||%
modisr_aqua_list_files <- function(product = "MODIS AQUA L2 SST",  max_results = 20, temporal = NULL, bounding_box = NULL, polygon = NULL, time_resolution = NULL){


  short_name <- modisr_aqua_product_sort_name(product)

  url <- glue::glue("https://cmr.earthdata.nasa.gov/search/granules.umm_json?sort_key=short_name&sort_key=start_date&short_name={short_name}&provider=OB_DAAC")

  if(!is.null(temporal)){
    url <- glue::glue("{url}&temporal={temporal[1]},{temporal[2]}")
  }

  if(!is.null(bounding_box)){

    url <- glue::glue("{url}&bounding_box={bounding_box[1]},{bounding_box[2]},{bounding_box[3]},{bounding_box[4]}")

  }


  if(!is.null(time_resolution) && time_resolution %in% c("DAY","MO","YR","8D")){


    url <- glue::glue("{url}&granule_ur[]=*{short_name}*.{time_resolution}.*&options[granule_ur][pattern]=true")



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


modisr_aqua_download_files <- function(files, dest, key, workers = 1) {


  future::plan("multisession", workers = workers)

  files %<>% dplyr::mutate(to_download = stringr::str_remove(GranuleUR, paste0(CollectionReference$Version,"_",CollectionReference$ShortName,"_")))


  download_oceandata_file <- function(file){


    url <- sprintf("https://oceandata.sci.gsfc.nasa.gov/ob/getfile/%s?appkey=%s", file, key)

    download.file(url, file.path(dest,file))

  }


  files$to_download %>% furrr::future_walk(download_oceandata_file)



  files$downloaded_file_path <- file.path(dest,files$to_download)

  return(files)

}

#' @importFrom magrittr %>%
modisr_aqua_read_metadata <- function(con){

  global <- ncdf4::ncatt_get(con, 0)

  vars <- con$var %>% names()

  vars_metadata <- vars %>% purrr::map(\(var) ncdf4::ncatt_get(con,var)) %>% magrittr::set_names(vars)

  dimensions_metadata <- con$dim

  out <- list(global = global, vars = vars_metadata, dimensions = dimensions_metadata)

  return(out)

}

#' @export
modisr_aqua_read_file_metadata <- function(file){

  con <- ncdf4::nc_open(file)

  on.exit(ncdf4::nc_close(con))

  out <- modisr_aqua_read_metadata(con)

  return(out)


}


modisr_aqua_read_vars <- function(con, vars){

out <- vars %>% purrr::map(\(var) ncdf4::ncvar_get(con, var)) %>% magrittr::set_names(vars)

return(out)

}

#' @export
modisr_aqua_read_file_vars <- function(file, vars){

con <- ncdf4::nc_open(file)

on.exit(ncdf4::nc_close(con))

out <- modisr_aqua_read_vars(con, vars)

return(out)


}


modisr_aqua_matrix_data_from_folder <- function(folder, var, workers = 1){

out <- matrix()



files <- list.files(folder, pattern = "*\\.nc", full.names = T)

file <- files[1]

browser()

file_metadata <- modisr_aqua_read_file_metadata(file)

start_time <- file_metadata$global$time_coverage_start
end_time <- file_metadata$global$time_coverage_end

file_data <- modisr_aqua_read_file_vars(file,c(var, "navigation_data/longitude","navigation_data/latitude"))

file_result <- c(file_data,list(start_time = start_time, end_time = end_time))

return(out)
}
