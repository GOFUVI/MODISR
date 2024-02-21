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
modisr_aqua_list_files <- function(product = "MODIS AQUA L2 SST",  max_results = 20, temporal = NULL, bounding_box = list(n_lat = 90, s_lat = -90, w_lon = -180, e_lon = 180), polygon = NULL, time_resolution = NULL){


  short_name <- modisr_aqua_product_sort_name(product)

  url <- glue::glue("https://cmr.earthdata.nasa.gov/search/granules.umm_json?sort_key=short_name&sort_key=start_date&short_name={short_name}&provider=OB_DAAC")

  url <- glue::glue("{url}&bounding_box={bounding_box$w_lon},{bounding_box$s_lat},{bounding_box$e_lon},{bounding_box$n_lat}")

  if(!is.null(temporal)){
    url <- glue::glue("{url}&temporal={temporal[1]},{temporal[2]}")
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

    download.file(url, file.path(dest,file),mode = "wb")

  }


  files$to_download %>% furrr::future_walk(download_oceandata_file)



  files$downloaded_file_path <- file.path(dest,files$to_download)

  return(files)

}

#' @importFrom magrittr %>%
modisr_aqua_read_metadata <- function(con, is_binned = FALSE){


  n_global_attributes <- RNetCDF::file.inq.nc(con)$ngatts

  global_attributes_names <- (seq_len(n_global_attributes) -1) %>% purrr::map(\(id) RNetCDF::att.inq.nc(con, "NC_GLOBAL",id)) %>% purrr::map_chr("name")

  global_attributes <- global_attributes_names %>% purrr::map(\(att) RNetCDF::att.get.nc(con,"NC_GLOBAL",att)) %>% magrittr::set_names(global_attributes_names)

  out <- list(global = global_attributes)

  if(is_binned){

    processing_control_grp <- RNetCDF::grp.inq.nc(connection, "processing_control")

    global_attributes_names <- (seq_len(processing_control_grp$ngatts) -1) %>% purrr::map(\(id) RNetCDF::att.inq.nc(processing_control_grp$self, "NC_GLOBAL",id)) %>% purrr::map_chr("name")

    global_attributes <- global_attributes_names %>% purrr::map(\(att) RNetCDF::att.get.nc(processing_control_grp$self,"NC_GLOBAL",att)) %>% magrittr::set_names(global_attributes_names)

    processing_control <- list(global = global_attributes)


    input_parameters_grp <- RNetCDF::grp.inq.nc(processing_control_grp$self, "input_parameters")






    global_attributes_names <- (seq_len(input_parameters_grp$ngatts) -1) %>% purrr::map(\(id) RNetCDF::att.inq.nc(input_parameters_grp$self, "NC_GLOBAL",id)) %>% purrr::map_chr("name")

    global_attributes <- global_attributes_names %>% purrr::map(\(att) RNetCDF::att.get.nc(input_parameters_grp$self,"NC_GLOBAL",att)) %>% magrittr::set_names(global_attributes_names)

    input_parameters <- list(global = global_attributes)

    processing_control$input_parameters <- input_parameters

    out$processing_control <- processing_control



  }else{

    stop("is_finned = FALSE not implemented")
  }
  return(out)

}

#' @export
modisr_aqua_read_file_metadata <- function(file, is_binned = FALSE){

  con <- RNetCDF::open.nc(file)

  on.exit(RNetCDF::close.nc(con))

  out <- modisr_aqua_read_metadata(con, is_binned = is_binned)

  return(out)


}

# The following functions are based on the pseudocode found in Appendix A of:
#
# Campbell, J.W., J.M. Blaisdell, and M. Darzi, 1995:
# Level-3 SeaWiFS Data Products: Spatial and Temporal Binning Algorithms.
# NASA Tech. Memo. 104566, Vol. 32,
# S.B. Hooker, E.R. Firestone, and J.G. Acker, Eds.,
# NASA Goddard Space Flight Center, Greenbelt, Maryland

initbin <- function(numrows) {
  basebin <- numeric(numrows) # Pre-allocate a vector with the length of numrows
  basebin[1] <- 1 # R is 1-indexed
  latbin <- numeric(numrows) # Pre-allocate a vector for latbin
  numbin <- integer(numrows) # Pre-allocate a vector for numbin

  for (row in 0:(numrows - 1)) {
    latbin[row + 1] <- ((row + 0.5) * 180 / numrows) - 90
    numbin[row + 1] <- as.integer(2 * numrows * cos(latbin[row + 1] * pi / 180) + 0.5)
    if (row > 0) {
      basebin[row + 1] <- basebin[row] + numbin[row]
    }
  }

  totbins <- basebin[numrows] + numbin[numrows] - 1

  list(basebin = basebin, latbin = latbin, numbin = numbin, totbins = totbins)
}

#' row is 0 based!!!
lat2row <- function(lat, numrows) {
  row <- as.integer((90 + lat) * numrows / 180)
  if (row >= numrows) {
    row <- numrows - 1
  }
  return(row)
}

constrain_lat <- function(lat) {
  if (lat > 90) lat <- 90
  if (lat < -90) lat <- -90
  return(lat)
}

constrain_lon <- function(lon) {
  while (lon < -180) lon <- lon + 360
  while (lon > 180) lon <- lon - 360
  return(lon)
}

rowlon2bin <- function(row, lon, numbin, basebin) {
  lon <- constrain_lon(lon)
  col <- as.integer((lon + 180) * numbin[row + 1] / 360) # R es indexado desde 1
  if (col >= numbin[row + 1]) {
    col <- numbin[row + 1] - 1
  }
  return(basebin[row + 1] + col)
}


latlon2bin <- function(lat, lon, numbin, basebin) {
  lat <- constrain_lat(lat)
  lon <- constrain_lon(lon)
  row <- lat2row(lat, numrows) + 1 # Ajustando para indexación basada en 1 en R
  col <- as.integer((lon + 180) * numbin[row] / 360)
  if (col >= numbin[row]) {
    col <- numbin[row] - 1
  }
  return(basebin[row] + col)
}

bin2latlon <- function(bin, basebin, numbin, latbin) {
  if (bin < 1) {
    bin <- 1
  }

  row <- max(which(bin >= basebin))

  clat <- latbin[row]
  clon <- 360 * (bin - basebin[row] + 0.5) / numbin[row] - 180
  return(list(lat = clat, lon = clon))
}

bounding_box2bins <- function(n_lat,s_lat,w_lon,e_lon, numrows){

bin_index <- initbin(numrows)

lat_bin <- bin_index$latbin

rows <- which(lat_bin >= s_lat & lat_bin <= n_lat)


w_bins <-bin_index$basebin[rows] + as.integer((w_lon + 180) * bin_index$numbin[rows] / 360)
e_bins <-bin_index$basebin[rows] + as.integer((e_lon + 180) * bin_index$numbin[rows] / 360)

bins <- purrr::map2(w_bins,e_bins,\(w_bin,e_bin) seq(w_bin,e_bin)) %>% purrr::list_c()

bounds <- bins %>% purrr::map(\(bin) bin2bounds(bin, numrows, bin_index$basebin, bin_index$numbin,bin_index$latbin)  %>% as.data.frame()) %>% purrr::list_rbind()  %>% dplyr::mutate(bin=bins,.before=1)

bounds %<>% dplyr::filter(west >= w_lon & east <= e_lon)



lat_lon <- bounds$bin %>% purrr::map(\(bin) bin2latlon(bin, bin_index$basebin, bin_index$numbin,bin_index$latbin) %>% as.data.frame()) %>% purrr::list_rbind()

out <- dplyr::bind_cols(bounds,lat_lon)

return(out)
}

bin2bounds <- function(bin, numrows, basebin, numbin, latbin) {
  row <- numrows
  if (bin < 1) {
    bin <- 1
  }
  row <- max(which(bin >= basebin))

  north <- latbin[row] + 90 / numrows
  south <- latbin[row] - 90 / numrows
  lon <- 360 * (bin - basebin[row] + 0.5) / numbin[row] - 180
  west <- lon - 180 / numbin[row]
  east <- lon + 180 / numbin[row]
  return(list(north = north, south = south, west = west, east = east))
}


modisr_aqua_read_vars <- function(con, vars= NULL, is_binned = FALSE, bounding_box = list(n_lat = 90, s_lat = -90, w_lon = -180, e_lon = 180), bins = NULL){

  if(is_binned){

    grp <- RNetCDF::grp.inq.nc(con, "level-3_binned_data")

    out <- grp$varids %>% purrr::map(\(varid) {



      var <- RNetCDF::var.get.nc(grp$self,varid)
      var  %<>% as.data.frame() %>% magrittr::set_attributes(.,c(attributes(.),RNetCDF::var.inq.nc(grp$self,varid)))



      return(var)

    })

    var_names <- out %>% purrr::map_chr(\(var) attr(var,"name", exact = T))

    names(out) <- var_names

    numrows <- nrow(out$BinIndex)

if(is.null(bins)){

  bins <- bounding_box2bins(bounding_box$n_lat, bounding_box$s_lat, bounding_box$w_lon, bounding_box$e_lon, numrows)
}



data_to_keep <- which(out$BinList$bin_num %in% bins$bin)

out$BinList %<>% dplyr::slice(data_to_keep) %>% dplyr::left_join(bins, dplyr::join_by(bin_num==bin))

out[!names(out) %in% c("BinList", "BinIndex")] %<>% purrr::map(\(data) dplyr::slice(data,data_to_keep))


out$numrows <- numrows
out$bins <- bins

  }else{

    stop("is_binned = FALSE not implemented")

  }

  return(out)

}

#' @export
modisr_aqua_read_file_vars <- function(file, vars= NULL, is_binned = FALSE, bounding_box = list(n_lat = 90, s_lat = -90, w_lon = -180, e_lon = 180), bins = NULL){

  con <- RNetCDF::open.nc(file)

  on.exit(RNetCDF::close.nc(con))

  out <- modisr_aqua_read_vars(con, vars = vars, is_binned = is_binned, bounding_box = bounding_box, bins = bins)

  return(out)


}


modisr_aqua_read_data_from_folder <- function(folder, vars= NULL, is_binned = FALSE, bounding_box = list(n_lat = 90, s_lat = -90, w_lon = -180, e_lon = 180), workers = 1, bins = NULL){

  out <- matrix()



  files <- list.files(folder, pattern = "*\\.nc", full.names = T)



  out <- files %>% purrr::map(\(file){

    file_metadata <- modisr_aqua_read_file_metadata(file, is_binned = is_binned)



    file_data <- modisr_aqua_read_file_vars(file,  vars = vars, is_binned = is_binned, bounding_box = bounding_box, bins = bins )

    if(is.null(bins)){
      bins <<- file_data$bins
    }

    file_result <- c(file_data)



    return(file_result)

  }, .progress = "Processing Files")



  return(out)
}
