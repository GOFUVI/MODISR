modist_get_gshhg_prefix_and_url <- function(gshhg_version = modisr_the$config$gshhg_ver){


  if(gshhg_version == "latest"){

    gshhg_prefix <- curl::curl("https://www.ngdc.noaa.gov/mgg/shorelines/data/gshhg/latest/") %>% readLines() %>% stringr::str_extract("gshhg-shp-.*?\\.zip") %>% purrr::discard(is.na) %>% stringr::str_remove("\\.zip")
    url <- glue::glue("https://www.ngdc.noaa.gov/mgg/shorelines/data/gshhg/latest/{gshhg_prefix}.zip")
  }else{
    gshhg_prefix <- glue::glue("gshhg-shp-{gshhg_version}")
    url<- glue::glue("https://www.ngdc.noaa.gov/mgg/shorelines/data/gshhg/oldversions/version{gshhg_version}/{gshhg_prefix}.zip")


  }

  out <- list(gshhg_prefix = gshhg_prefix, url = url)

  return(out)

}

modisr_get_gshhg_shp_path <- function(){
  modisr_the$config$gshhg_shp_path
}

#' @export
modisr_install_gshhg <- function(gshhg_version = modisr_the$config$gshhg_ver){

  cache_dir <- tools::R_user_dir("MODISR","cache")


  gshhg_prefix_and_url <- modist_get_gshhg_prefix_and_url(gshhg_version = gshhg_version)

  gshhg_prefix <- gshhg_prefix_and_url$gshhg_prefix

  url <- gshhg_prefix_and_url$url


  if(noD <- !dir.exists(cache_dir)) # should work user specifically:
    dir.create(cache_dir, recursive=TRUE)
  stopifnot(dir.exists(cache_dir))
  destpath <- file.path(cache_dir,glue::glue("{gshhg_prefix}.zip"))

  if(!file.exists(destpath)){

    download.file(url,destfile = destpath)
  }

  dest_folder <-  file.path(cache_dir,"gshhg")

  if(!dir.exists(dest_folder)){
    unzip(destpath,exdir = dest_folder)
  }

  stopifnot(dir.exists(dest_folder))

  shapefile <- modisr_get_gshhg_shp_path()

  stopifnot(file.exists(shapefile))

  shoreline <-sf::read_sf(shapefile)

  shoreline_path <- modisr_get_shoreline_path()

  save(shoreline,file = shoreline_path)

  file.mode(shoreline_path,mode = "444")

  invisible(NULL)

}


modisr_get_gshhg_shoreline <- function(){

  load(modisr_get_shoreline_path())

  return(shoreline)

}

#' @export
modisr_shoreline_for_bbox <- function(bounding_box = list(n_lat = 90, s_lat = -90, w_lon = -180, e_lon = 180)){

  shoreline <- modisr_get_gshhg_shoreline()



  bbox <- data.frame(
    lon=c(bounding_box$e_lon,bounding_box$e_lon,bounding_box$w_lon,bounding_box$w_lon,bounding_box$e_lon),
    lat=c(bounding_box$s_lat,bounding_box$n_lat,bounding_box$n_lat,bounding_box$s_lat,bounding_box$s_lat)
  ) %>% sf::st_as_sf(coords = c("lon","lat"))

  sf::st_crs(bbox) <- 4326

  bbox %<>%
    dplyr::summarise(geometry = sf::st_combine(geometry)) %>%
    sf::st_cast("POLYGON")


  sf::sf_use_s2(FALSE)
  out <- sf::st_intersection(shoreline,bbox)

  return(out)

}

#' Projection is sinusoidal
#' @export
modisr_get_gshhg_landmask <- function(bounding_box = list(n_lat = 90, s_lat = -90, w_lon = -180, e_lon = 180)){

  mask <- modisr_shoreline_for_bbox(bounding_box = bounding_box)

  crs <-modisr_get_crs_sinu()
  mask %<>% sf::st_transform(crs)


  return(mask)

}
