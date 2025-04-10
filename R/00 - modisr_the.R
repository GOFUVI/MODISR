modisr_the <- rlang::new_environment()


modisr_the$config <- list()

modisr_the$config$gshhg_ver <- "latest"

modisr_the$config$gshhg_shp_path <-  file.path(tools::R_user_dir("MODISR","cache"),"gshhg","GSHHS_shp","f","GSHHS_f_L1.shp")

modisr_the$config$shoreline_path <-  file.path(tools::R_user_dir("MODISR","cache"), "shoreline.RData")

modisr_the$config$crs_sinu <- "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs +type=crs"

modisr_get_crs_sinu <- function(){

  modisr_the$config$crs_sinu

}


modisr_get_shoreline_path <- function(){

  modisr_the$config$shoreline_path

}

#' @export
modisr_set_shoreline_path <- function(path){

  modisr_the$config$shoreline_path <- path

}

