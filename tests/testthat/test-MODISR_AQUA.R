test_that("related files exist", {
  expect_true(is.function(modisr_aqua_read_metadata))
})


describe("modisr_aqua_read_metadata",{

  it("should return the file metadata",{

here::i_am("tests/testthat/test-MODISR_AQUA.R")

connection <- RNetCDF::open.nc(here::here("tests/testthat/data/AQUA_MODIS.20230101.L3b.DAY.SST.nc"))

test <- modisr_aqua_read_metadata(connection, is_binned = TRUE)



expect_snapshot_value(test, style = "serialize")

  })

})

describe("modisr_aqua_list_files",{

  it("should return the result of a search for a product",{
    skip_if_offline()
    skip_on_cran()

    test <- modisr_aqua_list_files()

    test$took <- NULL

    expect_snapshot_value(test, style = "serialize")



  })

  describe("L3b SST",{
    it("should return the serch results",{

      skip_if_offline()
      skip_on_cran()

      test <- modisr_aqua_list_files(product = "MODIS AQUA L3 Binned SST")

      test$took <- NULL

      expect_snapshot_value(test, style = "serialize")

    })

  })


  describe("L3b CHL",{
    it("should return the serch results",{

      skip_if_offline()
      skip_on_cran()

      test <- modisr_aqua_list_files(product = "MODIS AQUA L3 Binned CHL")

      test$took <- NULL

      expect_snapshot_value(test, style = "serialize")

    })

  })

  describe("bounding_box option",{

    # https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#c-bounding-box
    # Bounding Box
    #
    # Bounding boxes define an area on the earth aligned with longitude and latitude. The Bounding box parameters must be 4 comma-separated numbers: lower left longitude, lower left latitude, upper right longitude, upper right latitude.
    # This parameter supports the and/or option as shown below.
    #
    # curl "https://cmr.earthdata.nasa.gov/search/collections?bounding_box[]=-10,-5,10,5"
    #
    # curl "https://cmr.earthdata.nasa.gov/search/collections?bounding_box[]=-10,-5,10,5&bounding_box[]=-11,-6,11,6&options[bounding_box][or]=true"
    #

    it("should retrieve files that overlap the bounding box",{

      skip_if_offline()
      skip_on_cran()
      n_lat  <- 44
      s_lat <- 42
      w_lon <- -10
      e_lon <- -8


      bounding_box <- list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon)


      test <- modisr_aqua_list_files(bounding_box=bounding_box)

      expect_snapshot_value(test, style = "serialize")

    })

  })

  describe("time_resolution option",{

    it("should return the results for the given time resolution",{

      skip_if_offline()
      skip_on_cran()

      test <- modisr_aqua_list_files(product = "MODIS AQUA L3 Binned CHL",time_resolution = "DAY")

      test$took <- NULL

      expect_snapshot_value(test, style = "serialize")


    })

  })

  describe("list ALL files",{

    it("should return all the files",{

      skip("too long")

      skip_if_offline()
      skip_on_cran()


      test <- modisr_aqua_list_files(max_results = NULL)

      test <- modisr_aqua_list_files(max_results = 4000)

    })

  })

})

describe("modisr_aqua_read_vars",{

  it("should read the vars for CHL",{


    connection <- RNetCDF::open.nc(here::here("tests/testthat/data/AQUA_MODIS.20230101.L3b.DAY.CHL.nc"))

    on.exit(RNetCDF::close.nc(connection))

    test <- modisr_aqua_read_vars(connection, is_binned = TRUE)

expect_snapshot_value(test, style = "serialize")

  })

  it("should read the vars for SST",{


    connection <- RNetCDF::open.nc(here::here("tests/testthat/data/AQUA_MODIS.20230701.L3b.DAY.SST.nc"))

    on.exit(RNetCDF::close.nc(connection))

    n_lat  <- 44
    s_lat <- 42
    w_lon <- -10
    e_lon <- -8

    test <- modisr_aqua_read_vars(connection, is_binned = TRUE, bounding_box = list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon))

    expect_snapshot_value(test, style = "serialize")

  })

  describe("auxiliary funs",{

    test_that("initbin works as expected",{


      numrows <- 4320
      test <- initbin(numrows)

expect_snapshot_value(test)

    })


    test_that("lat2row works as expected",{

      # Ejemplo de uso
      numrows <- 4320
      lat <- 45
      test <- lat2row(lat, numrows)
      expect_equal(test,3240)


    })

    test_that("bin2latlon works as expected",{

      numrows <- 4320
      bin_list <- initbin(numrows)
      test <- bin2latlon(19280505,bin_list$basebin,bin_list$numbin,bin_list$latbin)
      expect_snapshot_value(test)
    })

    test_that("latlon2bin works as expected",{


      bin_list <- initbin(numrows)
      test <- latlon2bin(38.52083, 15.52367,bin_list$numbin,bin_list$basebin)
      expect_equal(test, 19280505L)
    })
    test_that("bin2bounds works as expected",{

      numrows <- 4320
      bin_list <- initbin(numrows)
      test <- bin2bounds(19280505,numrows,bin_list$basebin,bin_list$numbin,bin_list$latbin)
      expect_snapshot_value(test)
    })

    test_that("bounding_box2bins works as expected",{

      numrows <- 4320

      n_lat  <- 44
      s_lat <- 42
      w_lon <- -10
      e_lon <- -8
      test <- bounding_box2bins(n_lat,s_lat,w_lon,e_lon,numrows)

    })

  })


})

describe("modisr_aqua_download_files",{

  it("should download all the files in a search result to a target folder and return the search results with the file paths added",{

    skip_if_offline()
    skip_on_cran()

files <- modisr_aqua_list_files()

temp_dir <- tempdir()

target_folder <- file.path(temp_dir, "MODISR")

unlink(target_folder,force = T,recursive = T)

dir.create(target_folder)

test <- modisr_aqua_download_files(files,target_folder, (readLines(here::here("tests/testthat/key"))),workers = 1)


expect_equal(sort(list.files(target_folder,full.names = T)), test$downloaded_file_path)




  })

})



describe("modisr_aqua_download_and_read_data",{

  it("should download all the files in a search result to a target folder and return the search results with the file paths added",{

    skip_if_offline()
    skip_on_cran()

    n_lat  <- 44
    s_lat <- 42
    w_lon <- -10
    e_lon <- -8


    bounding_box <- list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon)

    files <- modisr_aqua_list_files(product = "MODIS AQUA L3 Binned SST",time_resolution = "DAY",temporal = c("2023-07-01","2023-08-31"),max_results = 10,bounding_box = bounding_box)


    temp_dir <- tempdir()

    target_folder <- file.path(temp_dir, "MODISR")

    unlink(target_folder,force = T,recursive = T)

    dir.create(target_folder)


    test <- modisr_aqua_download_and_read_data(files,target_folder, (readLines(here::here("tests/testthat/key"))),workers = 1,bounding_box = bounding_box, is_binned = TRUE)


    expect_equal(sort(list.files(target_folder,full.names = T)), test$downloaded_file_path)




  })

})


describe("modisr_aqua_matrix_data_from_folder",{

  it("should read all the files in a folder and then create a matrix for a designated var",{

    skip_if_offline()
    skip_on_cran()


    n_lat  <- 44
    s_lat <- 42
    w_lon <- -10
    e_lon <- -8


    bounding_box <- list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon)

    files <- modisr_aqua_list_files(product = "MODIS AQUA L3 Binned SST",time_resolution = "DAY",temporal = c("2023-07-01","2023-08-31"),max_results = 10,bounding_box = bounding_box)

    temp_dir <- tempdir()

    target_folder <- file.path(temp_dir, "MODISR")

    unlink(target_folder,force = T,recursive = T)

    dir.create(target_folder)

    files <- modisr_aqua_download_files(files,target_folder, (readLines(here::here("tests/testthat/key"))),workers = 1)

test <- modisr_aqua_read_data_from_folder(target_folder, is_binned = TRUE, bounding_box = bounding_box)






  })

})

describe("land_mask",{

  describe("modisr_get_gshhg_landmask",{

    it("should return the landmask for a given bbox",{

      n_lat  <- 44
      s_lat <- 42
      w_lon <- -10
      e_lon <- -8


      bounding_box <- list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon)


      test <- modisr_get_gshhg_landmask(bounding_box)

      expect_snapshot_value(test, style = "serialize")

    })

  })

})


describe("modisr_binned_to_sf",{

  it("should return an sf object for the data",{


    connection <- RNetCDF::open.nc(here::here("tests/testthat/data/AQUA_MODIS.20230701.L3b.DAY.SST.nc"))

    on.exit(RNetCDF::close.nc(connection))

    n_lat  <- 44
    s_lat <- 42
    w_lon <- -10
    e_lon <- -8

    test_data <- modisr_aqua_read_vars(connection, is_binned = TRUE, bounding_box = list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon))

    test <- modisr_binned_to_sf(test_data)



    expect_snapshot_value(test, style = "serialize")

  })

})

describe("modisr_filter",{

  it("should filter the data",{


    file <- here::here("tests/testthat/data/AQUA_MODIS.20230701.L3b.DAY.SST.nc")



    n_lat  <- 44
    s_lat <- 42
    w_lon <- -10
    e_lon <- -8

    test_data <- modisr_aqua_read_file_vars(file, is_binned = TRUE, bounding_box = list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon))

    cond_fun1 <- function(df) df$sst_sum/df$weights > 17.5
    cond_fun2 <- function(df) df$sst_sum/df$weights < 18

    cond_fun3 <- function(df) df$sst_sum/df$weights > 18.5

    conds <- list(list(cond_fun1,cond_fun2), list(cond_fun3))

test <- modisr_filter(test_data,conds , is_binned = TRUE)

    # ggplot2::ggplot(modisr_binned_to_sf(test)) + ggplot2::geom_sf(ggplot2::aes(color=sst_sum/weights))


expect_snapshot_value(test, style = "serialize")

  })

})


describe("modisr_compute_total_data_area",{


it("should return the total area",{

  file <- here::here("tests/testthat/data/AQUA_MODIS.20230701.L3b.DAY.SST.nc")



  n_lat  <- 44
  s_lat <- 42
  w_lon <- -10
  e_lon <- -8

  test_data <- modisr_aqua_read_file_vars(file, is_binned = TRUE, bounding_box = list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon))

  # modisr_plot_binned_data(test_data, var="sst_sum")


  cond_fun1 <- function(df) df$sst_sum/df$weights > 17.5
  cond_fun2 <- function(df) df$sst_sum/df$weights < 18

  cond_fun3 <- function(df) df$sst_sum/df$weights > 18.5

  conds <- list(list(cond_fun1,cond_fun2), list(cond_fun3))

  filtered <- modisr_filter(test_data,conds , is_binned = TRUE)

  # modisr_plot_binned_data(filtered, var="sst_sum")

  test <- modisr_compute_total_data_area(filtered, is_binned = TRUE)



  expect_snapshot_value(test, style = "json2")


})

})


describe("modisr_compute_missing_bins_percent",{

it("should return the proportion of missing bins",{
  file <- here::here("tests/testthat/data/AQUA_MODIS.20230701.L3b.DAY.SST.nc")



  n_lat  <- 44
  s_lat <- 42
  w_lon <- -10
  e_lon <- -8

  test_data <- modisr_aqua_read_file_vars(file, is_binned = TRUE, bounding_box = list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon))

  test1 <- modisr_compute_missing_bins_percent(test_data)


  cond_fun1 <- function(df) df$sst_sum/df$weights > 17.5
  cond_fun2 <- function(df) df$sst_sum/df$weights < 18

  cond_fun3 <- function(df) df$sst_sum/df$weights > 18.5

  conds <- list(list(cond_fun1,cond_fun2), list(cond_fun3))

  filtered <- modisr_filter(test_data,conds , is_binned = TRUE)

  test2 <- modisr_compute_missing_bins_percent(filtered)

  expect_equal(test1,test2)

  expect_snapshot_value(test1, style = "json2")


})

})


describe("modisr_ts_from_folder",{

  it("should read all the files in a folder and then create ts of path pointers",{

    skip_if_offline()
    skip_on_cran()


    n_lat  <- 44
    s_lat <- 42
    w_lon <- -10
    e_lon <- -8


    bounding_box <- list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon)

    files <- modisr_aqua_list_files(product = "MODIS AQUA L3 Binned SST",time_resolution = "DAY",temporal = c("2023-07-01","2023-08-31"),max_results = 10,bounding_box = bounding_box)

    temp_dir <- tempdir()

    target_folder <- file.path(temp_dir, "MODISR")

    unlink(target_folder,force = T,recursive = T)

    dir.create(target_folder)

    files <- modisr_aqua_download_and_read_data(files,target_folder, (readLines(here::here("tests/testthat/key"))),workers = 1,bounding_box = bounding_box,is_binned = TRUE)

    test <- modisr_ts_from_folder(target_folder, workers = 2)



    expect_snapshot_value(test, style = "json2")


  })

})

describe("modisr_process_ts_binned",{


  n_lat  <- 44
  s_lat <- 42
  w_lon <- -10
  e_lon <- -8


  bounding_box <- list(n_lat = n_lat, s_lat = s_lat, w_lon = w_lon, e_lon = e_lon)

  files <- modisr_aqua_list_files(product = "MODIS AQUA L3 Binned SST",time_resolution = "DAY",temporal = c("2023-07-01","2023-08-31"),max_results = 10,bounding_box = bounding_box)

  temp_dir <- tempdir()

  target_folder <- file.path(temp_dir, "MODISR")

  unlink(target_folder,force = T,recursive = T)

  dir.create(target_folder)

  files <- modisr_aqua_download_and_read_data(files,target_folder, (readLines(here::here("tests/testthat/key"))),workers = 1,bounding_box = bounding_box,is_binned = TRUE)

  ts <- modisr_ts_from_folder(target_folder, workers = 2)

times_two <- function(data){

  data$sst$sum %<>% magrittr::multiply_by(2)

return(data)


}

add_three <-  function(data){

  data$sst$sum %<>% magrittr::add(3)

  return(data)


}

average_tss <- function(data){

  out <- mean(data$sst$sum/data$BinList$weights, na.rm = TRUE)

  return(out)

}

my_steps <- list(step_one = list(fun = add_three, type = "transform"),
              step_two = list(fun = times_two, type = "transform"),
              step_three = list(fun = average_tss, type = "summary", colname = "mean_TSS")
              )




test <- modisr_process_ts_binned(ts, steps = my_steps)

expect_snapshot_value(test)

})
