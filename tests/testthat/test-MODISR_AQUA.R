test_that("related files exist", {
  expect_true(is.function(modisr_aqua_read_metadata))
})


describe("modisr_aqua_read_metadata",{

  it("should return the file metadata",{

here::i_am("tests/testthat/test-MODISR_AQUA.R")

connection <- ncdf4::nc_open(here::here("tests/testthat/data/AQUA_MODIS.20240131T142000.L2.SST.NRT.nc"))

on.exit(ncdf4::nc_close(connection))

test <- modisr_aqua_read_metadata(connection)


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

      bounding_box <- c(-10,42,-9,43)

      test <- modisr_aqua_list_files(bounding_box=bounding_box)

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

  it("should read the vars",{

    connection <- ncdf4::nc_open(here::here("tests/testthat/data/AQUA_MODIS.20240131T142000.L2.SST.NRT.nc"))

    on.exit(ncdf4::nc_close(connection))

    test <- modisr_aqua_read_vars(connection, "geophysical_data/sst")

expect_snapshot_value(test, style = "serialize")

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
