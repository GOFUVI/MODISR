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

    test <- modisr_aqua_list_files()

    test$took <- NULL

    expect_snapshot_value(test, style = "serialize")



  })

  describe("list ALL files",{

    it("should return all the files",{

      skip("too long")
      test <- modisr_aqua_list_files(max_results = NULL)

      test <- modisr_aqua_list_files(max_results = 4000)

    })

  })

})
