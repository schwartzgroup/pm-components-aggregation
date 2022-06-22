# Constants and functions shared between different scripts

library(data.table)
library(sf)
library(stringr)
library(tools)

# Constants ---------------------------------------------------------------

# FIPS codes for each state in each region
FIPS_BY_REGION <- list(
  "NE1"  = c("09", "23", "25", "33", "44", "50"),
  "MA2"  = c("34", "36", "42"),
  "NEC3" = c("17", "18", "26", "39", "55"),
  "WNC4" = c("19", "20", "27", "29", "31", "38", "46"),
  "SA5"  = c("10", "11", "12", "13", "24", "45", "37", "51", "54"),
  "ESC6" = c("01", "21", "28", "47"),
  "WSC7" = c("05", "22", "40", "48"),
  "M8"   = c("04", "08", "16", "30", "32", "35", "49", "56"),
  # "P9"   = c("02", "06", "15", "41", "53")
  "P9"   = c("06", "41", "53") # removed Alaska and Hawaii
)

# Directories -------------------------------------------------------------

## Inputs ----

POLYGONS_DIR = "/media/qnap3/ShapeFiles/Polygons"

DECENNIAL_DIR = "/media/qnap4/Covariates/nhgis/output/"

# This directory should contain all of Heresh's model prediction files
HERESH_DATA_DIR <- "/media/qnap4/Heresh/"

## Outputs ----

GEO_DIR = "output/geo/"
GRIDS_DIR <- "output/grids/"
CLASSIFICATIONS_DIR = "output/urban_classifications"
CROSSWALKS_DIR = "output/crosswalks"
AGGREGATED_DIR = "output/aggregated"
DISTRIB_DIR = "output/distrib"

WORKING_GPKG <- "output/working.gpkg"

invisible(lapply(
  c(GEO_DIR, GRIDS_DIR, CLASSIFICATIONS_DIR, CROSSWALKS_DIR, AGGREGATED_DIR, DISTRIB_DIR),
  dir.create,
  showWarnings = FALSE,
  recursive = TRUE
))

# Functions ---------------------------------------------------------------

# Get information about what files are available and what is in them
get_predicted_files <- function(subdirectory_name,
                                data_dir = HERESH_DATA_DIR) {
  paths <- Sys.glob(file.path(HERESH_DATA_DIR, "*", subdirectory_name, "*.rds"))
  return(
    data.table(path = paths
    )[, basename := basename(path)
    ][, `:=` (year = str_extract(basename, "^[0-9]{4}"),
              region = str_extract(basename, "[A-Z]+[0-9]"),
              part_number = str_extract(basename, "(?<=part)[0-9]"),
              species = str_extract(
                # Inconsistent file names - some are {SPECIES}.rds;
                # some are {SPECIES}.part[0-9].rds
                str_replace(basename, "\\.part[0-9]", ""),
                "[^\\.]+(?=\\.rds)"
              ))
    ][, basename := NULL]
  )
}

# GPKG can't write directly to the QNAPs - need to write somewhere else then
# move to the desired location
st_write_gpkg <- function(obj, dsn, ...) {
  temp_path <- gsub(
    ".gpkg",
    "-temp.gpkg",
    file.path(Sys.getenv("HOME"), basename(dsn))
  )
  suppressWarnings(file.remove(temp_path))
  st_write(obj, temp_path, file_path_sans_ext(basename(dsn)), ..., driver = "GPKG")
  message(sprintf("Moving: %s => %s", temp_path, dsn))
  file.copy(temp_path, dsn)
  file.remove(temp_path)
  invisible()
}
