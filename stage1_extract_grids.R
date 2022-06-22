#!/usr/bin/env Rscript
#
# Read Heresh's particle components data, drop extra columns, convert to spatial
# frames, and save to GeoPackages
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(pbapply)
library(sf)

source("util.R")

# Species from which we will be sampling the grid coordinates
SPECIES_TO_SAMPLE <- "no3" # NO3 files tend to be the smallest

# Setup -------------------------------------------------------------------

pboptions(type = "timer")

# Collect file info -------------------------------------------------------

urban_files <- get_predicted_files("Urban areas at 50m spatial resolution")

rural_files <- get_predicted_files("Non-urban areas at 1km spatial resolution")

# How many files do we have?
xtabs(~ region + year + species, data = urban_files)
xtabs(~ region + year + species, data = rural_files)

# Create crosswalks -------------------------------------------------------
# The same grid is reused within each region for all different species and
# years, so we can just create a single crosswalk for each grid and reuse it for
# each species

read_grid <- function(files,
                      current_region,
                      current_species = SPECIES_TO_SAMPLE) {
  files_to_sample <- files[
    region == current_region & species == current_species
  ]
  
  message(sprintf("Reading %s$%s", current_region, current_species))
  
  # Subset to year file (urban data)
  min_year <- suppressWarnings(min(files_to_sample$year, na.rm = TRUE))
  if (is.na(min_year)) {
    paths_to_sample <- files_to_sample$path
  } else {
    message(sprintf("> Subsetting files to year %s", min_year))
    paths_to_sample <- files_to_sample[year == min_year]$path
  }
  
  # Subset to first year in the file (rural data)
  coords <- rbindlist(lapply(paths_to_sample, readRDS))
  if (length(unique(coords$year)) > 1) {
    message(sprintf("> Subsetting records to year %s", first(coords$year)))
    coords <- coords[year == first(year)]
  }
  
  # Subset columns to lon and lat and assign grid cell numbers
  message("> Extracting coordinates")
  coords <- coords[, list(lon, lat)]
  coords$gridcell_idx <- 1:nrow(coords)
  
  message("> Creating spatial frame")
  coords_sf <- st_as_sf(
    coords,
    coords = c("lon", "lat"),
    crs = 4326,
    agr = "constant"
  )
  
  return(coords_sf)
}

pblapply(
  names(FIPS_BY_REGION),
  function(region) {
    output_path_rural <- file.path(GRIDS_DIR, sprintf("%s_rural.gpkg", region))
    if (!file.exists(output_path_rural)) {
      st_write_gpkg(read_grid(rural_files, region), output_path_rural)
    }
    output_path_urban <- file.path(GRIDS_DIR, sprintf("%s_urban.gpkg", region))
    if (!file.exists(output_path_urban)) {
      st_write_gpkg(read_grid(urban_files, region), output_path_urban)
    }
  }
)
