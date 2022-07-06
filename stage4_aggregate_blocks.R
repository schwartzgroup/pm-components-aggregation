#!/usr/bin/env Rscript
#
# Use the generated point-in-polygon and nearest-neighbour crosswalks to
# aggregate predictions to Census blocks as follows:
#
# 1. For Census blocks encompassing grid cell centroids: take the mean of
#    encompassed grid cells.
# 2. For Census blocks that do not encompass any grid cell centroids (e.g.
#    coastal Census blocks), use the value of the nearest grid cell
#
# Separate aggregations will be calculated for urban predictions, which will
# likely be dominated by point-in-polygon joins, and non-urban predictions,
# which will have a greater proportion of nearest-neighbour joins.
#
# Aggregated data will contain the number of encompassed grid cells, if any, for
# potential later use in sensitivity analyses e.g. restricted only to blocks
# that contain at least one grid cell centroid.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(data.table)
library(pbapply)

source("util.R")

# Setup -------------------------------------------------------------------

pboptions(type = "timer")

# Collect file info -------------------------------------------------------

urban_files <- get_predicted_files("Urban areas at 50m spatial resolution")

rural_files <- get_predicted_files("Non-urban areas at 1km spatial resolution")

# How many files do we have?
xtabs(~ region + year + species, data = urban_files)
xtabs(~ region + year + species, data = rural_files)

# Load data ---------------------------------------------------------------
# Only load files that will be used across all aggregations - the rest will be
# loaded on-demand to save memory

# Aggregate Census blocks -------------------------------------------------

read_data <- function(files, current_region, current_species) {
  prediction_column <- sprintf("final.predicted.%s", current_species)
  files_to_read <- files[region == current_region & species == current_species]
  
  message(sprintf("> Reading %s parts", nrow(files_to_read)))
  data <- pbapply(
    files_to_read,
    1,
    function(row) {
      # data <- readRDS(row["path"])
      data <- readRDS(row["path"])[, c("year", prediction_column)]
      names(data) <- c("year", "predicted")
      return(data)
    }
  )
  
  message("> Binding rows")
  data <- rbindlist(data)
  
  n_years <- length(unique(data$year))
  message(sprintf("> Detected %s years", n_years))
  
  message("> Generating grid cell IDs")
  data$gridcell_idx <- rep(1:(nrow(data) / n_years), n_years)
  
  return(data)
}

aggregate_blocks <- function(region, # e.g. "NE1"
                             geography, # "blocks_2000" / "blocks_2010"
                             geoid_column, # "BLKIDFP00" / "GEOID10")
                             files, # urban_files / rural_files
                             species, # e.g. "ec"
                             urban_rural # "urban" / "rural"
                             ) {
  join_columns <- c("year", geoid_column)
  
  # Read data - using colClasses to avoid dropping leading zeroes from GEOIDs
  predictions <- read_data(files, region, species)
  crosswalk_within <- fread(
    file.path(
      CROSSWALKS_DIR,
      sprintf("%s_%s_within_%s.csv.gz", region, urban_rural, geography)
    ),
    # colClasses = c("integer", "character")
    colClasses = list(character = geoid_column)
  # The "within" join was a left spatial join on grid cells within Census
  # blocks, so some records may have not been joined to any Census block - need
  # to drop these
  )[get(geoid_column) != ""]
  crosswalk_nearest <- fread(
    file.path(
      CROSSWALKS_DIR,
      sprintf("%s_%s_nearest_%s.csv.gz", region, urban_rural, geography)
    ),
    # colClasses = c("character", "integer", "numeric")
    colClasses = list(character = geoid_column)
  )
  
  # Calculate the mean of encompassed grid cell predictions
  message("> Joining and aggregating within")
  aggregated_within <- predictions[crosswalk_within,
                                   on = "gridcell_idx"
                                   ][, list(predicted_within = mean(predicted),
                                            n_gridcells_within = .N),
                                     by = c("year", geoid_column)]
  
  # Join the nearest grid cell predictions
  message("> Joining nearest")
  aggregated_nearest <- predictions[crosswalk_nearest,
                                    on = "gridcell_idx",
                                    allow.cartesian = TRUE]
  setnames(
    aggregated_nearest,
    c("predicted", "distance"),
    c("predicted_nearest", "distance_nearest")
  )
  
  # Join predictions together
  # We are left joining on `aggregated_nearest` because `aggregated_nearest`
  # is guaranteed to contain all Census blocks while `aggregated_within` may not
  # have the coastal ones, if any
  message("> Merging within and nearest")
  aggregated_final <- aggregated_within[aggregated_nearest, on = join_columns
                                        ][, gridcell_idx := NULL]
  
  return(aggregated_final)
}

pbapply(
  expand.grid(
    region = setdiff(names(FIPS_BY_REGION), "M8"),
    # region = "M8",
    urban_rural = c("rural", "urban"),
    geography = c("blocks_2000", "blocks_2010"),
    # geography = c("blocks_2010", "blocks_2000"),
    # species = unique(urban_files$species)
    species = c("ec", setdiff(urban_files$species, "ec")) # prioritize EC
  ),
  1,
  function(row) {
    output_file <- file.path(
      AGGREGATED_DIR,
      sprintf(
        "%s_%s_%s_%s.csv.gz",
        row["region"], row["geography"], row["urban_rural"], row["species"]
      )
    )
    if (row["geography"] == "blocks_2000") {
      geoid_column <- "BLKIDFP00"
    } else {
      geoid_column <- "GEOID10"
    }
    if (row["urban_rural"] == "urban") {
      files <- urban_files
    } else {
      files <- rural_files
    }
    if (file.exists(output_file)) {
      message(sprintf("Skipping %s", output_file))
    } else {
      message(sprintf("Generating %s", output_file))
      fwrite(
        aggregate_blocks(
          region = row["region"],
          geography = row["geography"],
          geoid_column = geoid_column,
          files = files,
          species = row["species"],
          urban_rural = row["urban_rural"]
        ),
        output_file
      )
    }
  }
)
