#!/usr/bin/env Rscript
#
# Use the aggregated Census block-level data to generate population-weighted
# averages for higher-level geographies
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(data.table)
library(pbapply)

source("util.R")

# Setup -------------------------------------------------------------------

pboptions(type = "timer")

# Aggregate ---------------------------------------------------------------

aggregate_larger_geographies <- function(region, # e.g. "NE1"
                                         blocks, # "blocks_2000" / "blocks_2010"
                                         species, # e.g. "ec"
                                         urban_rural, # "urban" / "rural"
                                         aggregated_geography = "block_groups" # "block_groups" / "tracts" / "counties"
                                         ) {
  if (grepl("2000", blocks)) {
    block_geoid_column <- "BLKIDFP00"
    geoid_column <- GEOID_NAMES_2000[[aggregated_geography]]
  } else {
    block_geoid_column <- "GEOID10"
    geoid_column <- "GEOID10"
  }
  block_group_geography <- gsub("blocks", aggregated_geography, blocks)
  geoid_length <- GEOID_LENGTHS[[aggregated_geography]]
  
  message("> Reading block classifications")
  block_classifications <- fread(
    file.path(CLASSIFICATIONS_DIR, sprintf("%s.csv.gz", blocks)),
    colClasses = list(character = block_geoid_column)
  )
  
  message("> Reading aggregated data")
  aggregated <- fread(
    sprintf(
      "output/aggregated/%s_%s_%s_%s.csv.gz",
      region, blocks, urban_rural, species
    ),
    colClasses = list(character = block_geoid_column)
  )
  
  # message("> Reading block group classifications")
  # block_group_classifications <- fread(
  #   file.path(CLASSIFICATIONS_DIR, sprintf("%s.csv.gz", block_group_geography)),
  #   colClasses = list(character = block_group_geoid_column)
  # )[substr(BKGPIDFP00, 1, 2) %in% FIPS_BY_REGION[[region]]]
  
  # data.table cascaded bracket syntax is very messy here - using one operation
  # on each line instead
  message(sprintf("> Generating higher-level aggregates (%s)", aggregated_geography))
  result <- block_classifications[aggregated, on = block_geoid_column]
  result <- result[, predicted := fifelse(is.na(predicted_within),
                                          predicted_nearest,
                                          predicted_within)]
  result <- result[, (geoid_column) := substr(get(block_geoid_column), 1, geoid_length)]
  result <- result[, list(predicted = sum(predicted * population) / sum(population),
                          pct_pop_predicted = sum(!is.na(predicted_within)) / .N),
                   by = c("year", geoid_column)]
  
  return(result)
}

invisible(pbapply(
  expand.grid(
    region = names(FIPS_BY_REGION),
    # region = "NE1",
    blocks = c("blocks_2000", "blocks_2010"),
    species = unique(get_predicted_files("Non-urban areas at 1km spatial resolution")$species),
    # species = "ec",
    urban_rural = c("urban", "rural"),
    aggregated_geography = setdiff(names(GEOID_LENGTHS), "blocks")
  ),
  1,
  function(row) {
    output_file <- file.path(
      AGGREGATED_DIR,
      sprintf(
        "%s_%s_%s_%s.csv.gz",
        row["region"], gsub("blocks", row["aggregated_geography"], row["blocks"]),
        row["urban_rural"], row["species"]
      )
    )
    if (!file.exists(output_file)) {
      message(sprintf("Generating %s", output_file))
      fwrite(
        aggregate_larger_geographies(
          region = row["region"],
          blocks = row["blocks"],
          species = row["species"],
          urban_rural = row["urban_rural"],
          aggregated_geography = row["aggregated_geography"]
        ),
        output_file
      )
    } else {
      message(sprintf("Skipping %s", output_file))
    }
  }
))
