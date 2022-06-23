#!/usr/bin/env Rscript
#
# Join the aggregated data and urban/rural classifications to create files for
# distribution, subsetting urban predictions to urban Census blocks / block
# groups only.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(data.table)
library(pbapply)

source("util.R")

# Setup -------------------------------------------------------------------

pboptions(type = "timer")

generate_distrib <- function(region, # e.g. "NE1"
                             geography, # "blocks_2000" / "blocks_2010"
                             species, # e.g. "ec"
                             urban_rural # "urban" / "rural"
                             ) {
  geoid_column <- fcase(
    geography == "blocks_2000", "BLKIDFP00",
    geography == "block_groups_2000", "BKGPIDFP00",
    default = "GEOID10"
  )
  
  message("> Reading classifications")
  classifications <- fread(
    file.path(CLASSIFICATIONS_DIR, sprintf("%s.csv.gz", geography)),
    colClasses = list(character = geoid_column)
  )
  
  message("> Reading aggregated data")
  aggregated <- fread(
    sprintf(
      "output/aggregated/%s_%s_%s_%s.csv.gz",
      region, geography, urban_rural, species
    ),
    colClasses = list(character = geoid_column)
  )
  
  if (urban_rural == "urban") {
    message("> Subsetting to urban areas only")
    urban_geoids <- unique(classifications[urban == 1][[geoid_column]])
    result <- aggregated[get(geoid_column) %in% urban_geoids]
  } else {
    result <- aggregated
  }
  
  if (grepl("blocks", geography)) {
    message("> Choosing best predictor and subsetting columns")
    result <- result[, predicted := fifelse(is.na(predicted_within),
                                        predicted_nearest,
                                        predicted_within)]
  }
  subset_columns <- c(geoid_column, "year", "predicted")
  result <- result[, ..subset_columns]
  
  return(result)
}

invisible(pbapply(
  expand.grid(
    region = names(FIPS_BY_REGION),
    # region = "NE1",
    geography = c("blocks_2000", "blocks_2010", "block_groups_2000", "block_groups_2010"),
    species = unique(get_predicted_files("Non-urban areas at 1km spatial resolution")$species),
    # species = "ec",
    urban_rural = c("urban", "rural")
  ),
  1,
  function(row) {
    output_file <- file.path(
      DISTRIB_DIR,
      sprintf(
        "%s_%s_%s_%s.csv.gz",
        row["region"], row["geography"], row["urban_rural"], row["species"]
      )
    )
    if (!file.exists(output_file)) {
      message(sprintf("Generating %s", output_file))
      fwrite(
        generate_distrib(
          region = row["region"],
          geography = row["geography"],
          species = row["species"],
          urban_rural = row["urban_rural"]
        ),
        output_file
      )
    } else {
      message(sprintf("Skipping %s", output_file))
    }
  }
))
