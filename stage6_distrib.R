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
                             geography, # "blocks" / "block_groups" / "tracts" / "counties"
                             year, # 2000 / 2010
                             species, # e.g. "ec"
                             urban_rural # "urban" / "rural"
                             ) {
  geography_name <- sprintf("%s_%s", geography, year)
  if (year == 2000) {
    geoid_column <- GEOID_NAMES_2000[[geography]]
  } else {
    geoid_column <- "GEOID10"
  }
  
  message("> Reading classifications")
  classifications <- fread(
    file.path(CLASSIFICATIONS_DIR, sprintf("%s.csv.gz", geography_name)),
    colClasses = list(character = geoid_column)
  )
  
  message("> Reading aggregated data")
  aggregated <- fread(
    sprintf(
      "output/aggregated/%s_%s_%s_%s.csv.gz",
      region, geography_name, urban_rural, species
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
  
  if (grepl("blocks", geography_name)) {
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
    # region = names(FIPS_BY_REGION),
    region = setdiff(names(FIPS_BY_REGION), "M8"),
    # region = "NE1",
    geography = names(GEOID_LENGTHS),
    year = c(2000, 2010),
    species = unique(get_predicted_files("Non-urban areas at 1km spatial resolution")$species),
    # species = "ec",
    urban_rural = c("urban", "rural")
  ),
  1,
  function(row) {
    output_file <- file.path(
      DISTRIB_DIR,
      sprintf(
        "%s_%s_%s_%s_%s.csv.gz",
        row["region"], row["geography"], row["year"], row["urban_rural"], row["species"]
      )
    )
    if (!file.exists(output_file)) {
      message(sprintf("Generating %s", output_file))
      fwrite(
        generate_distrib(
          region = row["region"],
          geography = row["geography"],
          year = row["year"],
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
