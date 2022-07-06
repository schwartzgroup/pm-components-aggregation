#!/usr/bin/env Rscript
#
# Detect which prediction files have missingness by comparing yearly counts of
# predictions and print the files with inconsistent counts
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(data.table)
library(pbapply)

source("util.R")

# Setup -------------------------------------------------------------------

pboptions(type = "timer")

urban_files <- get_predicted_files("Urban areas at 50m spatial resolution")

rural_files <- get_predicted_files("Non-urban areas at 1km spatial resolution")

# Diagnostics -------------------------------------------------------------

# How many files do we have?
xtabs(~ region + year + species, data = urban_files)
xtabs(~ region + year + species, data = rural_files)

# Generate yearly counts of predictions - should be the same within each region
# / species / urban classification combination
invisible(pbapply(
  expand.grid(
    region = unique(urban_files$region),
    # region = "M8",
    species = unique(urban_files$species),
    urban_rural = c("rural", "urban")
  ),
  1,
  function(row) {
    output_file <- file.path(
      "diagnostics",
      sprintf("%s_%s_%s_counts.csv", row["region"], row["urban_rural"], row["species"])
    )
    if (row["urban_rural"] == "urban") {
      files <- urban_files
    } else {
      files <- rural_files
    }
    if (file.exists(output_file)) {
      message(sprintf("Skipping %s", output_file))
    } else {
      message(sprintf("Generating %s", output_file))
      write.table(
        table(
          rbindlist(pblapply(
            files[region == row["region"] & species == row["species"]]$path,
            function(path) {
              return(readRDS(path)[, c("year", "lon", "lat")])
            }
          ))$year
        ),
        output_file,
        row.names = FALSE
      )
    }
  }
))

# Load counts
all_counts <- pbsapply(
  Sys.glob("diagnostics/*_counts.csv"),
  function(path) {
    return(table(read.csv(path, sep = " ")$Freq))
  },
  simplify = FALSE
)

# Print which files have inconsistent counts between years
message("Files with inconsistent counts:")
message(paste(
  names(all_counts[sapply(all_counts, length) > 1]),
  collapse = "\n"
))

