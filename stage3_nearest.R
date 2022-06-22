#!/usr/bin/env Rscript
#
# Create nearest-neighbour crosswalks between Census blocks and grid cells
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(nabor)
library(pbapply)
library(sf)

source("util.R")

# Setup -------------------------------------------------------------------

pboptions(type = "timer")

# Create crosswalks -------------------------------------------------------

create_crosswalk <- function(blocks_gpkg, blocks_layer,
                             grid_gpkg, grid_layer,
                             is_centroids = TRUE) {
  message(sprintf(
    "Creating knn crosswalk from %s$%s -> %s$%s (k=1)",
    blocks_gpkg, blocks_layer, grid_gpkg, grid_layer
  ))
  
  if (is_centroids) {
    # Load block centroids
    message(sprintf("> Loading block centroids from %s$%s", blocks_gpkg, blocks_layer))
    block_centroids <- st_read(blocks_gpkg, blocks_layer)
  } else {
    # Load blocks
    message(sprintf("> Loading block polygons from %s$%s", blocks_gpkg, blocks_layer))
    blocks <- st_read(blocks_gpkg, blocks_layer)
    
    # Calculate block centroids
    message("> Calculating block centroids")
    block_centroids <- st_coordinates(st_centroid(blocks))
  }
  geoid_column <- names(block_centroids)[1]
  
  # Load grid
  message(sprintf("> Loading grid from %s$%s", grid_gpkg, grid_layer))
  grid <- st_read(WORKING_GPKG, grid_layer)
  
  # Join
  message("> Performing nearest-neighbor join")
  join <- data.frame(knn(
    st_coordinates(grid),
    st_coordinates(block_centroids),
    k = 1
  ))
  names(join) <- c("gridcell_idx", "distance")
  join[[geoid_column]] <- block_centroids[[geoid_column]]
  join <- join[, c(geoid_column, "gridcell_idx", "distance")]
  return(join)
}

pbapply(
  expand.grid(
    region = names(FIPS_BY_REGION),
    urban = c("rural", "urban"),
    blocks = c("blocks_2000", "blocks_2010")
  ),
  1,
  function(row) {
    region <- row[1]
    urban <- row[2]
    blocks <- row[3]
    output_file <- file.path(
      CROSSWALKS_DIR,
      sprintf(
        "%s_%s_nearest_%s.csv.gz",
        row["region"], row["urban"], row["blocks"]
      )
    )
    if (file.exists(output_file)) {
      message(sprintf("Skipping: %s", output_file))
    } else {
      crosswalk <- create_crosswalk(
        WORKING_GPKG,
        sprintf(
          "%s_centroids_%s", # e.g. blocks_2000_centroids_NE1
          row["blocks"], row["region"]
        ),
        WORKING_GPKG,
        sprintf(
          "%s_%s", # e.g. NE1_urban
          row["region"], row["urban"]
        )
      )
      write.csv(crosswalk, gzfile(output_file), row.names = FALSE)
    }
  }
)
