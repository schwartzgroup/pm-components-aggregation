#!/usr/bin/env Rscript
#
# Determine which Census blocks and block groups are urban as follows:
# * For Census blocks: if the centroid is within an urban area
# * For Census block groups: if 75% or more of the population (determined via
#   Census blocks) are classified as urban
#
# We must additionally calculate if blocks and block groups are entirely
# encompassed within the prediction area, as this will affect later aggregation
# decisions. The prediction area is defined as a 1-km buffer around North
# American Equidistant Conic (ESRI:102010)-projected urban areas.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(dplyr)
library(parallel)
library(pbapply)
library(sf)
library(tools)

source("util.R")

# Setup -------------------------------------------------------------------

pboptions(type = "timer")

PCT_POP_URBAN_CUTOFF <- 0.75

# Functions ---------------------------------------------------------------

st_read_valid <- function(..., select_columns = NULL, centroids_only = FALSE) {
  sf <- st_read(...)
  if (length(subset) > 0) {
    sf <- subset(sf, select = select_columns)
  }
  sf_valid <- st_make_valid(sf)
  if (centroids_only) {
    sf <- st_centroid(sf)
  }
  return(sf)
}

# Load and pre-process geographies ----------------------------------------
# Correct any invalid geographies + join multi-file data sets (i.e. the blocks)
# into single-file GeoPackages

## Set up cluster ----

cl <- makeCluster(30)
clusterEvalQ(cl, library(sf))
clusterExport(cl, c("st_read_valid"))
on.exit(stopCluster(cl))

## Urban areas and buffer ----

urban_areas_path <- file.path(
  GEO_DIR, "urban_areas_2010_fixed.gpkg"
)
if (!file.exists(urban_areas_path)) {
  urban_areas_2010 <- st_read_valid(
    file.path(POLYGONS_DIR, "2010/uac/tl_2010_us_uac10.shp"),
    select_columns = c("UACE10", "NAME10")
  )
  st_write_gpkg(urban_areas_2010, urban_areas_path)
}

## 2020 blocks ----

blocks_2020_path <- file.path(GEO_DIR, "blocks_2020.gpkg")
if (!file.exists(blocks_2010_path)) {
  blocks_2020 <- bind_rows(pblapply(
    Sys.glob(file.path(POLYGONS_DIR, "2020/block/*.shp")),
    st_read_valid,
    select_columns = c("GEOID20"),
    cl = cl
  ))
  st_write_gpkg(blocks_2020, blocks_2020_path)
} else {
  blocks_2020 <- st_read(blocks_2020_path)
}

## 2010 blocks ----

blocks_2010_path <- file.path(GEO_DIR, "blocks_2010.gpkg")
if (!file.exists(blocks_2010_path)) {
  blocks_2010 <- bind_rows(pblapply(
    Sys.glob(file.path(POLYGONS_DIR, "2010/block/*.shp")),
    st_read_valid,
    select_columns = c("GEOID10"),
    cl = cl
  ))
  st_write_gpkg(blocks_2010, blocks_2010_path)
} else {
  blocks_2010 <- st_read(blocks_2010_path)
}

## 2000 blocks ----

blocks_2000_path <- file.path(GEO_DIR, "blocks_2000.gpkg")
if (!file.exists(blocks_2000_path)) {
  blocks_2000 <- bind_rows(pblapply(
    Sys.glob(file.path(POLYGONS_DIR, "2000/block/*.shp")),
    st_read_valid,
    select_columns = c("BLKIDFP00"),
    cl = cl
  ))
  st_write_gpkg(blocks_2000, blocks_2000_path)
} else {
  blocks_2000 <- st_read(blocks_2000_path)
}

## Stop cluster ----

stopCluster(cl)

# Load Decennial Census data ----------------------------------------------

census_2000_pops <- read.csv(file.path(DECENNIAL_DIR, "2000_block_sf1.csv.gz")) %>%
  transmute(
    population,
    BLKIDFP00 = sprintf("%015.f", as.numeric(GEOID))
  )

census_2010_pops <- read.csv(file.path(DECENNIAL_DIR, "2010_block_sf1.csv.gz")) %>%
  transmute(
    population,
    GEOID10 = sprintf("%015.f", as.numeric(GEOID))
  )

census_2020_pops <- read.csv(file.path(DECENNIAL_DIR, "2020_block_pl.csv.gz")) %>%
  transmute(
    population,
    GEOID20 = sprintf("%015.f", as.numeric(GEOID))
  )

# Calculate urban Census block subsets ------------------------------------

## QGIS instructions ----
#
# The point-in-polygon operation is very expensive and slow in R; deferred to
# QGIS instead. Process:
# 1. Reproject urban areas to ESRI:102010 (North American Equidistant Conic)
# 2. Buffer urban areas by 1 kilometer
# 3. Calculate blocks within 1-kilometer buffer of urban areas and save as
#    blocks_XXXX_urban_1km_buffer.gpkg
# 4. Calculate block centroids
# 5. Calculate block centroids within urban areas and save as
#    blocks_XXXX_centroids_urban.gpkg

## Load results ----

blocks_2000_fully_predicted <- st_read(file.path(
  GEO_DIR, "blocks_2000_urban_1km_buffer.gpkg"
))

blocks_2000_centroids_urban <- st_read(file.path(
  GEO_DIR, "blocks_2000_centroids_urban.gpkg"
))

blocks_2010_fully_predicted <- st_read(file.path(
  GEO_DIR, "blocks_2000_urban_1km_buffer.gpkg"
))

blocks_2010_centroids_urban <- st_read(file.path(
  GEO_DIR, "blocks_2010_centroids_urban.gpkg"
)) 

blocks_2020_fully_predicted <- st_read(file.path(
  GEO_DIR, "blocks_2000_urban_1km_buffer.gpkg"
))

blocks_2020_centroids_urban <- st_read(file.path(
  GEO_DIR, "blocks_2020_centroids_urban.gpkg"
)) 

# Build Census block classifications --------------------------------------

blocks_2000_classification <- data.frame(
  BLKIDFP00 = blocks_2000$BLKIDFP00
) %>%
  mutate(
    urban = as.numeric(BLKIDFP00 %in% blocks_2000_centroids_urban$BLKIDFP00),
    fully_predicted = as.numeric(BLKIDFP00 %in% blocks_2000_fully_predicted$BLKIDFP00)
  ) %>%
  left_join(census_2000_pops, by = "BLKIDFP00")

blocks_2010_classification <- data.frame(
  GEOID10 = blocks_2010$GEOID10
) %>%
  mutate(
    urban = as.numeric(GEOID10 %in% blocks_2010_centroids_urban$GEOID10),
    fully_predicted = as.numeric(GEOID10 %in% blocks_2010_fully_predicted$GEOID10)
  ) %>%
  left_join(census_2010_pops, by = "GEOID10")

blocks_2020_classification <- data.frame(
  GEOID20 = blocks_2020$GEOID20
) %>%
  mutate(
    urban = as.numeric(GEOID20 %in% blocks_2020_centroids_urban$GEOID20),
    fully_predicted = as.numeric(GEOID20 %in% blocks_2020_fully_predicted$GEOID20)
  ) %>%
  left_join(census_2020_pops, by = "GEOID20")

## DEPRECATED: Missing Census block population ----
# <DEPRECATED - NEW CENSUS EXPORT CODE HAS NO MISSING VALUES IN THE STATES OF
# INTEREST>
#
# The following state and territory GEOIDs are missing in NHGIS:
# * American Samoa (60)
# * Guam (66)
# * Northern Mariana Islands (69)
# * Virgin Islands (78)
#
# In the 2010 NHGIS Census block-level data, these are the only missing GEOIDs,
# so we are OK.
#
# In the 2000 NHGIS Census block-level data, there are additional missing GEOIDs
# with state and territory GEOIDs less than 60 (540 total blocks). These are all
# 0 and can be safely replaced as such (see below for proof).

missing_2020 <- blocks_2020_classification %>%
  filter(is.na(population)) %>%
  pull(GEOID20)

missing_2020 %>%
  substr(1, 2) %>%
  .[. %in% unlist(FIPS_BY_REGION)] %>%
  table()

missing_2010 <- blocks_2010_classification %>%
  filter(is.na(population)) %>%
  pull(GEOID10)

missing_2010 %>%
  substr(1, 2) %>%
  .[. %in% unlist(FIPS_BY_REGION)] %>%
  table()

missing_2000 <- blocks_2000_classification %>%
  filter(is.na(population)) %>%
  pull(BLKIDFP00)

missing_2000 %>%
  substr(1, 2) %>%
  .[. %in% unlist(FIPS_BY_REGION)] %>%
  table()

# (missing_2000_table <- missing_2000 %>%
#   substr(1, 2) %>%
#   .[. %in% unlist(FIPS_BY_REGION)] %>%
#   table() %>%
#   addmargins())

### DEPRECATED: Validation via Census API ----
# <DEPRECATED - NEW CENSUS EXPORT CODE HAS NO MISSING VALUES IN THE STATES OF
# INTEREST>
#
# Retrieve data directly from the Census API using `tidycensus` to verify
# that population of missing GEOIDs == 0

# library(tidycensus)
# 
# census_api_key("<<ENTER CENSUS API KEY HERE>>")
# 
# tidycensus_2000 <- bind_rows(pblapply(
#   setdiff(names(missing_2000_table), "Sum"),
#   function(statefp) {
#     get_decennial("block", "P001001", year = 2000, state = statefp)
#   }
# ))
# 
# data.frame(GEOID = missing_2000) %>%
#   left_join(tidycensus_2000) %>%
#   filter(!is.na(value)) %>%
#   pull(value) %>%
#   table()

### DEPRECATED: Correct missing population ----
# <DEPRECATED - NEW CENSUS EXPORT CODE HAS NO MISSING VALUES IN THE STATES OF
# INTEREST>
#
# * For state and territory GEOIDs above 60: drop the records
# * Otherwise: fill NA with 0

# blocks_2010_classification <- blocks_2010_classification %>%
#   filter(!is.na(population))
# 
# blocks_2000_classification <- blocks_2000_classification %>%
#   filter(substr(BLKIDFP00, 1, 2) < "60") %>%
#   mutate(population = ifelse(is.na(population), 0, population))

### Write out ----

write.csv(blocks_2000_classification, gzfile(file.path(CLASSIFICATIONS_DIR, "blocks_2000_2.csv.gz")), row.names = FALSE)
write.csv(blocks_2010_classification, gzfile(file.path(CLASSIFICATIONS_DIR, "blocks_2010.csv.gz")), row.names = FALSE)
write.csv(blocks_2020_classification, gzfile(file.path(CLASSIFICATIONS_DIR, "blocks_2020.csv.gz")), row.names = FALSE)

# Verify that summed Census block populations agree with block group populations
# 
# block_groups_2000_classification <- blocks_2000_classification %>%
#   mutate(
#     BKGPIDFP00 = substr(BLKIDFP00, 1, 12), # Convert to Census block group GEOID
#     population_urban = population * urban
#   ) %>%
#   group_by(BKGPIDFP00) %>%
#   summarise(
#     population = sum(population),
#     population_urban = sum(population_urban),
#     pct_population_urban = sum(population_urban) / sum(population),
#     fully_predicted = as.numeric(all(fully_predicted == 1))
#   ) %>%
#   mutate(urban = as.numeric(pct_population_urban > PCT_POP_URBAN_CUTOFF))
# 
# block_groups_2010_classification <- blocks_2010_classification %>%
#   mutate(
#     GEOID10 = substr(GEOID10, 1, 12), # Convert to Census block group GEOID
#     population_urban = population * urban
#   ) %>%
#   group_by(GEOID10) %>%
#   summarise(
#     population = sum(population),
#     population_urban = sum(population_urban),
#     pct_population_urban = sum(population_urban) / sum(population),
#     fully_predicted = as.numeric(all(fully_predicted == 1))
#   ) %>%
#   mutate(urban = as.numeric(pct_population_urban > 0.75))
# 
# read.csv(file.path(DECENNIAL_DIR, "2010_blck_grp.csv.gz")) %>%
#   transmute(
#     population_nhgis = population,
#     GEOID10 = sprintf("%012.f", as.numeric(GEOID))
#   ) %>%
#   left_join(block_groups_2010_classification, by = "GEOID10") %>%
#   transmute(agree = population == population_nhgis) %>%
#   pull(agree) %>%
#   table()
# 
# read.csv(file.path(DECENNIAL_DIR, "2000_blck_grp.csv.gz")) %>%
#   transmute(
#     population_nhgis = population,
#     BLKIDFP00 = sprintf("%012.f", as.numeric(GEOID))
#   ) %>%
#   left_join(block_groups_2000_classification, by = "BLKIDFP00") %>%
#   transmute(agree = population == population_nhgis) %>%
#   pull(agree) %>%
#   table()
# 
# addmargins(
#   table(blocks_2000_classification$urban) / nrow(blocks_2000_classification) * 100
# )
# addmargins(
#   table(blocks_2010_classification$urban) / nrow(blocks_2010_classification) * 100
# )

# Parent geography classifications ----------------------------------------

generate_classifications <- function(geography, # "block_groups" / "tracts" / "counties'
                                     year # 2000 / 2010
                                     ) {
  geoid_length <- GEOID_LENGTHS[[geography]]
  if (year == 2000) {
    classification <- blocks_2000_classification
    block_geoid_column <- "BLKIDFP00"
    geoid_column <- GEOID_NAMES_2000[[geography]]
  } else if (year == 2010) {
    classification <- blocks_2010_classification
    block_geoid_column <- "GEOID10"
    geoid_column <- "GEOID10"
  } else {
    classification <- blocks_2020_classification
    block_geoid_column <- "GEOID20"
    geoid_column <- "GEOID20"
  }
  
  classification %>%
    mutate(
      # Truncate GEOID - see:
      # https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html
      !!geoid_column := substr(get(block_geoid_column), 1, geoid_length),
      population_urban = population * urban
    ) %>%
    group_by(across(geoid_column)) %>%
    summarise(
      population = sum(population),
      population_urban = sum(population_urban),
      pct_population_urban = sum(population_urban) / sum(population),
      fully_predicted = as.numeric(all(fully_predicted == 1))
    ) %>%
    mutate(urban = as.numeric(pct_population_urban > PCT_POP_URBAN_CUTOFF))
}

invisible(pbapply(
  expand.grid(
    geography = setdiff(unique(names(GEOID_LENGTHS)), "blocks"),
    year = c(2000, 2010, 2020)
  ),
  1,
  function(row) {
    output_file <- file.path(
      CLASSIFICATIONS_DIR,
      sprintf("%s_%s.csv.gz", row["geography"], row["year"])
    )
    if (file.exists(output_file)) {
      message(sprintf("Skipping: %s", output_file))
    } else {
      message(sprintf("Generating: %s", output_file))
      write.csv(
        generate_classifications(row["geography"], row["year"]),
        gzfile(output_file),
        row.names = FALSE
      )
    }
  }
))
