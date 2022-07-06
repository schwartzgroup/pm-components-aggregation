#!/usr/bin/env bash
#
# Set up a working GeoPackage database for use with QGIS in creating subsets.
# It's must faster to do these operations by directly running spatial queries
# against the GeoPackage and creating new tables vs calculating centroids or
# subsets directly in R / Python / QGIS.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

# Input directories
GEO_DIR="$(dirname "$0")/output/geo"
GRIDS_DIR="$(dirname "$0")/output/grids"

CROSSWALKS_DIR="$(dirname "$0")/output/crosswalks"
# Need to write to home then move to the current directory since SMB mounts
# don't play well with GeoPackages
WORKING_GPKG_TEMP=/home/edgar/working.gpkg
WORKING_GPKG_FINAL="$(dirname "$0")/output/working.gpkg"

REGIONS=(
    NE1  "'09', '23', '25', '33', '44', '50'"
    MA2  "'34', '36', '42'"
    NEC3 "'17', '18', '26', '39', '55'"
    WNC4 "'19', '20', '27', '29', '31', '38', '46'"
    SA5  "'10', '11', '12', '13', '24', '45', '37', '51', '54'"
    ESC6 "'01', '21', '28', '47'"
    WSC7 "'05', '22', '40', '48'"
    M8   "'04', '08', '16', '30', '32', '35', '49', '56'"
    P9   "'06', '41', '53'"
)

n_regions=${#REGIONS[@]}

mkdir -p "$CROSSWALKS_DIR"

# Given a GeoPackage layer of all Census blocks in the U.S., create subsets of
# the blocks in different tables according to region
function create_regional_subsets() {
    blocks_gpkg="$1"  # e.g. blocks_2000.gpkg / blocks_2010.gpkg / blocks_2020.gpkg
    blocks_layer="$2" # e.g. blocks_2000 / blocks_2010 / blocks_2020
    geoid_field="$3"  # e.g. BLKIDFP00 / GEOID10 / GEOID20
    for i in $(seq 1 $[n_regions / 2])
    do
        region=${REGIONS[($i - 1) * 2]}
        fips_codes=${REGIONS[($i - 1) * 2 + 1]}
        output_table=${blocks_layer}_${region}
        sql_code="SELECT *
                  FROM ${blocks_layer}
                  WHERE substr(${geoid_field}, 1, 2) IN (${fips_codes})"
        echo -e "Creating table ${output_table} using:"
        sed "s/^[ \t]*/> /" <<< "$sql_code"
        ogr2ogr -f GPKG "$blocks_gpkg" "$blocks_gpkg" \
            -nln "$output_table" \
            -update \
            -sql "$sql_code"
    done
}

# Given a GeoPackage layer of all Census blocks in the U.S., create subsets of
# *centroids* of the blocks in different tables according to region
function create_regional_centroids() {
    blocks_gpkg="$1"  # e.g. blocks_2000.gpkg / blocks_2010.gpkg / blocks_2020.gpkg
    blocks_layer="$2" # e.g. blocks_2000 / blocks_2010 / blocks_2020
    geoid_field="$3"  # e.g. BLKIDFP00 / GEOID10 / GEOID20
    for i in $(seq 1 $[n_regions / 2])
    do
        region=${REGIONS[($i - 1) * 2]}
        fips_codes=${REGIONS[($i - 1) * 2 + 1]}
        output_table=${blocks_layer}_centroids_${region}
        sql_code="SELECT ${geoid_field}, ST_Centroid(geom)
                  FROM ${blocks_layer}
                  WHERE substr(${geoid_field}, 1, 2) IN (${fips_codes})"
        echo -e "Creating table ${output_table} using:"
        sed "s/^[ \t]*/> /" <<< "$sql_code"
        ogr2ogr -f GPKG "$blocks_gpkg" "$blocks_gpkg" \
            -nln "$output_table" \
            -update \
            -sql "$sql_code"
    done
}

# DEPRECATED - use QGIS or PostGIS instead (much faster)
# Point-in-polygon spatial join of grid cells within Census blocks across the
# entire U.S.
function generate_crosswalks_within() {
    working_gpkg="$1" # e.g. working.gpkg
    blocks_layer="$2" # e.g. blocks_2000 / blocks_2010 / blocks_2020
    geoid_field="$3"  # e.g. BLKIDFP00 / GEOID10 / GEOID20
    output_dir="$4"   # e.g. "." / crosswalks
    mkdir -p "$output_dir"
    for i in $(seq 1 $[n_regions / 2])
    do
        region=${REGIONS[($i - 1) * 2]}
        fips_codes=${REGIONS[($i - 1) * 2 + 1]}
        region_layer=${region}_urban
        output_file="${output_dir}/${region}_within_${blocks_layer}.csv"
        output_table=${blocks_layer}_centroids_${region}
        sql_code="SELECT ${region_layer}.gridcell_idx, ${blocks_layer}.${geoid_field}
                  FROM ${region_layer}
                  LEFT JOIN ${blocks_layer}
                      ON ST_Within(${region_layer}.geom, ${blocks_layer}.geom)
                  WHERE substr(${blocks_layer}.BLKIDFP00, 1, 2) IN (${fips_codes})"
        echo -e "Creating file ${output_file} using:"
        sed "s/^[ \t]*/> /" <<< "$sql_code"
        ogr2ogr -f CSV "$output_file" "$working_gpkg" -sql "$sql_code"
    done
}

# DEPRECATED - use QGIS or PostGIS instead (much faster)
# Point-in-polygon spatial join of grid cells within Census blocks across the
# U.S., using the regional subsets created by `create_regional_subsets()`
function generate_crosswalks_within_subsets() {
    working_gpkg="$1" # e.g. working.gpkg
    blocks_layer_prefix="$2" # e.g. blocks_2000 / blocks_2010 / blocks_2020
    geoid_field="$3"  # e.g. BLKIDFP00 / GEOID10 / GEOID20
    output_dir="$4"   # e.g. "." / crosswalks
    mkdir -p "$output_dir"
    for i in $(seq 1 $[n_regions / 2])
    do
        region=${REGIONS[($i - 1) * 2]}
        blocks_layer="${blocks_layer_prefix}_${region}"
        fips_codes=${REGIONS[($i - 1) * 2 + 1]}
        region_layer=${region}_urban
        output_file="${output_dir}/${region}_within_${blocks_layer}.csv"
        output_table=${blocks_layer}_centroids_${region}
        sql_code="SELECT ${region_layer}.gridcell_idx, ${blocks_layer}.${geoid_field}
                  FROM ${region_layer}
                  LEFT JOIN ${blocks_layer}
                      ON ST_Within(${region_layer}.geom, ${blocks_layer}.geom)"
        echo -e "Creating file ${output_file} using:"
        sed "s/^[ \t]*/> /" <<< "$sql_code"
        ogr2ogr -f CSV "$output_file" "$working_gpkg" -sql "$sql_code"
    done
}

# Import layers into the working GeoPackage
#find "$GRIDS_DIR"/* "$GEO_DIR"/blocks_{2000,2010,2020}.gpkg |
echo "Importing layers"
find "$GEO_DIR"/blocks_{2000,2010,2020}.gpkg |
    sort |
    while read gpkg
    do
        echo "$gpkg"
        layer_name=$(basename $gpkg .gpkg)
        echo "importing ${gpkg}::${layer_name} => ${WORKING_GPKG_TEMP}"
        if [ -f "$WORKING_GPKG_TEMP" ]
        then
            ogr2ogr -f GPKG "$WORKING_GPKG_TEMP" "$gpkg" -nln "$layer_name" -update
        else
            ogr2ogr -f GPKG "$WORKING_GPKG_TEMP" "$gpkg" -nln "$layer_name"
        fi
    done

# Create Census block subsets
echo "Creating Census block subsets"
create_regional_subsets "$WORKING_GPKG_TEMP" blocks_2000 BLKIDFP00
create_regional_subsets "$WORKING_GPKG_TEMP" blocks_2010 GEOID10
create_regional_subsets "$WORKING_GPKG_TEMP" blocks_2020 GEOID20

# Create Census block centroid subsets
echo "Creating Census block centroid subsets"
create_regional_centroids "$WORKING_GPKG_TEMP" blocks_2000 BLKIDFP00
create_regional_centroids "$WORKING_GPKG_TEMP" blocks_2010 GEOID10
create_regional_centroids "$WORKING_GPKG_TEMP" blocks_2020 GEOID20

# Drop the non-subsetted data
ogrinfo "$WORKING_GPKG_TEMP" -sql "DROP TABLE blocks_2000"
ogrinfo "$WORKING_GPKG_TEMP" -sql "DROP TABLE blocks_2010"
ogrinfo "$WORKING_GPKG_TEMP" -sql "DROP TABLE blocks_2020"
ogrinfo "$WORKING_GPKG_TEMP" -sql "VACUUM"

# DEPRECATED - use QGIS or PostGIS instead (much faster)
# generate_crosswalks_within "$WORKING_GPKG_TEMP" blocks_2000 BLKIDFP00 "$CROSSWALKS_DIR"
# generate_crosswalks_within "$WORKING_GPKG_TEMP" blocks_2010 GEOID10 "$CROSSWALKS_DIR"
# generate_crosswalks_within "$WORKING_GPKG_TEMP" blocks_2020 GEOID20 "$CROSSWALKS_DIR"

# DEPRECATED - use QGIS or PostGIS instead (much faster)
# generate_crosswalks_within_subsets "$WORKING_GPKG_TEMP" blocks_2000 BLKIDFP00 "$CROSSWALKS_DIR"
# generate_crosswalks_within_subsets "$WORKING_GPKG_TEMP" blocks_2010 GEOID10 "$CROSSWALKS_DIR"
# generate_crosswalks_within_subsets "$WORKING_GPKG_TEMP" blocks_2020 GEOID20 "$CROSSWALKS_DIR"

# Finished - move the GeoPackage to the project directory
echo "Moving GeoPackage"
mv -v "$WORKING_GPKG_TEMP" "$WORKING_GPKG_FINAL"