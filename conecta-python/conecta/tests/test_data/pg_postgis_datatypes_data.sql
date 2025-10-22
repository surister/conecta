INSERT INTO pgis_datatypes (
    point_,
    linestring_,
    polygon_,
    polygon_holed,
    geom_collection
)
VALUES (
    ST_GeomFromText('POINT (99 777)', 4326),
    ST_GeomFromText('LINESTRING (0 0, 1 1, 2 1, 2 2)', 4326),
    ST_GeomFromText('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326),
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))', 4326),
    ST_GeomFromText('GEOMETRYCOLLECTION (POINT (2 0), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)))', 4326)
);
