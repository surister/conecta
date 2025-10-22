select
    point_ as point,
    ST_AsText(point_) as point_text,

    linestring_ as linestring,
    ST_AsText(linestring_) as linestring_text,

    polygon_ as polygon,
    ST_AsText(polygon_) as polygon_text,

    polygon_holed,
    ST_AsText(polygon_holed) as polygon_holed_text,

    geom_collection,
    ST_AsText(geom_collection) as geom_collection_text

from pgis_datatypes