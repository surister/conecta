CREATE TABLE pgis_datatypes (
    point_ geometry(Point, 4326),
    linestring_ geometry(LineString, 4326),
    polygon_ geometry(Polygon, 4326),
    polygon_holed geometry(Polygon, 4326),
    geom_collection geometry(GeometryCollection, 4326)
);
