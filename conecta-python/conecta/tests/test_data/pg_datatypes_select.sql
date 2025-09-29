select id,
       small_int,
       int_,
       big_int,
       -- decimal, ON PURPOSE
       -- numeric_, ON PURPOSE
       double_,
       varchar_,
       text_,
       bool_,
       uuid_,

       date_,
       time_,
       --timetz_,

       timestamp_,

       --arrays
       int_array,
       smallint_array,
       bigint_array,
       real_array,
       double_array,
       text_array,
       uuid_array,
       bool_array

-- Geo
-- point_
from pg_datatypes