truncate table lnd.name_basics_l;

LOAD DATA INFILE "F:/imdb_data/name_basics.tsv" INTO TABLE lnd.name_basics_l COLUMNS TERMINATED BY "\t" IGNORE 1 LINES;

truncate  table stg.name_basics_s;

insert into stg.name_basics_s select * from lnd.name_basics_l;

truncate table lnd.movie_collection_2022_l;

LOAD DATA INFILE "F:\\imdb_data\\\2022.csv" INTO TABLE lnd.movie_collection_2022_l FIELDS OPTIONALLY ENCLOSED BY '\"' TERMINATED BY "\," IGNORE 1 LINES (ID,movie_name,worldwide_collection,domestic_collection,domestic_percentage,foreign_collection,foreign_percentage) SET release_year = "2022";


delete from stg.movie_collection_s where release_year='2022';


insert into stg.movie_collection_s
				select 
				( select 
				coalesce(Max(id),0)  as id from stg.movie_collection_s) + (ROW_NUMBER() OVER (ORDER BY id))   as id,
				movie_name,
				replace(replace(replace(worldwide_collection,'$',''),',',''),'-','0') as worldwide_collection,
				replace(replace(replace(domestic_collection,'$',''),',',''),'-','0') as domestic_collection,
				replace(replace(domestic_percentage,'%',''),'-','0') as domestic_percentage,
				replace(replace(replace(foreign_collection,'$',''),',',''),'-','0') as foreign_collection,
				replace(replace(foreign_percentage,'%',''),'-','0') as foreign_percentage,
				release_year
				from  lnd.movie_collection_2022_l;