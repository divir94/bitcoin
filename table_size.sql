SELECT table_schema "gdax", 
Round(Sum(data_length + index_length) / 1024 / 1024, 1) "DB Size in MB" 
FROM   information_schema.tables 
GROUP  BY table_schema; 