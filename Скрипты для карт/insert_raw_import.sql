TRUNCATE vine_import;

########################################
#Наполняем таблицу данными из папки куда выгружаются данные из 1С
LOAD DATA LOCAL INFILE 'C:/SQL/Nomenklatura_roznica.txt'
INTO TABLE `testdb`.`raw_vine_import_2`
CHARACTER SET cp1251  #utf8mb4 
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

########################################
#Насыщаем данными `testdb`.`vine_import_2`
INSERT INTO `testdb`.`vine_import` (report_date, SKU, quantity_bottle, brand, restaurant, organozation, INN, col1, sales_channel, address, warehouse)
SELECT REPLACE(report_date,' ','') AS report_date,
       REPLACE(SKU,' ','') AS SKU,
       REPLACE(quantity_bottle,' ','') AS quantity_bottle,
       REPLACE(brand,' ','') AS brand,
       REPLACE(restaurant,' ','') AS restaurant,
       REPLACE(organozation,' ','') AS organozation,
       REPLACE(INN,' ','') AS INN,
       REPLACE(col1,' ','') AS col1,
       REPLACE(sales_channel,' ','') AS sales_channel,
       REPLACE(address,' ','') AS address,
       REPLACE(warehouse,' ','') AS warehouse
FROM `testdb`.`raw_vine_import_2`;

########################################
TRUNCATE raw_vine_import_2;