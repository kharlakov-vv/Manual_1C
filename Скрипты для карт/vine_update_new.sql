DROP table `testdb`.`raw_vine_target`;
DROP table `testdb`.`vine_target_all`;
DROP table `testdb`.`vine_import_1`;
DROP TABLE `testdb`.`raw_vine_import_2`;

truncate `testdb`.`raw_vine_import_2`;
truncate `testdb`.`raw_vine_target`;
truncate `testdb`.`vine_target_all`;
truncate `testdb`.`vine_import_1`;

#Создаем таблицу для выгрузки данных продаж из 1С
CREATE TABLE IF NOT EXISTS `testdb`.`raw_vine_import_2` (
  `region` text COMMENT 'Регион',
  `report_date` text COMMENT 'Отчетный период',
  `SKU` text DEFAULT NULL COMMENT 'Название вина от ЕГАИС',
  `quantity_bottle` text COMMENT 'Кол-во поставок, шт',
  `brand` text DEFAULT NULL COMMENT 'Бренд',
  `restaurant` text DEFAULT NULL COMMENT 'Ресторан',
  `organozation` text DEFAULT NULL COMMENT 'Организация',
  `INN` double DEFAULT NULL COMMENT 'ИНН',
  `col1` text DEFAULT NULL COMMENT 'Склад отгрузки',
  `sales_channel` text DEFAULT NULL COMMENT 'Канал сбыла',
  `address` text DEFAULT NULL COMMENT 'Адресс',
  `target` text DEFAULT NULL COMMENT 'Целевой',
  `warehouse` text DEFAULT NULL COMMENT 'Склад'
) ENGINE=InnoDB DEFAULT CHARSET=cp1251 COMMENT='Таблица AS IS';

#Начальная таблица для залива данных из 1С
CREATE TABLE IF NOT EXISTS `testdb`.`vine_import_1` (
  `region` varchar(15) DEFAULT NULL COMMENT 'Регион',
  `report_date` date DEFAULT NULL COMMENT 'Отчетный дата',
  `period` varchar(20) DEFAULT NULL COMMENT 'Период',
  `SKU` varchar(150) DEFAULT NULL COMMENT 'Полное наименование',
  `quantity_bottle` float DEFAULT NULL COMMENT 'Кол-во поставок, шт',
  `brand` varchar(100) DEFAULT NULL COMMENT 'Бренд',
  `restaurant` varchar(100) DEFAULT NULL COMMENT 'Ресторан',
  `organozation` varchar(150) DEFAULT NULL COMMENT 'Организация',
  `INN` double DEFAULT NULL COMMENT 'ИНН',
  `col1` varchar(150) DEFAULT NULL COMMENT 'Склад отгрузки',
  `sales_channel` varchar(45) DEFAULT NULL COMMENT 'Канал сбыла',
  `address` varchar(500) DEFAULT NULL COMMENT 'Адресс',
  `street` varchar(500) DEFAULT NULL COMMENT 'Улица',
  `target` varchar(20) DEFAULT NULL COMMENT 'Целевой/Нецелевой',
  `warehouse` varchar(45) DEFAULT NULL COMMENT 'Склад',
  `geo` varchar(25) DEFAULT NULL COMMENT 'Гео точка',
  `type_target` varchar(25) DEFAULT NULL COMMENT 'Тип целёвости',
  `quantity_bottle_G` varchar(25) DEFAULT NULL COMMENT 'Тип целёвости',
  `quantity_bottle_C` varchar(25) DEFAULT NULL COMMENT 'Тип целёвости',
  `id` int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=65536 DEFAULT CHARSET=cp1251 COMMENT='Таблица для импорта новых данных';

#Таблица raw для target
CREATE TABLE IF NOT EXISTS `testdb`.`raw_vine_target`(
    `restaurant` text DEFAULT NULL COMMENT 'Ресторан',
    `region` text DEFAULT NULL COMMENT 'Город',
    `street` text DEFAULT NULL COMMENT 'Улица',
    `brief_street` text DEFAULT NULL COMMENT 'Краткое обозначение улицы',
    `number_street` text DEFAULT NULL COMMENT 'Номер дома',
    `organozation` text DEFAULT NULL COMMENT 'Организация',
    `target` text DEFAULT NULL COMMENT 'Целевой/Нецелевой',
    `INN` bigint DEFAULT NULL COMMENT 'ИНН'
) ENGINE=InnoDB DEFAULT CHARSET=cp1251 COMMENT='Таблица справочник для Целевых ресторанов';

#Таблица target
CREATE TABLE IF NOT EXISTS `testdb`.`vine_target_all`(
    `restaurant` varchar(100) NOT NULL COMMENT 'Ресторан',
    `region` varchar(20) NOT NULL COMMENT 'Город',
    `address` varchar(300) NOT NULL COMMENT 'Улица',
    `organozation` varchar(150) DEFAULT NULL COMMENT 'Организация',
    `INN` bigint DEFAULT NULL COMMENT 'ИНН',
PRIMARY KEY (restaurant, region, address)) ENGINE=InnoDB DEFAULT CHARSET=cp1251 COMMENT='Таблица справочник для Целевых ресторанов';

#Скрипт на выгрузку продаж из 1С
LOAD DATA LOCAL INFILE 'C:/SQL/New_with_target.txt'
INTO TABLE `testdb`.`raw_vine_import_2`
CHARACTER SET cp1251  #utf8mb4 
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 4 ROWS;

#Скрипт на выгрузку справочника из 1С
LOAD DATA LOCAL INFILE 'C:/SQL/For_target.txt'
INTO TABLE `testdb`.`raw_vine_target`
CHARACTER SET cp1251  #utf8mb4 
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n' #\r
IGNORE 4 ROWS;

#Наполнение данными справочника из raw в справочник
INSERT INTO `testdb`.`vine_target_all` (restaurant, region, address, organozation, INN)
SELECT DISTINCT
       REPLACE(CONVERT(restaurant using cp1251),' ','') AS restaurant,
       REPLACE(REPLACE(CONVERT(region using cp1251),' ',''),'-',' ') AS region, 
       TRIM(concat(REPLACE(CONVERT(street using cp1251),' ',''),' ',REPLACE(brief_street,' ',''), ', ', REPLACE(number_street,' ',''))) AS address,
       REPLACE(CONVERT(organozation using cp1251),' ','') AS organozation,  
       trim(REPLACE(INN,' ','')) AS INN
FROM testdb.raw_vine_target
WHERE target = 'Да';

truncate `testdb`.`vine_import_1`;
#Наполнение данными продаж из raw в import 
INSERT INTO `testdb`.`vine_import_1` (region,report_date,SKU,quantity_bottle,brand,restaurant,organozation,INN,col1,sales_channel,address,target,warehouse,GEO)
SELECT 
       CONVERT(REPLACE(rvi.region,' ','') using cp1251) AS region, 
       CONVERT(DATE_FORMAT(STR_TO_DATE(REPLACE(report_date,' ',''), '%d.%m.%Y %H:%i:%s'), '%Y-%m-%d') using cp1251) AS report_date,
       CONVERT(REPLACE(REPLACE(SUBSTRING_INDEX(sku, ' ', LENGTH(sku) - LENGTH(REPLACE(sku, ' ', '')) - 1), 'ГАЛИЦКИЙ И ГАЛИЦКИЙ', 'ГАЛИЦКИЙ'),' ','') using cp1251) AS SKU, #по просьбе Романа, оставить только Галицкий и убрать объем бутылки
       REPLACE(quantity_bottle,' ','') AS quantity_bottle,  
       CONVERT(REPLACE(brand,' ','') using cp1251) AS brand,  
       CONVERT(REPLACE(restaurant,' ','') using cp1251) AS restaurant, 
       CONVERT(REPLACE(organozation,' ','') using cp1251) AS organozation, 
       trim(REPLACE(rvi.INN,' ','')) AS INN,  
       CONVERT(REPLACE(col1,' ','') using cp1251) AS col1,  
       CONVERT(REPLACE(sales_channel,' ','') using cp1251) AS sales_channel,  
       CASE WHEN rvi.address LIKE '%Арбатская пл%' #по просьбе Романа привести к единому виду тк адреса два
            THEN CONVERT('Москва г, Арбатская пл, 14' using cp1251)
            ELSE CONVERT(REPLACE(rvi.address,' ','') using cp1251) END AS address,  
       CONVERT('Нецелевой' using cp1251) AS target, 
       CONVERT(REPLACE(warehouse,' ','') using cp1251) AS warehouse,
       CONVERT(vg.geo using cp1251) AS GEO
FROM testdb.raw_vine_import_2 rvi
                            LEFT JOIN vine_geo vg ON (REPLACE(rvi.address,' ','') = vg.address AND 
                                                      REPLACE(rvi.INN,' ','') = vg.INN) 
WHERE REPLACE(rvi.region,' ','') <> 'Вся страна' AND 
      REPLACE(organozation,' ','') NOT LIKE '%симпл%' AND 
      CASE WHEN REPLACE(rvi.region,' ','') LIKE '%Санкт%' AND REPLACE(warehouse,' ','') like '%Санкт%' THEN 1 
            WHEN REPLACE(rvi.region,' ','') LIKE '%Москва%' AND REPLACE(warehouse,' ','') LIKE '%Москва%' THEN 1
            ELSE 0 END = 1;
     
#Нужно проставить бренду сикоры названия ресторанов
update `testdb`.`vine_import_1`
left join (SELECT DISTINCT #Уникальные целевые рестораны из таблицы Романа 
                      restaurant, 
                      address, 
                      organozation,
                      INN
           FROM `testdb`.`vine_target_all`) target_table
on (`vine_import_1`.`INN` = `target_table`.`INN` 
                     AND trim(SUBSTRING(`vine_import_1`.`address`, INSTR(`vine_import_1`.`address`, ',') + 1)) = `target_table`.`address`)
set `vine_import_1`.`restaurant` = `target_table`.`restaurant`
WHERE LENGTH (`vine_import_1`.`restaurant`) < 1;

#Добавляем 0 значения продаж по магазинам к таблице продаж
INSERT INTO `testdb`.`vine_import_1` (region,report_date,SKU,quantity_bottle,brand,restaurant,organozation,INN,col1,sales_channel,address,target,warehouse,GEO)
WITH 
matrix_table AS (SELECT q.report_date, vta.restaurant,vta.region,vta.address,vta.organozation,vta.INN
                 FROM `testdb`.`vine_target_all` vta, (SELECT DISTINCT report_date
                                                       FROM testdb.vine_import_1) q)
#нулевые значения по Сикоре
SELECT DISTINCT matrix_table.region, matrix_table.report_date, CONVERT('' using cp1251) AS SKU, 0 AS quantity_bottle, CONVERT('СИКОРЫ' using cp1251) as brand, matrix_table.restaurant, matrix_table.organozation, matrix_table.INN, atribut.col1, atribut.sales_channel, IFNULL(atribut.address, concat(matrix_table.region,', ',matrix_table.address)),atribut.target,atribut.warehouse,ifnull(atribut.geo,vine_geo.geo)
#таблица матрица для нулевых значений
FROM matrix_table LEFT JOIN (SELECT DISTINCT report_date,restaurant,organozation,INN,address,brand,region FROM `testdb`.`vine_import_1` WHERE brand = 'СИКОРЫ') tvi1
                  ON (tvi1.restaurant = matrix_table.restaurant AND tvi1.report_date = matrix_table.report_date AND tvi1.INN = matrix_table.INN AND matrix_table.address= trim(SUBSTRING(tvi1.address, INSTR(tvi1.address, ',') + 1)))
                  #для вывода недостающих полей
                  LEFT JOIN (SELECT DISTINCT restaurant,organozation,INN,address,region,brand,col1,sales_channel,target,warehouse,geo FROM `testdb`.`vine_import_1`) atribut
                  ON (matrix_table.restaurant = atribut.restaurant AND matrix_table.organozation = atribut.organozation)
                  #для проставления пустым значениям Гео
                  LEFT JOIN (SELECT INN,address,geo,region,street FROM `testdb`.`vine_geo`) AS vine_geo
                  ON (matrix_table.address = vine_geo.street AND matrix_table.INN = vine_geo.INN)
WHERE tvi1.report_date is NULL
UNION  
#нулевые значения по Галицкому
SELECT DISTINCT matrix_table.region, matrix_table.report_date, CONVERT('' using cp1251) AS SKU, 0 AS quantity_bottle, CONVERT('ГАЛИЦКИЙ И ГАЛИЦКИЙ' using cp1251) as brand, matrix_table.restaurant, matrix_table.organozation, matrix_table.INN, atribut.col1, atribut.sales_channel, IFNULL(atribut.address, concat(matrix_table.region,', ',matrix_table.address)),atribut.target,atribut.warehouse,ifnull(atribut.geo,vine_geo.geo)
FROM matrix_table LEFT JOIN (SELECT DISTINCT report_date,restaurant,organozation,INN,address,brand,region FROM `testdb`.`vine_import_1` WHERE brand = 'ГАЛИЦКИЙ И ГАЛИЦКИЙ') tvi1
                  ON (tvi1.restaurant = matrix_table.restaurant AND tvi1.report_date = matrix_table.report_date AND tvi1.INN = matrix_table.INN AND matrix_table.address= trim(SUBSTRING(tvi1.address, INSTR(tvi1.address, ',') + 1)))
                  #для вывода недостающих полей
                  LEFT JOIN (SELECT DISTINCT restaurant,organozation,INN,address,region,brand,col1,sales_channel,target,warehouse,geo FROM `testdb`.`vine_import_1`) atribut
                  ON (matrix_table.restaurant = atribut.restaurant AND matrix_table.organozation = atribut.organozation)
                  #для проставления пустым значениям Гео
                  LEFT JOIN (SELECT INN,address,geo,region,street FROM `testdb`.`vine_geo`) AS vine_geo
                  ON (matrix_table.address = vine_geo.street AND matrix_table.INN = vine_geo.INN)
WHERE tvi1.report_date is NULL;


#Обновляем данные на Целевой/Нецелевой по ресторанам
UPDATE `testdb`.`vine_import_1`
left join (SELECT DISTINCT #Уникальные целевые рестораны из таблицы Романа 
                      restaurant, 
                      address, 
                      organozation,
                      INN
           FROM `testdb`.`vine_target_all`) target_table
on (`vine_import_1`.`INN` = `target_table`.`INN` AND 
                     trim(SUBSTRING(`vine_import_1`.`address`, INSTR(`vine_import_1`.`address`, ',') + 1)) = `target_table`.`address`)
set `vine_import_1`.`target` = CONVERT('Целевой' using cp1251)
WHERE target_table.restaurant IS NOT null;




#Обновление на целевые и нецелевые рестораны
WITH #нецелевые рестораны не должны присутствовать в целевых, колонка MAP сделана того, чтобы рестики не повторялись в целевой не присутсвуем
targer_present AS 
                  (SELECT DISTINCT restaurant, concat(restaurant, geo) AS MAPS, geo, CONVERT('Целевой-присутствуем'using cp1251) AS type_target, INN
                   FROM `testdb`.`vine_import_1`
                   WHERE target = 'Целевой' AND brand like '%Галицкий%' AND (
                                   (region like '%Санкт%' and quantity_bottle > 0) 
                                   OR
                                   (region like '%Моск%' AND report_date >= (SELECT DATE_SUB(MAX(report_date), INTERVAL 3 MONTH) FROM `testdb`.`vine_import_1` WHERE region like '%Моск%') AND quantity_bottle > 0))),
targer_present_finished AS 
                  (SELECT DISTINCT restaurant,geo, CONVERT('Целевой - не присутствуем' using cp1251) AS type_target, INN
                   FROM `testdb`.`vine_import_1`
                   WHERE concat(restaurant, geo) NOT IN (SELECT MAPS FROM targer_present) AND #условие если рестик есть в целевой - присутствуем, значит его не должно быть в целевой не присутсвуем
                                                                       target = 'Целевой' AND brand like '%Галицкий%' AND (
                                                                                  (region like '%Санкт%' and quantity_bottle < 1) #условие для Питера
                                                                                   OR #ниже условие для Мск
                                                                                  (region like '%Моск%' AND report_date < (SELECT DATE_SUB(MAX(report_date), INTERVAL 3 MONTH) FROM `testdb`.`vine_import_1` WHERE region like '%Моск%') AND quantity_bottle >= 0))
                    UNION 
                    #целевой присутствуем и целевой не присутствуем
                    SELECT restaurant, geo, type_target, INN FROM targer_present)                              
UPDATE `testdb`.`vine_import_1` tvi
                 LEFT JOIN targer_present_finished 
                 ON (tvi.restaurant = targer_present_finished.restaurant AND tvi.geo = targer_present_finished.geo AND tvi.INN = targer_present_finished.INN)
SET tvi.type_target = ifnull(targer_present_finished.type_target, CONVERT('Нецелевой' using cp1251));

#обновим улицу
UPDATE `testdb`.`vine_import_1`
set `vine_import_1`.`street` = trim(SUBSTRING(`vine_import_1`.`address`, INSTR(`vine_import_1`.`address`, ',') + 1))

#обновим период
WITH 
all_date AS (SELECT DISTINCT report_date
             FROM `testdb`.`vine_import_1`),
date_with_period AS (SELECT report_date,
                     CONCAT(ROW_NUMBER() OVER (ORDER BY report_date),') ', MONTHNAME(report_date),' ',YEAR(report_date)) AS period
                     FROM all_date)
UPDATE `testdb`.`vine_import_1` vi1
                       LEFT JOIN date_with_period dwp ON (vi1.report_date = dwp.report_date)
set vi1.period = dwp.period;

#обновим поле organozation по просьбе Романа сделать абривиатуры ООО, ПАО и тд
UPDATE `testdb`.`vine_import_1` 
set `vine_import_1`.`organozation` = CASE #замена на ООО
	                                     WHEN CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%об%' AND 
	                                     CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%огр%' AND
	                                     CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%отве%' THEN CONCAT('OOO ', TRIM(SUBSTRING(CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251)) + 1) + 1) + 1) + 1)))
                                           #замена на Ао
                                         WHEN CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE 'акци%' AND 
                                              CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%об%' THEN CONCAT('АО ',TRIM(SUBSTRING(CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251)) + 1) + 1)))
                                           #замена на ЗАО
                                         WHEN CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%закр%' AND 
                                              CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%акц%' AND
                                              CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%об%' 
                                              THEN CONCAT('ЗАО ', TRIM(SUBSTRING(CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251)) + 1) + 1) + 1)))
                                           #замена на OАО
                                         WHEN CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%откр%' AND 
                                              CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%акц%' AND
                                              CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%об%' 
                                              THEN CONCAT('ОАО ', TRIM(SUBSTRING(CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251)) + 1) + 1) + 1)))
                                           #замена на ПАО
                                         WHEN CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%публ%' AND 
                                              CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%акц%' AND
                                              CONVERT(REPLACE(organozation,' ','') using cp1251) LIKE '%об%' 
                                              THEN CONCAT('ПАО ', TRIM(SUBSTRING(CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251), LOCATE(' ', CONVERT(REPLACE(organozation,' ','') using cp1251)) + 1) + 1) + 1)))
                                              ELSE 0 END; 


#отправка письма на обработку 
INSERT INTO `email`.`input_mail`
    SELECT
        null as `mail_id`,
        now() as `mail_date`,
        'test' as `group_id`,
        concat('Добавить GEO точки в таблицу vine_geo') as `subject`,
        qwe.send as `body`,
        0 as `is_done`
     FROM (WITH 
              a AS (SELECT vta.restaurant,vta.region,vta.address,vta.organozation,vta.INN
                    FROM `testdb`.`vine_target_all` vta
                                LEFT JOIN testdb.vine_geo vg ON vta.address = vg.street
                                                                  AND vta.inn = vg.inn
                    WHERE vg.INN IS NULL),
              b AS (SELECT count(*) AS e FROM a),
              c AS (SELECT CASE when b.e > 0 THEN 1 ELSE 'Нет данных' END AS e FROM b),
              d AS (SELECT 1 AS maps,CASE WHEN c.e = 1 THEN concat('<br>Проставить ГЕО точки: ',
                                                                   '<br><br>',
                                                                   '<table style="width: 100%">',
                                                                   '<colgroup>',
                                                                   '<col span="1" style="width: 50%;">',
                                                                   '<col span="1" style="width: 50%;">',
                                                                   '</colgroup>',
                                                                   '<tr>',
                                                                   '<th>Адрес</th>',
                                                                   '<th>ИНН</th>',
                                                                   '</tr>') ELSE 0 END AS letter_body FROM c),
              e AS (SELECT 1 AS maps, GROUP_CONCAT(concat('<tr>',
                                                          '<td>', address, '</td>',
                                                          '<td>', INN, '</td>'
                                                          '</tr>') SEPARATOR '') AS artefacts FROM a)
              SELECT concat(CONVERT(d.letter_body using cp1251), CONVERT(e.artefacts using cp1251), '</table>') AS send 
              FROM e JOIN d USING(maps)) qwe;
              