-- CREATE TABLE IF NOT EXISTS `partner`.`rates` (
--     `id` INTEGER PRIMARY KEY,
--     `rate` FLOAT
-- ) ENGINE = InnoDB;

GRANT ALL PRIVILEGES ON partner.* TO 'datalens'@'%' IDENTIFIED BY 'qwerty';
