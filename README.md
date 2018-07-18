# ya
У вас есть файл с большим массивом данных (миллион записей) и таблица в базе данных. Загрузите данные в таблицу с помощью Python.
Перед запуском необходимо в файле settings.ini указать параметры для подключения к БД.
В БД необходимо создать таблицу для импорта данных.
Пример:
CREATE TABLE for_loadings (
    ID NUMBER(5) NOT NULL,
    TEXT_FIELD_1 VARCHAR2(100),
    TEXT_FIELD_2 VARCHAR2(100) NOT NULL,
    num_field NUMBER(20)
    );

