
create_shcema = 'create schema if not exists SibarИt_prod'
set_path = 'set search_path to SibarИt_prod'

""" Below querry for creating general table if not exists"""
    
delete_tmp_1st = """
    drop table if exists STG_tmp_apart_data;
"""

delete_tmp_view = """
    drop view STG_v_apart_data;
"""

delete_tmp_2nd = """    
    drop table if exists STG_tmp_apart_data_new_rows;
    drop table if exists STG_tmp_apart_data_updated_rows;
"""

create_FACT_table = """
    CREATE TABLE if not exists DWH_FACT_apart_data(
            id serial4,
            link varchar(180),
            "Год постройки" date,
            "Год сдачи" date,
            Дом varchar(50),
            "Жилая площадь" integer,
            "Общая площадь" integer,
            Отделка varchar(50),
            "Площадь кухни" integer,
            Этаж varchar(50),
            city varchar(50),
            rooms integer,
            metro varchar(50),
            price integer,
            effective_from timestamp default current_timestamp,
            effective_to timestamp default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
            deleted_flg integer default 0
            );
    """

""" create temporary table for SCD2 table """
create_tmp_table = """ 
    CREATE TABLE if not exists STG_tmp_apart_data (
            id serial4,
            link varchar(180),
            "Год постройки" date,
            "Год сдачи" date,
            Дом varchar(50),
            "Жилая площадь" integer,
            "Общая площадь" integer,
            Отделка varchar(50),
            "Площадь кухни" integer,
            Этаж varchar(50),
            city varchar(50),
            rooms integer,
            metro varchar(50),
            price integer,
            effective_from timestamp default current_timestamp,
            effective_to timestamp default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
            deleted_flg integer default 0
            );
"""

""" This is the realisation of Incremental_load.
     Names of created tables by func: {table_name}_new_rows, {table_name}_deleted_rows,
      {table_name}_changed_rows.
    """
inc_views = """
    CREATE VIEW STG_v_apart_data as
        SELECT
            id, link, price, "Жилая площадь", "Общая площадь"
            FROM DWH_FACT_apart_data           
            WHERE deleted_flg = 0
            AND current_timestamp between effective_from and effective_to
    ;
"""

tmp_new_rows = """    
    CREATE TABLE if not exists STG_tmp_apart_data_new_rows as(
            select
                t1.*
            from STG_tmp_apart_data t1
            left join STG_v_apart_data t2
            on t1.link = t2.link
            WHERE t2.link is NULL
    );
"""

tmp_updated_rows = """
    CREATE TABLE if not exists STG_tmp_apart_data_updated_rows as(
        select
        t2.*
        from STG_v_apart_data t1
        inner join STG_tmp_apart_data t2
        on t1.link = t2.link
        and (t1.price <> t2.price
            or t1."Общая площадь" <> t2."Общая площадь"
            or t1."Жилая площадь"<> t2."Жилая площадь")
            );
"""

inc_insert1 = """
    INSERT INTO DWH_FACT_apart_data(
            link, "Год постройки", "Год сдачи",
                     "Дом", "Жилая площадь", "Общая площадь", "Отделка", "Площадь кухни", "Этаж", city, rooms, metro, price
            )
        SELECT
            link, "Год постройки", "Год сдачи",
                     "Дом", "Жилая площадь", "Общая площадь", "Отделка", "Площадь кухни", "Этаж", city, rooms, metro, price
        FROM STG_tmp_apart_data_new_rows    
    ;
"""

inc_update1 = """
    UPDATE DWH_FACT_apart_data
        set effective_to = date_trunc('second', now() - interval '1 second'),
        deleted_flg = 1
        where link in (select link from STG_tmp_apart_data_updated_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    ;
"""

inc_insert2 = """
    INSERT INTO DWH_FACT_apart_data(
            link, "Год постройки", "Год сдачи",
                     "Дом", "Жилая площадь", "Общая площадь", "Отделка", "Площадь кухни", "Этаж", city, rooms, metro, price
            )
        SELECT
            link, "Год постройки", "Год сдачи",
                     Дом, "Жилая площадь", "Общая площадь", "Отделка", "Площадь кухни", "Этаж", city, rooms, metro, price
        FROM STG_tmp_apart_data_updated_rows
    ;
"""