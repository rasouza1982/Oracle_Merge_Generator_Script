--> Merge Generator 

WITH
    target AS
    (SELECT
      upper(trim(:owner))      AS owner,
      upper(trim(:table_name)) AS table_name
--        upper(trim(:owner))      AS owner,
--        upper(trim(:table_name)) AS table_name
      FROM
        dual
    ),
    all_cols AS
    (SELECT
        atc.owner       AS owner,
        atc.table_name  AS table_name,
        atc.column_name AS column_name,
        atc.column_id   AS column_id,
        atc.data_type   AS data_type,
        atc.data_length AS data_length
      FROM
        all_tab_cols atc,
        target
      WHERE
        atc.owner           = target.owner      AND
        atc.table_name      = target.table_name AND
        atc.hidden_column  != 'YES'             AND
        atc.virtual_column != 'YES'
      ORDER BY
        column_id
    ),
    pk_cols AS
    (SELECT
        ac.owner           AS owner,
        ac.table_name      AS table_name,
        acc.column_name    AS column_name,
        acc.position       AS position,
        all_cols.column_id AS column_id
      FROM
        all_constraints ac,
        all_cons_columns acc,
        all_cols
      WHERE
        ac.owner            = all_cols.owner AND
        ac.table_name       = all_cols.table_name AND
        ac.constraint_type  = 'P' AND
        acc.owner           = ac.owner AND
        acc.table_name      = ac.table_name AND
        acc.constraint_name = ac.constraint_name AND
        acc.column_name     = all_cols.column_name
      ORDER BY
        acc.position
    ),
    data_cols AS
    (SELECT
        owner,
        table_name,
        column_name,
        column_id
      FROM
        all_cols
    MINUS
    SELECT
        owner,
        table_name,
        column_name,
        column_id
      FROM
        pk_cols
      ORDER BY
        column_id
    ),
    sql_parts AS
    (SELECT
        'SELECT'                 AS sql_select,
        'FROM'                   AS sql_from,
        'WHERE'                  AS sql_where,
        'ORDER BY'               AS sql_order_by,
        ';'                      AS sql_semi,
        'MERGE INTO'             AS sql_merge_into,
        'USING'                  AS sql_using,
        'ON'                     AS sql_on,
        'WHEN MATCHED THEN'      AS sql_when_matched,
        'UPDATE SET'             AS sql_update_set,
        'WHEN NOT MATCHED THEN'  AS sql_when_not_matched,
        'INSERT'                 AS sql_insert,
        'VALUES'                 AS sql_values,
        '('                      AS sql_paren_open,
        ')'                      AS sql_paren_close,
        chr(10)                  AS sql_lf,
        '  '                     AS sql_t,
        MAX(LENGTH(column_name)) AS max_col_length
      FROM
        dual,
        all_cols
    )
  --------------------------------------------------------------------------------
  SELECT
      STATEMENT
    FROM
      (SELECT
          1              AS statement_order,
          rownum         AS row_order,
          sql_merge_into AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          2      AS statement_order,
          rownum AS row_order,
          sql_parts.sql_t
          ||lower(owner)
          ||'.'
          ||lower(table_name)
          ||' o' AS STATEMENT
        FROM
          target,
          sql_parts
      UNION
      SELECT
          3         AS statement_order,
          rownum    AS row_order,
          sql_using AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          4      AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_paren_open AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          5      AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          ||sql_select AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          6      AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          ||sql_t
          ||sql_t
          || rpad(lower(all_cols.column_name), sql_parts.max_col_length, ' ')
          ||rpad(' AS '
          ||all_cols.column_name
          ||(
            CASE
              WHEN lead(all_cols.column_id) over (order by all_cols.column_id) IS NOT NULL
              THEN ','
              ELSE NULL
            END), sql_parts.max_col_length+6, ' ')
          ||'-- '
          ||all_cols.data_type
          ||sql_parts.sql_paren_open
          ||all_cols.data_length
          ||sql_parts.sql_paren_close AS STATEMENT
        FROM
          all_cols,
          sql_parts
      UNION
      SELECT
          7      AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          ||sql_t
          ||sql_from
        FROM
          sql_parts
      UNION
      SELECT
          8      AS statement_order,
          rownum AS row_order,
          sql_parts.sql_t
          ||sql_parts.sql_t
          ||sql_parts.sql_t
          ||sql_parts.sql_t
          || lower(target.owner
          ||'.'
          ||target.table_name) AS STATEMENT
        FROM
          target,
          sql_parts
      UNION
      SELECT
          9      AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          ||sql_t
          || sql_order_by AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          10       AS statement_order,
          position AS row_order,
          sql_t
          ||sql_t
          ||sql_t
          ||sql_t
          || lower(column_name)
          || (
            CASE
              WHEN lead(position) over (order by position) IS NOT NULL
              THEN ','
              ELSE NULL
            END )
        FROM
          pk_cols,
          sql_parts
      UNION
      SELECT
          11     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_paren_close
          ||' n' AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          12     AS statement_order,
          rownum AS row_order,
          sql_on AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          13     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_paren_open AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          14       AS statement_order,
          position AS row_order,
          sql_t
          ||sql_t
          || rpad('o.'
          ||lower(column_name), max_col_length+2, ' ')
          ||' = '
          ||'n.'
          ||lower(column_name)
          ||(
            CASE
              WHEN lead(column_id) over (order by position) IS NOT NULL
              THEN ' AND'
              ELSE NULL
            END) AS STATEMENT
        FROM
          pk_cols,
          sql_parts
      UNION
      SELECT
          15     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_paren_close AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          16               AS statement_order,
          rownum           AS row_order,
          sql_when_matched AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          17     AS statement_order,
          rownum AS row_order,
          sql_t
          || sql_update_set AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          18     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          || rpad('o.'
          ||lower(column_name), max_col_length+2, ' ')
          ||' = '
          ||'n.'
          ||lower(column_name)
          ||(
            CASE
              WHEN lead(column_id) over (order by column_id) IS NOT NULL
              THEN ','
              ELSE NULL
            END) AS STATEMENT
        FROM
          data_cols,
          sql_parts
      UNION
      SELECT
          19                   AS statement_order,
          rownum               AS row_order,
          sql_when_not_matched AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          20     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_insert AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          21     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          ||sql_paren_open AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          22     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          ||sql_t
          || 'o.'
          ||lower(column_name)
          ||(
            CASE
              WHEN lead(column_id) over (order by column_id) IS NOT NULL
              THEN ','
              ELSE NULL
            END) AS STATEMENT
        FROM
          all_cols,
          sql_parts
      UNION
      SELECT
          23     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          ||sql_paren_close AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          24     AS statement_order,
          rownum AS row_order,
          sql_t
          || sql_values
        FROM
          sql_parts
      UNION
      SELECT
          25     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          ||sql_paren_open AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          26     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          ||sql_t
          || 'n.'
          ||lower(column_name)
          ||(
            CASE
              WHEN lead(column_id) over (order by column_id) IS NOT NULL
              THEN ','
              ELSE NULL
            END) AS STATEMENT
        FROM
          all_cols,
          sql_parts
      UNION
      SELECT
          27     AS statement_order,
          rownum AS row_order,
          sql_t
          ||sql_t
          ||sql_paren_close AS STATEMENT
        FROM
          sql_parts
      UNION
      SELECT
          28       AS statement_order,
          rownum   AS row_order,
          sql_semi AS STATEMENT
        FROM
          sql_parts
      )
    ORDER BY
      statement_order,
row_order ;
