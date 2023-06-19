CREATE TABLE {{ database_name }}.{{ table_name }} (
    {% for colname, coltype, coldesc in columns %}
        {{ colname }} {{ coltype }} COMMENT '{{ coldesc }}'{{ ", " if not loop.last else "" }}
    {% endfor %}
)
{% if partitions %}
    PARTITIONED BY (
        {% for partition in partitions %}
            {{ partition }}{{ ", " if not loop.last else "" }}
        {% endfor %}
    )
{% endif %}
LOCATION '{{ table_location }}'
TBLPROPERTIES (
    'table_type'='iceberg',
    'format'='parquet'
)
