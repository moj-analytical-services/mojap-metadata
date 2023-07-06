ALTER TABLE {{ database_name }}.{{ table_name }} ADD COLUMNS (
    {% for colname, coltype in columns %}
        {{ colname }} {{ coltype }}{{ ", " if not loop.last else "" }}
    {% endfor %}
)
