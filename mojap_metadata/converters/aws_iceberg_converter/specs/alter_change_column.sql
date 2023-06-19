ALTER TABLE {{ database_name }}.{{ table_name }}
CHANGE {{ column_old_name }} {{ column_new_name }} {{ column_type }}
