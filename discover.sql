CREATE CATALOG local_catalog
TYPE 'iceberg'
PROPERTIES(
    'type' = 'hadoop',
    'warehouse' = '/Users/ahmedabualsaud/github/zero-copy-iceberg/warehouse'
);

SHOW DATABASES IN local_catalog;

SHOW TABLES IN local_catalog.my_namespace;

SELECT * FROM local_catalog.my_namespace.my_table;