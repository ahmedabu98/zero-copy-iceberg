-- HADOOP LOCAL
CREATE CATALOG local_catalog
TYPE 'iceberg'
PROPERTIES(
    'type' = 'hadoop',
    'warehouse' = '/Users/ahmedabualsaud/github/zero-copy-iceberg/warehouse'
);

SELECT * FROM local_catalog.my_namespace.my_table;


-- LAKEHOUSE GCS
CREATE CATALOG lakehouse TYPE 'iceberg'
PROPERTIES (
    'type' = 'rest',
    'uri' = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    'warehouse' = 'gs://zero-copy-warehouse',
    'header.x-goog-user-project' = 'apache-beam-testing',
    'rest.auth.type' = 'google',
    'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO',
    'header.X-Iceberg-Access-Delegation' = 'vended-credentials'
);

SELECT COUNT(*) FROM lakehouse.my_namespace.my_table;