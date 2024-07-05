CREATE OR REPLACE STORAGE INTEGRATION AWS_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::130227582337:role/snowflake2'
  STORAGE_ALLOWED_LOCATIONS = ('s3://youtube.teste/')
  COMMENT = 'Integration to AWS s3' ;

-- STRIP_OUTER_ARRAY para retirar JSONs de dentro de uma lista
CREATE OR replace FILE FORMAT JSON_FMT
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE;

-- Funções para descrever entidades
DESC INTEGRATION AWS_S3_INT;
DESC FILE FORMAT JSON_FMT;
LIST @cursos/videos/files;

-- Query para pegar os metadados de dentro do bucket
SELECT * FROM TABLE(infer_schema(
    location => '@cursos/videos/files' ,
    files => 'Delta Live Tables-14-3-19.json',
    file_format=>'JSON_FMT',
    ignore_case => true
));
