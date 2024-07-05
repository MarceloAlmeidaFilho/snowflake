-- Injestão automática quando tem documento novo no S3
CREATE PIPE cursos_raw
  AUTO_INGEST = TRUE
  AS
  COPY INTO cursos_raw
    FROM (
        SELECT * 
            ,METADATA$FILE_LAST_MODIFIED 
        FROM @cursos/videos/files
    )         
    FILE_FORMAT = (FORMAT_NAME = 'JSON_FMT')
    ;
