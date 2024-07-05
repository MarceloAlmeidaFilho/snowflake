--Criação da tabela com a definição de coluna do tipo 
-- VARIANT que serve para armazenar objetos JSON
CREATE TABLE IF NOT EXISTS cursos_raw (
    JSON_RAW VARIANT
    ,file_last_modified DATETIME
);

-- Inserção na tabela fazendo query diretamente no S3
--  e usando a keyword METADATA para pegar data de alteração
-- O uso do file format diz como a engine deve tratar o JSON na leitura
COPY INTO cursos_raw
    FROM (
        SELECT * 
            ,METADATA$FILE_LAST_MODIFIED 
        FROM @cursos/videos/files
    )         
    FILE_FORMAT = (FORMAT_NAME = 'JSON_FMT')
    ;
