-- Referência a camada do storage do S3
CREATE STAGE cursos
    URL='s3://youtube.teste/'
    STORAGE_INTEGRATION=AWS_S3_INT;
