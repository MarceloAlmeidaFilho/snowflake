-- Criação de tasks que rodam em warehouse serverless aplicando as tranformações necessárias
CREATE OR REPLACE TASK popula_transiente
  SCHEDULE = 'USING CRON */2 * * * * America/Sao_Paulo'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  AS
    CREATE OR REPLACE TRANSIENT TABLE YOUTUBE.CLEAN_DATA.transient_data AS 
        SELECT 
            DISTINCT json_raw['items'][0]['id']::VARCHAR(15) AS id
            ,file_last_modified
            ,json_raw['items'][0]['contentDetails']['duration']::VARCHAR AS duracao
            ,json_raw['items'][0]['snippet']['channelTitle']::VARCHAR AS titulo_canal
            ,json_raw['items'][0]['snippet']['defaultAudioLanguage']::VARCHAR AS idioma
            ,json_raw['items'][0]['snippet']['publishedAt']::VARCHAR AS data
            ,json_raw['items'][0]['snippet']['title']::VARCHAR AS titulo
            ,json_raw['items'][0]['snippet']['tags']::VARCHAR AS palavras_chave
            ,json_raw:items[0].statistics.commentCount::INT AS num_comentario
            ,json_raw['items'][0]['statistics']['favoriteCount']::INT  AS qtde_favorito
            ,json_raw['items'][0]['statistics']['likeCount']::INT  AS likes
            ,json_raw['items'][0]['statistics']['viewCount']::INT AS visualizacao
        FROM
        (   
            SELECT json_raw, file_last_modified
            FROM
                (
                SELECT json_raw AS json_raw, file_last_modified
                , ROW_NUMBER() OVER (
                              PARTITION BY json_raw['items'][0]['id']
                              ORDER BY json_raw['items'][0]['id'] DESC
                            ) DupRank
                FROM cursos_raw
                )
            WHERE DupRank = 1 AND OBJECT_KEYS(JSON_RAW)[0] = 'etag' 
            
            UNION
            
            SELECT DISTINCT elements.value AS json_raw, file_last_modified
            FROM cursos_raw
            , LATERAL FLATTEN ( input => JSON_RAW) elements
            WHERE TYPEOF(elements.value) = 'OBJECT' 
            AND OBJECT_KEYS(json_raw)[0] <> 'etag'
        )
        ORDER BY id;


CREATE or replace TASK upsert_refined 
    SCHEDULE = 'USING CRON */2 * * * * America/Sao_Paulo'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    AS
        MERGE INTO YOUTUBE.VIDEOS_OK.REFINED AS target_table
        USING  (
                SELECT 
                    id
                    ,file_last_modified
                    ,duracao
                    ,titulo_canal
                    ,idioma
                    ,data
                    ,titulo
                    ,palavras_chave
                    ,qtde_favorito
                    ,likes
                    ,visualizacao
                    ,num_comentario
                FROM
                    (
                    SELECT *
                    , ROW_NUMBER() OVER (
                                  PARTITION BY id
                                  ORDER BY file_last_modified DESC
                                ) mais_recente
                    FROM YOUTUBE.CLEAN_DATA.TRANSIENT_DATA
                    ) 
                WHERE mais_recente = 1 
                ORDER BY likes DESC
            ) AS source_table 
            
        ON target_table.id = source_table.id
            WHEN MATCHED THEN 
                UPDATE SET 
                     target_table.file_last_modified = source_table.file_last_modified
                    ,target_table.duracao = source_table.duracao
                    ,target_table.titulo_canal = source_table.titulo_canal
                    ,target_table.idioma = source_table.idioma
                    ,target_table.data = source_table.data
                    ,target_table.titulo = source_table.titulo
                    ,target_table.palavras_chave = source_table.palavras_chave
                    ,target_table.qtde_favorito = source_table.qtde_favorito
                    ,target_table.likes = source_table.likes
                    ,target_table.visualizacao = source_table.visualizacao
                    ,target_table.num_comentario = source_table.num_comentario
            WHEN NOT MATCHED 
                THEN INSERT 
                     (target_table.id
                    ,target_table.file_last_modified
                    ,target_table.duracao
                    ,target_table.titulo_canal 
                    ,target_table.idioma 
                    ,target_table.data 
                    ,target_table.titulo 
                    ,target_table.palavras_chave 
                    ,target_table.qtde_favorito 
                    ,target_table.likes 
                    ,target_table.visualizacao 
                    ,target_table.num_comentario) 
                VALUES 
                    (source_table.id
                    ,source_table.file_last_modified
                    ,source_table.duracao
                    ,source_table.titulo_canal
                    ,source_table.idioma
                    ,source_table.data
                    ,source_table.titulo
                    ,source_table.palavras_chave
                    ,source_table.qtde_favorito
                    ,source_table.likes
                    ,source_table.visualizacao
                    ,source_table.num_comentario);

