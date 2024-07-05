DROP TABLE YOUTUBE.CLEAN_DATA.clean; 

-- Existiam arquivos com a formatação de tags(head) diferentes e para termos um padrão único usamos a query abaixo.
-- E usamos o JSON para popular uma tabela.
-- Criada um tabela do tipo TRANSIENT pois essa é uma tabela intermediária e não é necessário a função de timetravel
CREATE TRANSIENT TABLE IF NOT EXISTS YOUTUBE.CLEAN_DATA.transient_data AS 
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
