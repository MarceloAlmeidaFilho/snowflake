-- Query para filtrar arquivo pela data mais recente baseado no ID
CREATE TABLE IF NOT EXISTS YOUTUBE.VIDEOS_OK.REFINED AS
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
        FROM transient_data
        ) WHERE mais_recente = 1 
    ORDER BY likes DESC; 


DESC TABLE transient_data
