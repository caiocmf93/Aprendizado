create or replace PACKAGE BODY PKG_LOAD_SYNONYM IS
   --
   VG_ASPA       CHAR(1) := ''''; -- DEFINICAO DE ASPA SIMPLES
   VG_DASPA      CHAR(2) := ''''''; -- DEFINICAO DE DUPLA ASPA SIMPLES
   VG_SQL        VARCHAR2(4000); -- SQL QUE SERA EXECUTADO NO PROCESSO
   VG_ExSQL      VARCHAR2(4000); -- SQL QUE SERA EXECUTADO NO PROCESSO
   VG_PROCESSO   VARCHAR2(50) := 'PKG_LOAD_SYNONYM'; -- VARIAVEL GLOBAL PARA REGISTRAR O PROCESSO
   VG_DTINICIO   DATE; -- DATA DE INICIO DO PROCESSO
   VG_OBS        VARCHAR2(4000) := NULL; -- VARIAVEL DE OBSERVACOES DO PROCESSO
   VG_ERRO       VARCHAR2(1000) := NULL; -- VARIAVEL PARA COLETA DE ERRO DO PROCESSO
 --  VG_TABELA     VARCHAR2(30);
 --  VG_OWNER      VARCHAR2(30);
   -- ----------------------------------------------------------------------------------------
   -- Esta procedure tem como objetivo preparar (truncar e remover indices) a tabela ???_TMP para poder ser carregada.
   --    A tabela *_TMP deve estar preparada para o processo e deve ser identica a tabela FINAL.
   PROCEDURE PRC_PRE_LOAD ( P_NM_TABELA_ORIGINAL   IN VARCHAR2 ) IS
     --
--      V_SQL                  VARCHAR2(4000); -- SQL QUE SERA EXECUTADO NO PROCESSO
 --    V_SUFIXO               VARCHAR2(3); -- SULFIXO DA TABELA TEMPORARIA - DEVE TERMINAR COM 'TMP'
 --    V_CHK_SULFIXO          VARCHAR2(3) := 'TMP'; -- SULFIXO PARA CONFERENCIA
 --    V_NR_ERRO              NUMBER(5); -- PONTO DA OCORRENCIA
      V_TAB_TEMP            VARCHAR2(30); -- NOME DA TABELA A SER TRUNCADA
      V_OWNER_TEMP          VARCHAR2(30); -- NOME DO OWNER DA TABELA A SER TRUNCADA
--      V_SAIDA                VARCHAR2(4000); -- ACUMULA MENSAGENS NO PROCESSO
      -- CURSOR PARA SELECIONAR DADOS DAS TABELAS TEMPORARIAS
      CURSOR CS_TABS IS
         SELECT a.TABELA_TMP, a.OWNER_TMP--, TABELA_BKP, ATIVO_PROCESSO, CAMPO_PART, TIPO_PART, TIPO_SUB_PART, QTDE_PART, QTDE_SUB_PART
            FROM SASDM_O.DW_CONTROLE_TABELA_PART a
            INNER JOIN ALL_SYNONYMS B 
                ON a.TABELA_ORI = b.SYNONYM_NAME 
           WHERE a.TABELA_ORI = P_NM_TABELA_ORIGINAL
             AND a.ATIVO_PROCESSO = 2;
   --
   BEGIN
      -- BUSCA DADOS DA TABELA TEMP A SER TRUNCADA
      --V_NR_ERRO := 400;
      OPEN CS_TABS;
      LOOP
         --V_NR_ERRO := 500;
         FETCH CS_TABS INTO V_TAB_TEMP, V_OWNER_TEMP;
         EXIT WHEN CS_TABS%NOTFOUND;
         --DBMS_OUTPUT.PUT_LINE('PRC_PRE_LOAD -> '||V_OWNER_TEMP||'.'||V_TAB_TEMP);
         -- VERIFICA SE HA INDICES E TORNA-OS INATIVOS
         PRC_DESATIVA_INDICE(V_OWNER_TEMP, V_TAB_TEMP);
         -- EFETUA O TRUNCATE DA TABELA TEMP
         PRC_TRUNCA_TEMP(V_OWNER_TEMP, V_TAB_TEMP);
      END LOOP;
      --
      --V_NR_ERRO := 700;
      CLOSE CS_TABS;
/*    END IF;
    --
    V_SAIDA := V_SAIDA||'-- INICIO DO PRE-LOAD DA TABELA '||P_NM_OWNER_TMP||'.'||P_NM_TABELA_TMP||' - '||TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS')
               ||CHR(10)||LPAD('*',80,'*')||CHR(10);
*/
   EXCEPTION
      WHEN OTHERS THEN
         --V_SAIDA := V_SAIDA||LPAD('*',17,'*')||' E R R O '||LPAD('*',17,'*')||' E R R O '
         --||LPAD('*',17,'*')||' E R R O '||LPAD('*',17,'*')||CHR(10)
         --           ||'-- PONTO ERRO: '||V_NR_ERRO||CHR(10)||'-- MENSAGEM DO ERRO: '||SQLERRM||CHR(10)||LPAD('*',80,'*')||CHR(10);
         CLOSE CS_TABS;
         RAISE_APPLICATION_ERROR(-20999, 'PRC_PRE_LOAD: '||SQLERRM);
   END PRC_PRE_LOAD;
   --
   -- ----------------------------------------------------------------------------------------
   -- Esta procedure tem como objetivo Transferir os dados da tabela TMP para a tabela DESTINO. As tabelas DESTINO e TMP deve ser identicas.
   PROCEDURE PRC_DESATIVA_INDICE ( P_NM_OWNER_TEMP    IN VARCHAR2
                                 , P_NM_TABELA_TEMP   IN VARCHAR2 ) IS
      -- CURSOR PARA SELECIONAR INDICES DA TABELA TEMP
      CURSOR CS_INDICES_DES IS
         SELECT INDEX_NAME, OWNER
            FROM ALL_INDEXES
            WHERE TABLE_NAME = P_NM_TABELA_TEMP
              AND TABLE_OWNER = P_NM_OWNER_TEMP
              --AND STATUS NOT LIKE 'UNUS%'
              ;
      V_NM_INDICE VARCHAR2(30);
      V_NM_OWNER  VARCHAR2(30);
   BEGIN
      -- VERIFICA SE HA INDICES E TORNA-OS INATIVOS
--      V_NR_ERRO := 400;
      VG_ExSQL := NULL;
      VG_SQL := NULL;
      OPEN CS_INDICES_DES;
      LOOP
--         V_NR_ERRO := 500;
         FETCH CS_INDICES_DES INTO V_NM_INDICE, V_NM_OWNER;
         EXIT WHEN CS_INDICES_DES%NOTFOUND;
         VG_ExSQL := 'ALTER INDEX '|| V_NM_OWNER ||'.'|| V_NM_INDICE ||' UNUSABLE';
         VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- forma correta
         --DBMS_OUTPUT.put_line('PRC_DESATIVA_INDICE -> '||VG_SQL);
--         V_SAIDA := V_SAIDA||'-- ALTERA INDICE '||V_NM_INDICE||' P/ UNUSABLE - '||TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS')
--                    ||CHR(10)||'-- '||SUBSTR(V_SQL,001,100)||CHR(10)||'--';
--         V_NR_ERRO := 600;
         -- EXECUTA O COMMANDO
         EXECUTE IMMEDIATE (VG_SQL);
      END LOOP;
      if VG_ExSQL is null then
          --DBMS_OUTPUT.put_line('PRC_DESATIVA_INDICE SEM INDICE -> '||P_NM_OWNER_TEMP||'.'||P_NM_TABELA_TEMP);
          NULL;
      end if;
--      V_NR_ERRO := 700;
      CLOSE CS_INDICES_DES;
   EXCEPTION
      WHEN OTHERS THEN
--         V_SAIDA := V_SAIDA||LPAD('*',17,'*')||' E R R O '||LPAD('*',17,'*')||' E R R O '
--||LPAD('*',17,'*')||' E R R O '||LPAD('*',17,'*')||CHR(10)
--                    ||'-- PONTO ERRO: '||V_NR_ERRO||CHR(10)||'-- MENSAGEM DO ERRO: '||SQLERRM||CHR(10)||LPAD('*',80,'*')||CHR(10);
         CLOSE CS_INDICES_DES;
         RAISE_APPLICATION_ERROR(-20999, 'ERRO PRC_DESATIVA_INDICE: '||SQLERRM/*V_SAIDA*/);
   END PRC_DESATIVA_INDICE;
   --
   -- ----------------------------------------------------------------------------------------
   -- Esta procedure tem como objetivo Transferir os dados da tabela TMP para a tabela DESTINO. As tabelas DESTINO e TMP deve ser identicas.
   PROCEDURE PRC_TRUNCA_TEMP ( P_NM_OWNER_TEMP    IN VARCHAR2
                             , P_NM_TABELA_ORIGINAL   IN VARCHAR2 ) IS
		V_SYNONYM_NAME VARCHAR2(100);
		V_TABLE_NAME VARCHAR2(100);
		V_OWNER_NAME VARCHAR2(100);
   BEGIN
	  SELECT SYNONYM_NAME, TABLE_NAME, OWNER
	      INTO V_SYNONYM_NAME, V_TABLE_NAME, V_OWNER_NAME
  	    FROM ALL_SYNONYMS
	     WHERE SYNONYM_NAME = P_NM_TABELA_ORIGINAL;
	  --
      VG_ExSQL := 'TRUNCATE TABLE '||V_OWNER_NAME||'.'||V_TABLE_NAME;
      VG_SQL := 'CALL '||V_OWNER_NAME||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- forma correta
      --DBMS_OUTPUT.put_line('PRC_TRUNCA_TEMP -> '||VG_SQL);
      EXECUTE IMMEDIATE (VG_SQL);
   END PRC_TRUNCA_TEMP;
   --
   -- ----------------------------------------------------------------------------------------
   -- Esta procedure tem como objetivo Transferir os dados da tabela TMP para a tabela DESTINO. As tabelas DESTINO e TMP deve ser identicas.
   PROCEDURE PRC_TRNCA_TMP ( P_NM_OWNER_TEMP    IN VARCHAR2
                             , P_NM_TABELA_TEMP   IN VARCHAR2 ) IS
   BEGIN
      VG_ExSQL := 'TRUNCATE TABLE '||P_NM_OWNER_TEMP||'.'||P_NM_TABELA_TEMP;
      VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- forma correta
      --DBMS_OUTPUT.put_line('PRC_TRUNCA_TEMP -> '||VG_SQL);
      EXECUTE IMMEDIATE (VG_SQL);
   END PRC_TRNCA_TMP;
   --
   -- ----------------------------------------------------------------------------------------
   -- Esta procedure tem como objetivo Transferir os dados da tabela TMP para a tabela DESTINO. As tabelas DESTINO e TMP deve ser identicas.
   PROCEDURE PRC_POS_LOAD ( P_NM_TABELA_ORIGINAL     IN VARCHAR2 ) IS
      --
      V_TAB_TEMP            VARCHAR2(30); -- NOME DA TABELA A SER TRUNCADA
      V_TAB_BKP             VARCHAR2(30); -- NOME DA TABELA A SER TRUNCADA
      V_TAB_FUL             VARCHAR2(30); -- NOME DA TABELA A SER UTILIZADA PARA TROCA DE DADOS
      V_OWNER_TEMP          VARCHAR2(30); -- NOME DO OWNER DA TABELA A SER TRUNCADA
      V_OWNER_ORI           VARCHAR2(30); -- NOME DO OWNER DA TABELA A SER TRUNCADA
--      V_SAIDA                VARCHAR2(4000); -- ACUMULA MENSAGENS NO PROCESSO
      TYPE R_CURSOR_TMP     IS REF CURSOR;  -- REFERENCIA PARA CURSOR DINAMICO
      V_CURSOR_TMP          R_CURSOR_TMP;   -- VARIAVEL PARA CURSOR DINAMICO
      V_QUERY_TMP           VARCHAR2(4000); -- VARIAVEL PARA QUERY/STRING GENERICA
      V_QTDE_TMP            NUMBER; -- ARMAZENA SE A TMP ESTAH OU NAO VAZIA
      V_CAMPOS              VARCHAR2(3000);
      -- CURSOR PARA SELECIONAR DADOS DAS TABELAS TEMPORARIAS
      CURSOR CS_TABS_POS_LOAD IS

		 /*SELECT TABELA_TMP, OWNER_TMP, OWNER_ORI, TABELA_BKP, TABELA_FUL--, TABELA_BKP, ATIVO_PROCESSO, CAMPO_PART, TIPO_PART, TIPO_SUB_PART, QTDE_PART, QTDE_SUB_PART
            FROM SASDM_O.DW_CONTROLE_TABELA_PART
           WHERE TABELA_ORI = P_NM_TABELA_ORIGINAL
             AND ATIVO_PROCESSO = 1
             AND ATIVO_POS_LOAD = 1;*/
		    SELECT a.TABELA_TMP, a.OWNER_TMP, a.OWNER_ORI, a.TABELA_BKP, a.TABELA_FUL
            FROM DW_CONTROLE_TABELA_PART a
            INNER JOIN ALL_SYNONYMS B 
                ON a.TABELA_ORI = b.SYNONYM_NAME 
           WHERE a.TABELA_ORI = P_NM_TABELA_ORIGINAL
             AND a.ATIVO_PROCESSO = 2
             AND a.ATIVO_POS_LOAD = 2;
   --
   BEGIN
      --
      VG_DTINICIO := SYSDATE;
      VG_ERRO := NULL;
      OPEN CS_TABS_POS_LOAD;
      LOOP
         --
         FETCH CS_TABS_POS_LOAD INTO V_TAB_TEMP, V_OWNER_TEMP, V_OWNER_ORI, V_TAB_BKP, V_TAB_FUL;
         EXIT WHEN CS_TABS_POS_LOAD%NOTFOUND;
         -- INCLUSAO DE LOG
         VG_OBS := 'INICIO POS_LOAD - '||P_NM_TABELA_ORIGINAL||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
         PRC_CTRL_PROCESSO ( PNM_PROCESSO        => VG_PROCESSO                   , PDT_INI_EXECUCAO    => VG_DTINICIO
                           , PNM_OWNER           => USER                          , PNM_AGENDA          => P_NM_TABELA_ORIGINAL
                           , PNM_TIPO            => 'EXCHANGE PACKAGE'            , PST_STATUS_EXECUCAO => 'EXECUTANDO'
                           , PDS_PASSO           => 'INICIO - VER SE HA DADOS NA TMP');
         -- Acertar tabelas e retirar esse IF inteiro
         if P_NM_TABELA_ORIGINAL in ('NBA_RTD_PACOTE_MOVEL','NBA_RTD_PROMO_PRECO_MV') then
            -- VALIDA A QUANTIDADE DE REGISTROS DA TMP PARA CONTINUAR O PROCESSO
            V_QTDE_TMP := 0;
            V_QUERY_TMP := 'SELECT COUNT(1) FROM '||V_OWNER_TEMP||'.'||V_TAB_TEMP||' WHERE ROWNUM < 2'; -- VE SE TABELA TEM PELO MENOS 1 REGISTRO
            --
            OPEN V_CURSOR_TMP FOR V_QUERY_TMP;
            FETCH V_CURSOR_TMP INTO V_QTDE_TMP;
            CLOSE V_CURSOR_TMP;
            -- VALIDA QUANTIDADE - SE NÃ¿ HOUVER REGISTROS NA TABELA TMP O PROCESSO PARA
            IF V_QTDE_TMP > 0 THEN
            --
               -- INSERE BKP
               PRC_TRNCA_TMP(V_OWNER_TEMP, V_TAB_BKP);
               V_QUERY_TMP := 'INSERT /*+ APPEND*/ INTO '||V_OWNER_TEMP||'.'||V_TAB_BKP||' (';
               FOR X IN (SELECT COLUMN_NAME||' ,' COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = V_TAB_BKP AND OWNER = V_OWNER_TEMP) LOOP
                  V_CAMPOS := V_CAMPOS || X.COLUMN_NAME;
               END LOOP;
               V_CAMPOS := SUBSTR(V_CAMPOS,1,LENGTH(V_CAMPOS)-1);
               V_QUERY_TMP := V_QUERY_TMP || V_CAMPOS || ') ' ; --
               V_QUERY_TMP := V_QUERY_TMP || 'SELECT /*+ PARALLEL 8*/ ' || REPLACE(REPLACE(V_CAMPOS,'COD_PARTITION','1'),'DAT_CARGA','SYSDATE') || ' FROM '||V_OWNER_ORI||'.'||P_NM_TABELA_ORIGINAL;
               EXECUTE IMMEDIATE V_QUERY_TMP;
               COMMIT;
               -- INSERE ORI
               PRC_TRUNCA_TEMP(V_OWNER_ORI, P_NM_TABELA_ORIGINAL);
               V_CAMPOS := NULL;
               V_QUERY_TMP := 'INSERT /*+ APPEND*/ INTO '||V_OWNER_ORI||'.'||P_NM_TABELA_ORIGINAL||' (';
               FOR X IN (SELECT COLUMN_NAME||' ,' COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = P_NM_TABELA_ORIGINAL AND OWNER = V_OWNER_ORI) LOOP
                  V_CAMPOS := V_CAMPOS || X.COLUMN_NAME;
               END LOOP;
               V_CAMPOS := SUBSTR(V_CAMPOS,1,LENGTH(V_CAMPOS)-1);
               V_QUERY_TMP := V_QUERY_TMP || V_CAMPOS || ') ' ; --
               V_QUERY_TMP := V_QUERY_TMP || 'SELECT /*+ PARALLEL 8*/ ' || REPLACE(REPLACE(V_CAMPOS,'COD_PARTITION','1'),'DAT_CARGA','SYSDATE') || ' FROM '||V_OWNER_TEMP||'.'||V_TAB_TEMP;
               EXECUTE IMMEDIATE V_QUERY_TMP;
               COMMIT;
               PRC_GERA_ESTATISTICA(V_OWNER_ORI, P_NM_TABELA_ORIGINAL);
            --          
            END IF;
           null;
         else
         --
         -- VALIDA A QUANTIDADE DE REGISTROS DA TMP PARA CONTINUAR O PROCESSO
         V_QTDE_TMP := 0;
         V_QUERY_TMP := 'SELECT COUNT(1) FROM '||V_OWNER_TEMP||'.'||V_TAB_TEMP||' WHERE ROWNUM < 2'; -- VE SE TABELA TEM PELO MENOS 1 REGISTRO
         --
         OPEN V_CURSOR_TMP FOR V_QUERY_TMP;
         FETCH V_CURSOR_TMP INTO V_QTDE_TMP;
         CLOSE V_CURSOR_TMP;
         -- VALIDA QUANTIDADE - SE NÃ¿ HOUVER REGISTROS NA TABELA TMP O PROCESSO PARA
         IF V_QTDE_TMP > 0 THEN
            -- VERIFICA SE HA INDICES INVALIDOS NA TEMP E TORNA-OS ATIVOS
            PRC_ATIVA_INDICE(V_OWNER_TEMP, V_TAB_TEMP);
            DBMS_OUTPUT.PUT_LINE('chegou no ponto 1');
            -- GERA ESTATISTICAS PARA A TABELA TEMP
            PRC_GERA_ESTATISTICA(V_OWNER_TEMP, V_TAB_TEMP);
            DBMS_OUTPUT.PUT_LINE('chegou no ponto 2');
            -- DESTIVA INDICE DA TABELA PRINCIPAL
   --         PRC_DESATIVA_INDICE(V_OWNER_ORI, P_NM_TABELA_ORIGINAL);
            -- FAZ A BUSCA PARA TRATAMENTO DE PARTICAO
            PRC_BUSCA_PARTICAO(P_NM_TABELA_ORIGINAL);
            DBMS_OUTPUT.PUT_LINE('chegou no ponto 3');
            -- VERIFICA SE HA INDICES PARA TORNA-LOS ATIVOS
            PRC_ATIVA_INDICE(V_OWNER_ORI, P_NM_TABELA_ORIGINAL);
            DBMS_OUTPUT.PUT_LINE('chegou no ponto 4');
            -- GERA ESTATISTICAS PARA A TABELA ORIGINAL
            PRC_GERA_ESTATISTICA(V_OWNER_ORI, P_NM_TABELA_ORIGINAL);
            DBMS_OUTPUT.PUT_LINE('chegou no ponto 5');
            -- GERA ESTATISTICAS PARA A TABELA BACKUP
            IF V_TAB_BKP IS NOT NULL THEN
               --PRC_GERA_ESTATISTICA(V_OWNER_TEMP, V_TAB_BKP);--comentado temporariamente - Murca
               IF V_TAB_FUL IS NOT NULL THEN
                  PRC_TRNCA_TMP(V_OWNER_TEMP, V_TAB_FUL);
               END IF;
               PRC_TRNCA_TMP(V_OWNER_TEMP, V_TAB_TEMP);
            END IF;
         ELSE
            VG_OBS := VG_OBS||CHR(10)||'PRC_POS_LOAD ZERO REGISTROS -> '||V_OWNER_TEMP||'.'||V_TAB_TEMP||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
         END IF;
         end if;
      END LOOP;
      --
      --V_NR_ERRO := 700;
      CLOSE CS_TABS_POS_LOAD;
      -- valida indices invÃ¡dos - ASM OUT/2020
      -- Faz a verificaÃ§ se tem algum Ã­ice invÃ¡do
      BEGIN
         -- Verifica apenas Ã­ice
         FOR IX IN ( SELECT DISTINCT a.TABLE_NAME, a.TABLE_OWNER
						FROM ALL_INDEXES a
						INNER JOIN ALL_SYNONYMS b 
                          ON a.TABLE_NAME = b.TABLE_NAME 
                         AND a.TABLE_OWNER = b.TABLE_OWNER
						WHERE b.SYNONYM_NAME  = P_NM_TABELA_ORIGINAL
						AND a.TABLE_OWNER = V_OWNER_ORI
						AND STATUS IN ('UNUSABLE', 'N/A')
                   ) LOOP
            DBMS_OUTPUT.PUT_LINE('Tabela: ' || IX.TABLE_NAME || ', Owner: ' || IX.TABLE_OWNER || ' está com índices inválidos ou indisponíveis.');
            --DBMS_OUTPUT.PUT_LINE('IDX - '||IX.TABLE_NAME);
            PRC_ATIVA_INDICE(IX.TABLE_OWNER, IX.TABLE_NAME);
            DBMS_OUTPUT.PUT_LINE('PASSOU NA PRC_ATIVA_INDICE');
         END LOOP;
         --
         -- Verifica partiÃ§ de Ã­ice
         FOR IX IN ( SELECT DISTINCT S.TABLE_NAME,S.TABLE_OWNER 
                            FROM ALL_SYNONYMS S
                            INNER JOIN ALL_INDEXES I ON S.TABLE_NAME = I.TABLE_NAME
                            WHERE S.SYNONYM_NAME = P_NM_TABELA_ORIGINAL
                            AND I.STATUS IN ('UNUSABLE', 'N/A')
                   ) LOOP
                DBMS_OUTPUT.PUT_LINE('Tabela: ' || IX.TABLE_NAME || ', Owner: ' || IX.TABLE_OWNER || ' está com índices inválidos ou indisponíveis. 1');
            --DBMS_OUTPUT.PUT_LINE('PRT - '||IX.TABLE_NAME);
            PRC_ATIVA_INDICE(IX.TABLE_OWNER, IX.TABLE_NAME);
         END LOOP;
         --
      END;
      --
      --
      -- REGISTRA SUCESSO NO FIM DE PROCESSO
      PRC_CTRL_PROCESSO ( PNM_PROCESSO        => VG_PROCESSO
                        , PDT_INI_EXECUCAO    => VG_DTINICIO
                        , PNM_AGENDA          => P_NM_TABELA_ORIGINAL
                        , PST_STATUS_EXECUCAO => 'FINALIZADO COM SUCESSO'
                        , PDT_FIM_EXECUCAO    => SYSDATE
                        , PDS_PASSO           => 'CONCLUIDO'
                        , PDS_OBS             => VG_OBS );
      --
   EXCEPTION
      WHEN OTHERS THEN
         --
         CLOSE CS_TABS_POS_LOAD;
         VG_ERRO := SQLERRM;
         VG_OBS := VG_OBS||CHR(10)||VG_ERRO||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
         --
         -- valida indices invÃ¡dos - ASM OUT/2020
         -- Faz a verificaÃ§ se tem algum Ã­ice invÃ¡do
         BEGIN
            -- Verifica apenas Ã­ice
            FOR IX IN ( SELECT DISTINCT S.TABLE_NAME,S.TABLE_OWNER 
                            FROM ALL_SYNONYMS S
                            INNER JOIN ALL_INDEXES I ON S.TABLE_NAME = I.TABLE_NAME
                            WHERE S.SYNONYM_NAME = P_NM_TABELA_ORIGINAL
                            AND I.STATUS IN ('UNUSABLE', 'N/A')
                      ) LOOP
               --DBMS_OUTPUT.PUT_LINE('IDX - '||IX.TABLE_NAME);
               DBMS_OUTPUT.PUT_LINE('Tabela: ' || IX.TABLE_NAME || ', Owner: ' || IX.TABLE_OWNER || ' está com índices inválidos ou indisponíveis. 2');
               PRC_ATIVA_INDICE(IX.TABLE_OWNER, IX.TABLE_NAME);
            END LOOP;
            --
            -- Verifica partiÃ§ de Ã­ice
            FOR IX IN ( SELECT DISTINCT S.TABLE_NAME,S.TABLE_OWNER 
                            FROM ALL_SYNONYMS S
                            INNER JOIN ALL_INDEXES I ON S.TABLE_NAME = I.TABLE_NAME
                            WHERE S.SYNONYM_NAME = P_NM_TABELA_ORIGINAL
                            AND I.STATUS IN ('UNUSABLE', 'N/A')
                      ) LOOP
               --DBMS_OUTPUT.PUT_LINE('PRT - '||IX.TABLE_NAME);
               DBMS_OUTPUT.PUT_LINE('Tabela: ' || IX.TABLE_NAME || ', Owner: ' || IX.TABLE_OWNER || ' está com índices inválidos ou indisponíveis. 3');
               PRC_ATIVA_INDICE(IX.TABLE_OWNER, IX.TABLE_NAME);
            END LOOP;
            --
         END;
         --
         --
         -- REGISTRA ERRO NO FIM DE PROCESSO
         PRC_CTRL_PROCESSO ( PNM_PROCESSO        => VG_PROCESSO
                           , PDT_INI_EXECUCAO    => VG_DTINICIO
                           , PNM_AGENDA          => P_NM_TABELA_ORIGINAL
                           , PST_STATUS_EXECUCAO => 'FINALIZADO COM ERRO'
                           , PDT_FIM_EXECUCAO    => SYSDATE
                           , PDS_PASSO           => NULL
                           , PDS_OBS             => VG_OBS );
         RAISE_APPLICATION_ERROR(-20999, 'PRC_POS_LOAD: '||VG_ERRO);
   END PRC_POS_LOAD;
   --
   -- ----------------------------------------------------------------------------------------
   -- Esta procedure tem como objetivo Transferir os dados da tabela TMP para a tabela DESTINO. As tabelas DESTINO e TMP deve ser identicas.
   PROCEDURE PRC_BUSCA_PARTICAO ( P_NM_TABELA_ORIGINAL   IN VARCHAR2 ) IS
      V_CAMPO_PART         VARCHAR2(30); -- Campo utilizado para fazer particionamento
      V_TIPO_PART          VARCHAR2(30); -- Tipo de particionamento LIST ou HASH
      V_TIPO_SUBPART       VARCHAR2(30); -- Tipo de particionamento LIST ou HASH
      V_TABELA_TMP         VARCHAR2(30); -- Tabela com os dados temporarios - para particionadas todos os dados cairao nesta tabela
      V_TABELA_BKP         VARCHAR2(30); -- Tabela servira de backup dos dados, primcipalmente quando houver muitas particoes
      V_TABELA_FUL         VARCHAR2(30); -- Tabela que sera utilizada para a troca dos dados ate completar a carga full de uma tabela
      V_OWNER_ORI          VARCHAR2(30); -- Owner tabela original
      V_OWNER_TMP          VARCHAR2(30); -- Owner tabela temporaria, full e backup
      V_QTDE_PART          NUMBER; -- Quanditdade de particoes
      -- CURSOR PARA SELECIONAR DADOS DAS TABELAS TEMPORARIAS
      CURSOR CS_TABS IS
         SELECT a.CAMPO_PART, a.TIPO_PART, a.QTDE_PART, a.OWNER_ORI, a.TABELA_TMP, a.OWNER_TMP, a.TABELA_BKP, a.TABELA_FUL, a.TIPO_SUB_PART--, ATIVO_PROCESSO, TIPO_SUB_PART, QTDE_SUB_PART
            FROM DW_CONTROLE_TABELA_PART a
            INNER JOIN ALL_SYNONYMS B 
                ON a.TABELA_ORI = b.SYNONYM_NAME 
           WHERE a.TABELA_ORI = P_NM_TABELA_ORIGINAL
             AND a.ATIVO_PROCESSO = 2
             AND a.ATIVO_POS_LOAD = 2;
      -- VARIAVEIS CURSOR DINAMICO
      TYPE RCURSORTB   IS REF CURSOR; -- REFERENCIA PARA CURSOR DINAMICO
      VCURSORTB        RCURSORTB; -- VARIAVEL PARA CURSOR DINAMICO
      VQUERYTB         VARCHAR2(4000); -- VARIAVEL PARA QUERY/STRING GENERICA
      VCAMPO           varchar2(30);
   --
   BEGIN
      VG_OBS := VG_OBS||CHR(10)||'BUSCA PARTICAO - '||P_NM_TABELA_ORIGINAL||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
      --
      OPEN CS_TABS;
      LOOP
         FETCH CS_TABS INTO V_CAMPO_PART, V_TIPO_PART, V_QTDE_PART, V_OWNER_ORI, V_TABELA_TMP, V_OWNER_TMP, V_TABELA_BKP, V_TABELA_FUL, V_TIPO_SUBPART;
         EXIT WHEN CS_TABS%NOTFOUND;
         --
         -- VALIDA QUANTIDADE DE PARTICOES
         IF V_QTDE_PART > 1 THEN
            -- BUSCA PARTICAO MULTIPLA --
            --
            BEGIN
               VQUERYTB := 'SELECT /*+ PARALLEL 16*/ '||V_CAMPO_PART||' FROM '||V_OWNER_TMP||'.'||V_TABELA_TMP||' GROUP BY '||V_CAMPO_PART;
               --
               OPEN VCURSORTB FOR VQUERYTB;
               LOOP
                  FETCH VCURSORTB INTO VCAMPO;
                  EXIT WHEN VCURSORTB%NOTFOUND;
                  --
                  -- limpa a tabela FUL
                  PRC_TRUNCA_TEMP(V_OWNER_TMP, V_TABELA_FUL);
                  VQUERYTB := 'insert /*+ append*/ into '||V_OWNER_TMP||'.'||V_TABELA_FUL||' select /*+ parallel 10*/ * from '||V_OWNER_TMP||'.'||V_TABELA_TMP||
                              ' where '||V_CAMPO_PART||' = '''||VCAMPO||'''';
                  --
                  execute immediate(VQUERYTB);
                  commit;
                  --
                  -- Busca as particoes da tabela para o exchange partition
                  for ppart in (WITH XML AS
                                    ( SELECT DBMS_XMLGEN.GETXMLTYPE(
                                         'SELECT TABLE_NAME, PARTITION_NAME, HIGH_VALUE, PARTITION_POSITION, COMPRESSION, NUM_ROWS '||
                                         ' FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = '''||P_NM_TABELA_ORIGINAL||'''') AS PRT
                                      FROM DUAL )
                                    SELECT EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/TABLE_NAME')                  TABLE_NAME,
                                           EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_NAME')              PARTITION,
                                           EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_POSITION')          POSICAO,
                                           EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/NUM_ROWS')                    LINHAS,
                                           EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/COMPRESSION')                 COMPRESSAO,
                                           REPLACE(EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/HIGH_VALUE'),'''','') VALOR
                                    FROM XML PRT, TABLE(XMLSEQUENCE(EXTRACT(PRT.PRT, '/ROWSET/ROW'))) RWS
                                    ORDER BY POSICAO )
                  loop
                     -- verifica valor da particao --
                     if ppart.valor = VCAMPO then
                        VG_SQL := 'ALTER TABLE '||V_OWNER_ORI||'.'||P_NM_TABELA_ORIGINAL||' EXCHANGE PARTITION '||ppart.partition||
                                  ' WITH TABLE '||V_OWNER_TMP||'.'||V_TABELA_FUL||' EXCLUDING INDEXES WITHOUT VALIDATION';
                        -- Faz a troca de particao
                        --DBMS_OUTPUT.PUT_LINE('PRC_BUSCA_PARTICAO TMP -> '||VG_SQL);
                        EXECUTE IMMEDIATE(VG_SQL);
                     end if;
                  end loop;
                  --
                  -- faz backup dos dados anteriores da tabela --
                  for ppart in (WITH XML AS
                                   ( SELECT DBMS_XMLGEN.GETXMLTYPE(
                                       'SELECT TABLE_NAME, PARTITION_NAME, HIGH_VALUE, PARTITION_POSITION, COMPRESSION, NUM_ROWS '||
                                        ' FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER = '''||V_OWNER_TMP||''' AND TABLE_NAME = '''||V_TABELA_BKP||'''') AS PRT
                                     FROM DUAL )
                                   SELECT EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/TABLE_NAME')                  TABLE_NAME,
                                          EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_NAME')              PARTITION,
                                          EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_POSITION')          POSICAO,
                                          EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/NUM_ROWS')                    LINHAS,
                                          EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/COMPRESSION')                 COMPRESSAO,
                                          REPLACE(EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/HIGH_VALUE'),'''','') VALOR
                                   FROM XML PRT, TABLE(XMLSEQUENCE(EXTRACT(PRT.PRT, '/ROWSET/ROW'))) RWS
                                   ORDER BY POSICAO )
                  loop
                     -- verifica valor da particao --
                     if ppart.valor = VCAMPO then
                        VG_SQL := 'ALTER TABLE '||V_OWNER_TMP||'.'||V_TABELA_BKP||' EXCHANGE PARTITION '||ppart.partition||
                                  ' WITH TABLE '||V_OWNER_TMP||'.'||V_TABELA_FUL||' EXCLUDING INDEXES WITHOUT VALIDATION';
                        -- Faz a troca de particao
                        EXECUTE IMMEDIATE(VG_SQL);
                     end if;
                  end loop;
                  --
                  --
               END LOOP;-- varrer tabela FUL e jogar o registro na TMP depois rodar o processo abaixo
               CLOSE VCURSORTB;
            EXCEPTION
               WHEN OTHERS THEN
                  --VCAMPO := NULL; -- EVITA ERRO CASO SEJA SELECIONADO NOME INVALIDO.
                  --DBMS_OUTPUT.PUT_LINE('---------ERRO----------------'||SQLERRM);
                  CLOSE VCURSORTB;
                  RAISE_APPLICATION_ERROR(-20999, 'ERRO: Ao copiar a partiÃ§. SQLERRM: '||SUBSTR(SQLERRM,1,100));
            END;
            -- FIM BUSCA PARTICAO MULTIPLA --
         ELSE -- Inicio de particao unica - CD_PARTITION
            --
            -- VERIFICA SE ALGUMA CÃ¿IA PRECISA SER FEITA - TEMPORARIAMENTE FUNCIONANDO PRA TABELA DE POSSE_ATUAL
            IF V_QTDE_PART = 1 AND V_TIPO_SUBPART = 'HASH' AND V_TABELA_FUL IS NOT NULL THEN
               -- limpa a tabela FUL
               PRC_TRUNCA_TEMP(V_OWNER_TMP, V_TABELA_FUL);
               VQUERYTB := 'insert /*+ append*/ into '||V_OWNER_TMP||'.'||V_TABELA_FUL||' select /*+ parallel 10*/ * from '||V_OWNER_TMP||'.'||V_TABELA_TMP;
               --
               execute immediate(VQUERYTB);
               commit;
               --
               -- Busca as particoes da tabela para o exchange partition
               for ppart in (WITH XML AS
                                 ( SELECT DBMS_XMLGEN.GETXMLTYPE(
                                      'SELECT TABLE_NAME, PARTITION_NAME, HIGH_VALUE, PARTITION_POSITION, COMPRESSION, NUM_ROWS '||
                                      ' FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = '''||P_NM_TABELA_ORIGINAL||'''') AS PRT
                                   FROM DUAL )
                                 SELECT EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/TABLE_NAME')                  TABLE_NAME,
                                        EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_NAME')              PARTITION,
                                        EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_POSITION')          POSICAO,
                                        EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/NUM_ROWS')                    LINHAS,
                                        EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/COMPRESSION')                 COMPRESSAO,
                                        REPLACE(EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/HIGH_VALUE'),'''','') VALOR
                                 FROM XML PRT, TABLE(XMLSEQUENCE(EXTRACT(PRT.PRT, '/ROWSET/ROW'))) RWS
                                 ORDER BY POSICAO )
               loop
                  -- verifica valor da particao --
                  VG_SQL := 'ALTER TABLE '||V_OWNER_ORI||'.'||P_NM_TABELA_ORIGINAL||' EXCHANGE PARTITION '||ppart.partition||
                            ' WITH TABLE '||V_OWNER_TMP||'.'||V_TABELA_FUL||' EXCLUDING INDEXES WITHOUT VALIDATION';
                  -- Faz a troca de particao
                  --DBMS_OUTPUT.PUT_LINE('PRC_BUSCA_PARTICAO TMP -> '||VG_SQL);
                  EXECUTE IMMEDIATE(VG_SQL);
               end loop;
               --
               --
               -- faz backup dos dados anteriores da tabela --
               for ppart in (WITH XML AS
                                ( SELECT DBMS_XMLGEN.GETXMLTYPE(
                                    'SELECT TABLE_NAME, PARTITION_NAME, HIGH_VALUE, PARTITION_POSITION, COMPRESSION, NUM_ROWS '||
                                     ' FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER = '''||V_OWNER_TMP||''' AND TABLE_NAME = '''||V_TABELA_BKP||'''') AS PRT
                                  FROM DUAL )
                                SELECT EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/TABLE_NAME')                  TABLE_NAME,
                                       EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_NAME')              PARTITION,
                                       EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_POSITION')          POSICAO,
                                       EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/NUM_ROWS')                    LINHAS,
                                       EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/COMPRESSION')                 COMPRESSAO,
                                       REPLACE(EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/HIGH_VALUE'),'''','') VALOR
                                FROM XML PRT, TABLE(XMLSEQUENCE(EXTRACT(PRT.PRT, '/ROWSET/ROW'))) RWS
                                ORDER BY POSICAO )
               loop
                  -- verifica valor da particao --
                  VG_SQL := 'ALTER TABLE '||V_OWNER_TMP||'.'||V_TABELA_BKP||' EXCHANGE PARTITION '||ppart.partition||
                            ' WITH TABLE '||V_OWNER_TMP||'.'||V_TABELA_FUL||' EXCLUDING INDEXES WITHOUT VALIDATION';
                           -- ' WITH TABLE '||V_OWNER_TMP||'.'||V_TABELA_FUL||' EXCLUDING INDEXES WITHOUT VALIDATION';
                  -- Faz a troca de particao
                  EXECUTE IMMEDIATE(VG_SQL);
               end loop;
               --
            ELSE
               -- BUSCA PARTICAO UNICA --
               for ppart in (WITH XML AS
                                 ( SELECT DBMS_XMLGEN.GETXMLTYPE(
                                      'SELECT TABLE_NAME, PARTITION_NAME, HIGH_VALUE, PARTITION_POSITION, COMPRESSION, NUM_ROWS '||
                                      ' FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = '''||P_NM_TABELA_ORIGINAL||'''') AS PRT
                                   FROM DUAL )
                                 SELECT EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/TABLE_NAME')                  TABLE_NAME,
                                        EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_NAME')              PARTITION,
                                        EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_POSITION')          POSICAO,
                                        EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/NUM_ROWS')                    LINHAS,
                                        EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/COMPRESSION')                 COMPRESSAO,
                                        REPLACE(EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/HIGH_VALUE'),'''','') VALOR
                                 FROM XML PRT, TABLE(XMLSEQUENCE(EXTRACT(PRT.PRT, '/ROWSET/ROW'))) RWS
                                 ORDER BY POSICAO )
               loop
                     -- Faz a troca dos dados da tabela DESTINO com os dados atuais da _TMP
                     VG_SQL := 'ALTER TABLE '||V_OWNER_ORI||'.'||P_NM_TABELA_ORIGINAL||' EXCHANGE PARTITION '||ppart.partition||
                               ' WITH TABLE '||V_OWNER_TMP||'.'||V_TABELA_TMP||' EXCLUDING INDEXES WITHOUT VALIDATION';
                     -- Faz a troca de particao
                     EXECUTE IMMEDIATE(VG_SQL);
               end loop;
               --
               -- faz backup dos dados anteriores da tabela --
               if V_TABELA_BKP is not null then
                  for ppart in (WITH XML AS
                                ( SELECT DBMS_XMLGEN.GETXMLTYPE(
                                    'SELECT TABLE_NAME, PARTITION_NAME, HIGH_VALUE, PARTITION_POSITION, COMPRESSION, NUM_ROWS '||
                                     ' FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER = '''||V_OWNER_TMP||''' AND TABLE_NAME = '''||V_TABELA_BKP||'''') AS PRT
                                  FROM DUAL )
                                SELECT EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/TABLE_NAME')                  TABLE_NAME,
                                       EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_NAME')              PARTITION,
                                       EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/PARTITION_POSITION')          POSICAO,
                                       EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/NUM_ROWS')                    LINHAS,
                                       EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/COMPRESSION')                 COMPRESSAO,
                                       REPLACE(EXTRACTVALUE(RWS.OBJECT_VALUE, '/ROW/HIGH_VALUE'),'''','') VALOR
                                FROM XML PRT, TABLE(XMLSEQUENCE(EXTRACT(PRT.PRT, '/ROWSET/ROW'))) RWS
                                ORDER BY POSICAO )
                  loop
                     -- Faz a troca dos dados da tabela DESTINO com os dados atuais da _TMP
                     VG_SQL := 'ALTER TABLE '||V_OWNER_TMP||'.'||V_TABELA_BKP||' EXCHANGE PARTITION '||ppart.partition||
                               ' WITH TABLE '||V_OWNER_TMP||'.'||V_TABELA_TMP||' EXCLUDING INDEXES WITHOUT VALIDATION';
                     -- Faz a troca de particao
                     EXECUTE IMMEDIATE(VG_SQL);
                  end loop;
               end if; -- valida se hÃ¡abela BKP
            END IF;
         END IF; -- FIM VALIDA QUANTIDADE DE PARTICOES
      END LOOP;
      --
      CLOSE CS_TABS;
   EXCEPTION
      WHEN OTHERS THEN
         CLOSE CS_TABS;
         RAISE_APPLICATION_ERROR(-20999, 'PRC_BUSCA_PARTICAO: '||SQLERRM);
   END PRC_BUSCA_PARTICAO;
   --
   -- ----------------------------------------------------------------------------------------
   -- Esta procedure tem como objetivo Transferir os dados da tabela TMP para a tabela DESTINO. As tabelas DESTINO e TMP deve ser identicas.
   PROCEDURE PRC_ATIVA_INDICE ( P_NM_OWNER_TEMP    IN VARCHAR2
                              , P_NM_TABELA_TEMP   IN VARCHAR2 ) IS
      -- CURSOR PARA SELECIONAR INDICES DA TABELA
      CURSOR CS_INDICES_ATV IS
         SELECT INDEX_NAME, OWNER, DEGREE
            FROM ALL_INDEXES
            WHERE TABLE_NAME = P_NM_TABELA_TEMP
              AND TABLE_OWNER = P_NM_OWNER_TEMP
              AND STATUS IN ('UNUSABLE', 'N/A')
             ;
      V_NM_INDICE VARCHAR2(30);
      V_NM_OWNER  VARCHAR2(30);
      V_PARALLEL  NUMBER;
   BEGIN
      -- VERIFICA SE HA INDICES E TORNA-OS INATIVOS
      VG_ExSQL := NULL;
      VG_SQL := NULL;
      --
      OPEN CS_INDICES_ATV;
      LOOP
         FETCH CS_INDICES_ATV INTO V_NM_INDICE, V_NM_OWNER, V_PARALLEL;
         EXIT WHEN CS_INDICES_ATV%NOTFOUND;
         VG_OBS := VG_OBS||CHR(10)||'RECONSTROI INDICE INVALIDO - '||V_NM_INDICE||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
         VG_ExSQL := 'ALTER INDEX ' || V_NM_OWNER||'.'||V_NM_INDICE || ' REBUILD PARALLEL 12';
         DBMS_OUTPUT.PUT_LINE('PONTO DE CONTROLE ATIVA INDICE 1');
         --VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- forma correta
         --
         -- EXECUTA O COMMANDO
         EXECUTE IMMEDIATE (VG_ExSQL);
        -- VOLTA O ESTADO ORIGINAL DE PARALLEL DO INDEX
         VG_ExSQL := 'ALTER INDEX ' || V_NM_OWNER||'.'||V_NM_INDICE || ' PARALLEL '||V_PARALLEL;
         DBMS_OUTPUT.PUT_LINE('PONTO DE CONTROLE ATIVA INDICE 2');
         --VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- forma correta
         -- EXECUTA O COMMANDO
         EXECUTE IMMEDIATE (VG_ExSQL);
      END LOOP;
      CLOSE CS_INDICES_ATV;
      --
      -- VERIFICA INDICES PARTICIONADOS INVALIDOS --
      FOR IP IN (SELECT 'ALTER INDEX '|| P.INDEX_OWNER ||'.'|| P.INDEX_NAME ||' REBUILD PARTITION ' || P.PARTITION_NAME ||'' QUERYS
                      , P.INDEX_NAME, P.INDEX_OWNER, P.PARTITION_NAME
                   FROM ALL_IND_PARTITIONS P
                      , ALL_INDEXES        I
                  WHERE I.TABLE_NAME  = P_NM_TABELA_TEMP
                    AND I.TABLE_OWNER = P_NM_OWNER_TEMP
                    AND P.INDEX_OWNER = I.OWNER
                    AND P.INDEX_NAME  = I.INDEX_NAME
                    AND P.STATUS IN ('UNUSABLE', 'N/A')
                ) LOOP
        DBMS_OUTPUT.PUT_LINE('Reconstruindo índice: ' || IP.INDEX_NAME || ', Partição: ' || IP.PARTITION_NAME ||'este ponto eh na ativa indice');
         VG_OBS := VG_OBS||CHR(10)||'RECONSTROI INDICE PARTICAO INVALIDO - '||IP.INDEX_NAME||' - PART '||IP.PARTITION_NAME||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
         VG_ExSQL := IP.QUERYS;
         --VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- forma correta
         --
         -- EXECUTA O COMMANDO
         EXECUTE IMMEDIATE (VG_ExSQL);
      END LOOP;
      --
      -- AJUSTA INDICES (SUBPARTICAO) DE POSSE E CONVIVENCIA --
      IF P_NM_TABELA_TEMP = 'NBA_RTD_POSSE_ATUAL' THEN
         VG_ExSQL := 'DROP INDEX SASDM_O.NBA_RTD_POSSE_ATUAL_IDX';
         VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- EXCLUI INDICE
         EXECUTE IMMEDIATE (VG_ExSQL);
         VG_ExSQL := 'CREATE INDEX SASDM_O.NBA_RTD_POSSE_ATUAL_IDX ON SASDM_O.NBA_RTD_POSSE_ATUAL (NUM_CONTRATO, COD_OPERADORA) INITRANS 16 NOLOGGING   TABLESPACE TBS_RTDPRD0_DAT       LOCAL PARALLEL 12';
         VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- RECRIA INDICE
         EXECUTE IMMEDIATE (VG_ExSQL);
         VG_ExSQL := 'ALTER INDEX SASDM_O.NBA_RTD_POSSE_ATUAL_IDX NOPARALLEL';
         VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- TIRA O PARALLEL
         EXECUTE IMMEDIATE (VG_ExSQL);
         VG_OBS := VG_OBS||CHR(10)||'RECONSTROI INDICE INVALIDO - '||'NBA_RTD_POSSE_ATUAL_IDX'||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
      ELSIF P_NM_TABELA_TEMP = 'NBA_RTD_CONVIVENCIA_PRODUTO' THEN
         VG_ExSQL := 'DROP INDEX SASDM_O.NBA_RTD_CONV_PRODUTO_IDX';
         VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- EXCLUI INDICE
         EXECUTE IMMEDIATE (VG_ExSQL);
         VG_ExSQL := 'CREATE INDEX SASDM_O.NBA_RTD_CONV_PRODUTO_IDX ON SASDM_O.NBA_RTD_CONVIVENCIA_PRODUTO (COD_BASE, COD_CIDADE, COD_PRODUTO_CONVIVENCIA, COD_PRODUTO) INITRANS 16 NOLOGGING TABLESPACE TBS_RTDPRD0_DAT LOCAL PARALLEL 12';
         VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- RECRIA INDICE
         EXECUTE IMMEDIATE (VG_ExSQL);
         VG_ExSQL := 'ALTER INDEX SASDM_O.NBA_RTD_CONV_PRODUTO_IDX NOPARALLEL';
         VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';  -- TIRA O PARALLEL
         EXECUTE IMMEDIATE (VG_ExSQL);
         VG_OBS := VG_OBS||CHR(10)||'RECONSTROI INDICE INVALIDO - '||'NBA_RTD_CONV_PRODUTO_IDX'||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
      END IF;
      --
      if VG_ExSQL is null then
          VG_OBS := VG_OBS||CHR(10)||'RECONSTROI INDICE INVALIDO - SEM INDICE'||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
      end if;
      --
   EXCEPTION
      WHEN OTHERS THEN
         CLOSE CS_INDICES_ATV;
         RAISE_APPLICATION_ERROR(-20999, 'ERRO PRC_ATIVA_INDICE: '||SQLERRM/*V_SAIDA*/);
   END PRC_ATIVA_INDICE;
   --
   -- ----------------------------------------------------------------------------------------
   -- Esta procedure gera as estatisticas para a tabela selecionada
   PROCEDURE PRC_GERA_ESTATISTICA ( P_NM_OWNER_TEMP    IN VARCHAR2

                                  , P_NM_TABELA_TEMP   IN VARCHAR2 ) IS
   BEGIN
      VG_OBS := VG_OBS||CHR(10)||'GERA ESTATISTICA - '||P_NM_TABELA_TEMP||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
      VG_ExSQL := 'CALL DBMS_STATS.GATHER_TABLE_STATS('||VG_DASPA||P_NM_OWNER_TEMP||VG_DASPA||','||VG_DASPA||P_NM_TABELA_TEMP||VG_DASPA||',DEGREE=>24)';
      VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';
      --dbms_output.put_line('PRC_GERA_ESTATISTICA -> '||VG_SQL);
      EXECUTE IMMEDIATE (VG_ExSQL);
   EXCEPTION
      WHEN OTHERS THEN
         -- TENTA NOVAMENTE COM DEGREE = 12
         VG_ExSQL := 'CALL DBMS_STATS.GATHER_TABLE_STATS('||VG_DASPA||P_NM_OWNER_TEMP||VG_DASPA||','||VG_DASPA||P_NM_TABELA_TEMP||VG_DASPA||',DEGREE=>12)';
         VG_SQL := 'CALL '||P_NM_OWNER_TEMP||'.SAFE_EXECUTE('||VG_ASPA||VG_ExSQL||VG_ASPA||')';
         EXECUTE IMMEDIATE (VG_ExSQL);
   END PRC_GERA_ESTATISTICA;
   --
   -- ----------------------------------------------------------------------------------------
   -- Esta procedure faz o exchange partition da tabela
   PROCEDURE PRC_EXCHANGE_TABELA ( P_NM_TABELA_ORIGINAL  IN VARCHAR2 ) IS
      V_TAB_TEMP            VARCHAR2(30); -- NOME DA TABELA A SER TRUNCADA
      V_OWNER_TEMP          VARCHAR2(30); -- NOME DO OWNER DA TABELA A SER TRUNCADA
      -- CURSOR PARA SELECIONAR DADOS DAS TABELAS TEMPORARIAS
      CURSOR CS_TABS_EXC IS
         SELECT A.TABELA_TMP, A.OWNER_TMP--, TABELA_FUL, TABELA_BKP, OWNER_ORI, ATIVO_PROCESSO, CAMPO_PART, TIPO_PART, TIPO_SUB_PART, QTDE_PART, QTDE_SUB_PART
            FROM DW_CONTROLE_TABELA_PART a
            INNER JOIN ALL_SYNONYMS B 
                ON a.TABELA_ORI = b.SYNONYM_NAME 
           WHERE a.TABELA_ORI = P_NM_TABELA_ORIGINAL
             AND a.ATIVO_PROCESSO = 2
             AND a.ATIVO_POS_LOAD = 2;
   --
   BEGIN
      VG_OBS := VG_OBS||CHR(10)||'EXCHANGE TABELA - '||P_NM_TABELA_ORIGINAL||' -> '||TO_CHAR(SYSDATE,'HH24:MI:SS');
      --
      OPEN CS_TABS_EXC;
      LOOP
         --V_NR_ERRO := 500;
         FETCH CS_TABS_EXC INTO V_TAB_TEMP, V_OWNER_TEMP;
         EXIT WHEN CS_TABS_EXC%NOTFOUND;
         --DBMS_OUTPUT.PUT_LINE('PRC_POS_LOAD -> '||V_OWNER_TEMP||'.'||V_TAB_TEMP);
         -- VERIFICA SE HA INDICES NA TEMP E TORNA-OS ATIVOS
         PRC_ATIVA_INDICE(V_OWNER_TEMP, V_TAB_TEMP);
         -- GERA ESTATISTICAS PARA A TABELA TEMP
         PRC_GERA_ESTATISTICA(V_OWNER_TEMP, V_TAB_TEMP);
         --PRC_TRUNCA_TEMP(V_OWNER_TEMP, V_TAB_TEMP);
      END LOOP;
      --
      --V_NR_ERRO := 700;
      CLOSE CS_TABS_EXC;
   EXCEPTION
      WHEN OTHERS THEN
         CLOSE CS_TABS_EXC;
         RAISE_APPLICATION_ERROR(-20999, 'ERRO PRC_EXCHANGE_TABELA: '||SQLERRM);
   END PRC_EXCHANGE_TABELA;
   --
   -- ----------------------------------------------------------------------------------------
   PROCEDURE PRC_CTRL_PROCESSO
   -- --------------------------------------------------------------------------------------- --
   -- Objetivo: Controlar as execuÃ§s efetuadas via chamadas de procedures, packages, etc.
   -- --------------------------------------------------------------------------------------- --
   ( PNM_PROCESSO         IN VARCHAR2
   , PNM_OWNER            IN VARCHAR2 DEFAULT USER
   , PNM_TIPO             IN VARCHAR2 DEFAULT 'PROCEDURE'
   , PNM_AGENDA           IN VARCHAR2 DEFAULT NULL
   , PDT_INI_EXECUCAO     IN DATE
   , PDT_FIM_EXECUCAO     IN DATE     DEFAULT NULL
   , PST_STATUS_EXECUCAO  IN VARCHAR2 DEFAULT NULL
   , PDS_PASSO            IN VARCHAR2
   , PDS_SAIDA_01         IN VARCHAR2 DEFAULT NULL
   , PDS_SAIDA_02         IN VARCHAR2 DEFAULT NULL
   , PDS_SAIDA_03         IN VARCHAR2 DEFAULT NULL
   , PDS_SAIDA_04         IN VARCHAR2 DEFAULT NULL
   , PDS_SAIDA_05         IN VARCHAR2 DEFAULT NULL
   , PDS_SAIDA_06         IN VARCHAR2 DEFAULT NULL
   , PDS_SAIDA_07         IN VARCHAR2 DEFAULT NULL
   , PDS_SAIDA_08         IN VARCHAR2 DEFAULT NULL
   , PDS_SAIDA_09         IN VARCHAR2 DEFAULT NULL
   , PDS_OBS              IN VARCHAR2 DEFAULT NULL
   ) IS
     VEXISTE NUMBER := 0;
   BEGIN
      --
      BEGIN
         SELECT 1 INTO VEXISTE FROM SASDM_O.DW_CONTROLE_PROCESSO
            WHERE NM_PROCESSO = PNM_PROCESSO
               AND DT_INI_EXECUCAO = PDT_INI_EXECUCAO
               AND NM_AGENDA = PNM_AGENDA
               AND ROWNUM < 2;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            VEXISTE := 0;
      END;
      --
      IF VEXISTE > 0 THEN
         BEGIN
            --dbms_output.put_line('-- update - '||PNM_PROCESSO||' - '||PDT_INI_EXECUCAO);
            UPDATE SASDM_O.DW_CONTROLE_PROCESSO
               SET NM_OWNER           = NVL(NM_OWNER, PNM_OWNER)
                 , NM_TIPO            = NVL(NM_TIPO, PNM_TIPO)
                -- , NM_AGENDA          = NVL(NM_AGENDA, PNM_AGENDA)
                 , DT_FIM_EXECUCAO    = NVL(DT_FIM_EXECUCAO, PDT_FIM_EXECUCAO)
                 , ST_STATUS_EXECUCAO = NVL(PST_STATUS_EXECUCAO, ST_STATUS_EXECUCAO) -- DIFERENTE
                 , DS_PASSO           = NVL(PDS_PASSO, DS_PASSO) -- DIFERENTE
                 , DS_SAIDA_01        = NVL(PDS_SAIDA_01, DS_SAIDA_01) -- DIFERENTE
                 , DS_SAIDA_02        = NVL(PDS_SAIDA_02, DS_SAIDA_02) -- DIFERENTE
                 , DS_SAIDA_03        = NVL(DS_SAIDA_03, PDS_SAIDA_03)
                 , DS_SAIDA_04        = NVL(DS_SAIDA_04, PDS_SAIDA_04)
                 , DS_SAIDA_05        = NVL(DS_SAIDA_05, PDS_SAIDA_05)
                 , DS_SAIDA_06        = NVL(DS_SAIDA_06, PDS_SAIDA_06)
                 , DS_SAIDA_07        = NVL(DS_SAIDA_07, PDS_SAIDA_07)
                 , DS_SAIDA_08        = NVL(DS_SAIDA_08, PDS_SAIDA_08)
                 , DS_SAIDA_09        = NVL(DS_SAIDA_09, PDS_SAIDA_09)
                 , DS_OBS             = NVL(PDS_OBS, DS_OBS) -- DIFERENTE
               WHERE NM_PROCESSO     = PNM_PROCESSO
                 AND DT_INI_EXECUCAO = PDT_INI_EXECUCAO
                 AND NM_AGENDA       = PNM_AGENDA;
         END;
      ELSE
         BEGIN
            --dbms_output.put_line('-- insert - '||PNM_PROCESSO||' - '||PDT_INI_EXECUCAO);
            INSERT INTO SASDM_O.DW_CONTROLE_PROCESSO
               ( NM_PROCESSO, NM_OWNER, NM_TIPO, NM_AGENDA, DT_INI_EXECUCAO, DT_FIM_EXECUCAO, ST_STATUS_EXECUCAO, DS_PASSO
               , DS_SAIDA_01, DS_SAIDA_02, DS_SAIDA_03, DS_SAIDA_04, DS_SAIDA_05, DS_SAIDA_06, DS_SAIDA_07, DS_SAIDA_08
               , DS_SAIDA_09, DS_OBS )
               VALUES
               ( PNM_PROCESSO, PNM_OWNER, PNM_TIPO, PNM_AGENDA, PDT_INI_EXECUCAO, PDT_FIM_EXECUCAO, PST_STATUS_EXECUCAO, PDS_PASSO
               , PDS_SAIDA_01, PDS_SAIDA_02, PDS_SAIDA_03, PDS_SAIDA_04, PDS_SAIDA_05, PDS_SAIDA_06, PDS_SAIDA_07, PDS_SAIDA_08
               , PDS_SAIDA_09, PDS_OBS );
         END;
      END IF;
      --
      COMMIT;
   END PRC_CTRL_PROCESSO;
   --
END PKG_LOAD_SYNONYM;