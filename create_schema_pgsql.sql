-- Limpeza inicial
DROP TABLE IF EXISTS informe_diario;
DROP TABLE IF EXISTS dados_cadastrais;

-- 1. Tabela Dados Cadastrais
CREATE TABLE IF NOT EXISTS dados_cadastrais (
    "TP_FUNDO" VARCHAR(20) NOT NULL,
    "COD_CNPJ" VARCHAR(50) NOT NULL,
    "CNPJ_FUNDO" VARCHAR(50) NOT NULL,     
    "DENOM_SOCIAL" VARCHAR(400),     
    "DT_REG" TEXT,     
    "DT_CONST" TEXT,
    "CD_CVM" NUMERIC(7,0),
    "DT_CANCEL" TEXT,     
    "SIT" VARCHAR(40),
    "DT_INI_SIT" TEXT,     
    "DT_INI_ATIV" TEXT,     
    "DT_INI_EXERC" TEXT,     
    "DT_FIM_EXERC" TEXT,     
    "CLASSE" VARCHAR(100),     
    "DT_INI_CLASSE" TEXT,     
    "RENTAB_FUNDO" TEXT,
    "CONDOM" TEXT,     
    "FUNDO_COTAS" TEXT,     
    "FUNDO_EXCLUSIVO" TEXT,     
    "TRIB_LPRAZO" TEXT,     
    "INVEST_QUALIF" TEXT,     
    "ENTID_INVEST" VARCHAR(1),
    "TAXA_PERFM" TEXT,     
    "INF_TAXA_PERFM" TEXT,     
    "TAXA_ADM" TEXT,     
    "INF_TAXA_ADM" TEXT,     
    "VL_PATRIM_LIQ" TEXT,     
    "DT_PATRIM_LIQ" TEXT,     
    "DIRETOR" TEXT,     
    "CNPJ_ADMIN" VARCHAR(50),     
    "ADMIN" TEXT,     
    "PF_PJ_GESTOR" TEXT,     
    "CPF_CNPJ_GESTOR" VARCHAR(50),     
    "GESTOR" TEXT,     
    "CNPJ_AUDITOR" TEXT,     
    "AUDITOR" TEXT,     
    "CNPJ_CUSTODIANTE" TEXT,     
    "CUSTODIANTE" TEXT,     
    "CNPJ_CONTROLADOR" VARCHAR(50),     
    "CONTROLADOR" TEXT,
    PRIMARY KEY ("TP_FUNDO", "COD_CNPJ")
);

-- Índices Dados Cadastrais (Removida redundância de TP_FUNDO + COD_CNPJ)
CREATE UNIQUE INDEX idx_dados_cadastrais_01 ON dados_cadastrais ("COD_CNPJ");
CREATE UNIQUE INDEX idx_dados_cadastrais_02 ON dados_cadastrais ("CNPJ_FUNDO");
CREATE INDEX idx_dados_cadastrais_03 ON dados_cadastrais ("DENOM_SOCIAL");
CREATE INDEX idx_dados_cadastrais_04 ON dados_cadastrais ("SIT");
CREATE INDEX idx_dados_cadastrais_05 ON dados_cadastrais ("CLASSE");
CREATE INDEX idx_dados_cadastrais_06 ON dados_cadastrais ("CNPJ_ADMIN");
CREATE INDEX idx_dados_cadastrais_07 ON dados_cadastrais ("CPF_CNPJ_GESTOR");
CREATE INDEX idx_dados_cadastrais_08 ON dados_cadastrais ("CNPJ_CONTROLADOR");
CREATE INDEX idx_dados_cadastrais_09 ON dados_cadastrais ("CD_CVM");


-- 2. Tabela Informe Diário
CREATE TABLE IF NOT EXISTS informe_diario (
    "COD_CNPJ" VARCHAR(50) NOT NULL,     
    "CNPJ_FUNDO" VARCHAR(50) NOT NULL,     
    "DT_REF" DATE NOT NULL,     
    "DT_COMPTC" VARCHAR(50) NOT NULL,     
    "VL_TOTAL" NUMERIC(17,2),     
    "VL_QUOTA" NUMERIC(27,12),     
    "VL_PATRIM_LIQ" NUMERIC(17,2),     
    "CAPTC_DIA" NUMERIC(17,2),     
    "RESG_DIA" NUMERIC(17,2),     
    "NR_COTST" INTEGER,
    "ANO_REF" INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM "DT_REF")::INTEGER) STORED,
    "MES_REF" INTEGER GENERATED ALWAYS AS (EXTRACT(MONTH FROM "DT_REF")::INTEGER) STORED,
    PRIMARY KEY ("COD_CNPJ", "DT_REF")
);  

-- Índices Informe Diário (Renumerados e Otimizados)
-- Nota: O índice para COD_CNPJ + DT_REF já existe pela PRIMARY KEY.

CREATE INDEX idx_informe_diario_01 ON informe_diario ("CNPJ_FUNDO");
CREATE INDEX idx_informe_diario_02 ON informe_diario ("DT_REF" DESC); -- Data descendente para buscas recentes
CREATE INDEX idx_informe_diario_03 ON informe_diario ("DT_COMPTC" DESC);
CREATE UNIQUE INDEX idx_informe_diario_04 ON informe_diario ("CNPJ_FUNDO", "DT_REF" DESC);
CREATE INDEX idx_informe_diario_05 ON informe_diario ("ANO_REF" DESC);
CREATE INDEX idx_informe_diario_06 ON informe_diario ("MES_REF" DESC);
CREATE INDEX idx_informe_diario_07 ON informe_diario ("CNPJ_FUNDO", "ANO_REF" DESC, "MES_REF" DESC);
