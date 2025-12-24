CREATE TABLE IF NOT EXISTS dados_cadastrais (
        TP_FUNDO VARCHAR(20) NOT NULL,
	    COD_CNPJ VARCHAR(14) NOT NULL,
        CNPJ_FUNDO VARCHAR(20) NOT NULL, 	
        DENOM_SOCIAL VARCHAR(400), 	
        DT_REG TEXT, 	
        DT_CONST TEXT,
	    CD_CVM NUMERIC(7,0),
        DT_CANCEL TEXT, 	
        SIT VARCHAR(40),
        DT_INI_SIT TEXT, 	
        DT_INI_ATIV TEXT, 	
        DT_INI_EXERC TEXT, 	
        DT_FIM_EXERC TEXT, 	
        CLASSE VARCHAR(100), 	
        DT_INI_CLASSE TEXT, 	
        RENTAB_FUNDO TEXT,
        CONDOM TEXT, 	
        FUNDO_COTAS TEXT, 	
        FUNDO_EXCLUSIVO TEXT, 	
        TRIB_LPRAZO TEXT, 	
        INVEST_QUALIF TEXT, 	
        ENTID_INVEST VARCHAR(1),
	    TAXA_PERFM TEXT, 	
        INF_TAXA_PERFM TEXT, 	
        TAXA_ADM TEXT, 	
        INF_TAXA_ADM TEXT, 	
        VL_PATRIM_LIQ TEXT, 	
        DT_PATRIM_LIQ TEXT, 	
        DIRETOR TEXT, 	
        CNPJ_ADMIN VARCHAR(50), 	
        ADMIN TEXT, 	
        PF_PJ_GESTOR TEXT, 	
        CPF_CNPJ_GESTOR VARCHAR(50), 	
        GESTOR TEXT, 	
        CNPJ_AUDITOR TEXT, 	
        AUDITOR TEXT, 	
        CNPJ_CUSTODIANTE TEXT, 	
        CUSTODIANTE TEXT, 	
        CNPJ_CONTROLADOR VARCHAR(50), 	
        CONTROLADOR TEXT,
        PRIMARY KEY(TP_FUNDO, COD_CNPJ),
        -- Índices de Cadastro
        UNIQUE INDEX idx_dados_cadastrais_01 (COD_CNPJ),
        UNIQUE INDEX idx_dados_cadastrais_02 (CNPJ_FUNDO),
        INDEX idx_dados_cadastrais_03 (DENOM_SOCIAL),
        INDEX idx_dados_cadastrais_04 (SIT),
        INDEX idx_dados_cadastrais_05 (CLASSE),
        INDEX idx_dados_cadastrais_06 (CNPJ_ADMIN),
        INDEX idx_dados_cadastrais_07 (CPF_CNPJ_GESTOR),
        INDEX idx_dados_cadastrais_08 (CNPJ_CONTROLADOR),
        INDEX idx_dados_cadastrais_09 (CD_CVM)
);

CREATE TABLE IF NOT EXISTS informe_diario (
        COD_CNPJ VARCHAR(14) NOT NULL, 	
        CNPJ_FUNDO VARCHAR(20) NOT NULL, 	
        DT_REF DATE NOT NULL, 	
        DT_COMPTC VARCHAR(20) NOT NULL, 	
        VL_TOTAL NUMERIC(20,2), 	
        VL_QUOTA NUMERIC(27,12), 	
        VL_PATRIM_LIQ NUMERIC(17,2), 	
        CAPTC_DIA NUMERIC(17,2), 	
        RESG_DIA NUMERIC(17,2), 	
        NR_COTST INTEGER,
	    ANO_REF INTEGER GENERATED ALWAYS AS (YEAR(DT_REF)) STORED,
	    MES_REF INTEGER GENERATED ALWAYS AS (MONTH(DT_REF)) STORED,
        PRIMARY KEY(COD_CNPJ, DT_REF desc),
        -- Índices de Informe (Otimizados para ordenação e busca)
        INDEX idx_informe_diario_01 (CNPJ_FUNDO),
        INDEX idx_informe_diario_02 (DT_REF DESC),
        INDEX idx_informe_diario_03 (DT_COMPTC),
        UNIQUE INDEX idx_informe_diario_04 (CNPJ_FUNDO, DT_REF DESC),
        INDEX idx_informe_diario_05 (ANO_REF DESC),
        INDEX idx_informe_diario_06 (MES_REF),
        INDEX idx_informe_diario_07 (CNPJ_FUNDO, ANO_REF DESC, MES_REF)	
);

CREATE VIEW ultima_data as select max(d."DT_REF") as DT_REF from informe_diario d;

CREATE VIEW IF NOT EXISTS ultima_quota as 
        select c."COD_CNPJ", c."CNPJ_FUNDO", c."DENOM_SOCIAL", i."DT_REF", i."VL_QUOTA"
        FROM dados_cadastrais c
        inner join informe_diario i on (c."COD_CNPJ"=i."COD_CNPJ")
        where not exists (
            select 1 from informe_diario i2
            where i2."COD_CNPJ"=i."COD_CNPJ"
            and i2."DT_REF" > i."DT_REF"
        );

