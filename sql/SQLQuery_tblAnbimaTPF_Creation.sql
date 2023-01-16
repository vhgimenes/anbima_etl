CREATE TABLE [dbo].[tblAnbimaTPF](
	[TIPO] [varchar](255) NOT NULL,
	[ID] [varchar](255) NOT NULL,
	[REF_DATE] [varchar](10) NOT NULL,
	[COD_SELIC] [varchar](255) NOT NULL,
	[DATA_EMISSAO] [varchar](10) NOT NULL,
	[DATA_VENC] [varchar](10) NOT NULL,
    [TX_MAX] [float] NOT NULL,
    [TX_MIN] [float] NOT NULL,
    [TX_IND] [float] NOT NULL,
	[PU] [float] NOT NULL,
	[DESV_PAD] [float] NOT NULL,
    CONSTRAINT PK_TPF PRIMARY KEY (ID,REF_DATE)
) 
