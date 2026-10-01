CREATE VIEW dw.dim_local_authority
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://ukhousingstorageadls.dfs.core.windows.net/raw/managed_table/__unitystorage/catalogs/7b605d7d-06ce-4991-a0d5-f5e983177d19/tables/75b920ea-57ba-46e8-868c-9989c9b803b1',
    FORMAT = 'DELTA'
) AS data;
GO


CREATE VIEW dw.fact_house_prices
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://ukhousingstorageadls.dfs.core.windows.net/raw/managed_table/__unitystorage/catalogs/7b605d7d-06ce-4991-a0d5-f5e983177d19/tables/1d49070b-cf0e-40fa-adcc-d4575289038b',
    FORMAT = 'DELTA'
) AS data;
GO

CREATE VIEW dw.fact_homelessness
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://ukhousingstorageadls.dfs.core.windows.net/raw/managed_table/__unitystorage/catalogs/7b605d7d-06ce-4991-a0d5-f5e983177d19/tables/758185c7-a033-4ae1-add5-4bc8de45b7d6',
    FORMAT = 'DELTA'
) AS data;
GO

CREATE VIEW dw.fact_lahs
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://ukhousingstorageadls.dfs.core.windows.net/raw/managed_table/__unitystorage/catalogs/7b605d7d-06ce-4991-a0d5-f5e983177d19/tables/90bb3cae-02ad-4389-a38c-143ba2db0ae5',
    FORMAT = 'DELTA'
) AS data;
GO

CREATE VIEW dw.fact_imd
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://ukhousingstorageadls.dfs.core.windows.net/raw/managed_table/__unitystorage/catalogs/7b605d7d-06ce-4991-a0d5-f5e983177d19/tables/b882b7a0-4362-40ca-b283-e01fc0ea23ce',
    FORMAT = 'DELTA'
) AS data;
GO



SELECT TOP 10 * FROM dw.dim_local_authority;
GO

SELECT TOP 10 * FROM dw.fact_house_prices;
GO

SELECT TOP 10 * FROM dw.fact_homelessness;
GO

SELECT TOP 10 * FROM dw.fact_lahs;
GO

SELECT TOP 10 * FROM dw.fact_imd;
GO



