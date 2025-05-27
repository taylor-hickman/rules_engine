## RULE 7

```sql
Replace PROCEDURE HSLABCORNERSTONE.RULE_7_Spayer_NPI_SPECIALTIES ()
SQL SECURITY INVOKER
MAIN: BEGIN
DELETE FROM HSLABCORNERSTONE.RULE_7_Spayer_NPI_SPECIALTIES_TB;
INSERT INTO HSLABCORNERSTONE.RULE_7_Spayer_NPI_SPECIALTIES_TB
SELECT
A.NPI,
D.SpecialtyName
FROM HSLABCORNERSTONE.NPI_universe_20250506 A
JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_practitioners B
ON A.NPI = B.NationalProviderID
JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONERSPECIALTIES C
ON B.PractitionerID = C.PractitionerID
JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_SPECIALTIES D
ON D.SpecialtyId = C.SpecialtyId
WHERE TRIM(CONCAT(A.NPI, '-', D.SpecialtyName)) IN (
SELECT  DISTINCT TRIM(CONCAT(p.NationalProviderID, '-', sp.SpecialtyName))
                          FROM   PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_practitioners p
                                 JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONERSPECIALTIES PRSP
                                   ON p.PractitionerID = PRSP.PractitionerID
                                 JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONEREDUCATION PE
                                   ON PRSP.PractitionerID = PE.PractitionerID
                                 LEFT JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_DEGREES DEG ON PE.DegreeID = DEG.DegreeID
                                 JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONERPRODUCTS PRPROD
                                   ON PRSP.PractitionerID = PRPROD.PractitionerID
                                 JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONERPRODUCTSPECIALTIES PRPRODSP
                                   ON PRPRODSP.PractitionerProductRecID = PRPROD.PractitionerProductRecID
                                 left join PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_SPECIALTIES SP on
                                   SP.SpecialtyID = PRSP.SpecialtyID
                          WHERE  ((PRSP.SpecialtyId = 1244642263 AND (DEG.DegreeCode NOT IN ('DO', 'MD') OR DEG.DegreeCode IS NULL))
                          OR (PRSP.SpecialtyId = 1244642264 AND (DEG.DegreeCode NOT IN ('CP', 'LP', 'LCCP', 'LCP', 'MD', 'MPAP') 
                          OR DEG.DegreeCode IS NULL)) OR (PRSP.SpecialtyId = 1244642200 AND (DEG.DegreeCode NOT IN ('LCSW', 'LCSWC', 'LCSWR', 'LICSW', 'LISW', 'LISWAP', 'LISWCP', 'LMSW', 'LSCSW', 'LCSWPIP') 
                          OR DEG.DegreeCode IS NULL)) OR (PRSP.SpecialtyId = 1244642201 AND (DEG.DegreeCode NOT IN ('LMFT', 'IMFT', 'LCMFT') 
                          OR DEG.DegreeCode IS NULL)) OR (PRSP.SpecialtyId = 1244642203 AND (DEG.DegreeCode 
                          NOT IN ('LMHC', 'LCPC', 'LPC', 'LPCC', 'LCMHC', 'LPCMH', 'LPCMHSP') OR DEG.DegreeCode IS NULL))
                          OR (
            DEG.DegreeCode IN ('CRNP','FNP','CNP','APNP','PMHNP','NP','LNP','CNS','ARNP','CNSPMH','ANP','ACNP','PA','PA_C','APN','APPN','APRN','APRNCNP','APRNCNS','APRNPMH','CRNPPMH','PMHCNS')
            AND EXISTS (
                SELECT 1
                FROM PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONERPRODUCTS prprd
                LEFT JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRODUCTS prd 
                    ON prprd.ProductID = prd.ProductID
                WHERE prprd.PractitionerID = PRSP.PractitionerID 
                AND (prd.ProductCode NOT IN ('683FZ','554UQ') OR prd.ProductCode IS NULL)
            )
        ))
                          and trim(CONCAT(COALESCE(p.NationalProviderID, ''), '-', COALESCE(PRSP.SpecialtyID, ''))) NOT IN                        
(select  distinct trim(CONCAT(COALESCE(c.NationalProviderID, ''), '-', COALESCE(d.SpecialtyId, '')))
FROM   PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_DEGREES a
       left join PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONEREDUCATION b on a.degreeId = b.degreeId
       left join PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_practitioners c on b.practitionerId = c.practitionerId
       left join PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONERSPECIALTIES d on d.practitionerId = c.practitionerId
where  (((d.SpecialtyId = 1244642263) AND (a.DegreeCode  IN ('DO', 'MD') OR a.DegreeCode IS NULL)
       ) OR ((d.SpecialtyId = 1244642264) AND (a.DegreeCode  IN ('CP', 'LP', 'LCCP', 'LCP', 'MD', 'MPAP') OR a.DegreeCode IS NULL)
       ) OR ((d.SpecialtyId = 1244642200) AND (a.DegreeCode IN ('LCSW', 'LCSWC', 'LCSWR', 'LICSW', 'LISW', 'LISWAP', 'LISWCP', 'LMSW', 'LSCSW', 'LCSWPIP') OR a.DegreeCode IS NULL)
       ) OR ((d.SpecialtyId = 1244642201) AND (a.DegreeCode  IN ('LMFT', 'IMFT', 'LCMFT') OR a.DegreeCode IS NULL)
       ) OR ((d.SpecialtyId = 1244642203) AND (a.DegreeCode  IN ('LMHC', 'LCPC', 'LPC', 'LPCC', 'LCMHC', 'LPCMH', 'LPCMHSP') OR a.DegreeCode IS NULL)
       ) OR (a.DegreeCode  IN
         ('CRNP', 'FNP', 'CNP', 'APNP', 'PMHNP', 'NP', 'LNP',
       'CNS', 'ARNP', 'CNSPMH', 'ANP', 'ACNP', 'PA', 'PA_C',
       'APN', 'APPN', 'APRN', 'APRNCNP', 'APRNCNS', 'APRNPMH', 'CRNPPMH',
       'PMHCNS')  AND  EXISTS (
                SELECT 1
                FROM PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONERPRODUCTS prprd
                LEFT JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRODUCTS prd 
                    ON prprd.ProductID = prd.ProductID
                WHERE prprd.PractitionerID = d.PractitionerID 
                AND (prd.ProductCode IN ('683FZ','554UQ'))
            ))))
       ) ;
end;
```