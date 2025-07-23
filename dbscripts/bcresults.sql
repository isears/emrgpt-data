/*
 Goal schema:
 - hadm_id
 - charttime
 - org_name
 - org_clf (GNR, GPC, Fungus, etc.)
 
 FROM MIMIC IV Docs:
 If no growth is found, the remaining columns will be NULL
 If bacteria is found, then each organism of bacteria will be present in org_name, resulting in multiple rows for the single specimen (i.e. multiple rows for the given spec_type_desc).
 If antibiotics are tested on a given bacterial organism, then each antibiotic tested will be present in the ab_name column (i.e. multiple rows for the given org_name associated with the given spec_type_desc). Antibiotic parameters and sensitivities are present in the remaining columns (dilution_text, dilution_comparison, dilution_value, interpretation).
 
 Strategy for representing paired blood culture sets:
 */
DROP TABLE IF EXISTS mimiciv_local.bcresults;
CREATE TABLE mimiciv_local.bcresults AS (
    WITH bloodculture_results AS (
        SELECT subject_id,
            CASE
                -- Gram Negatives
                WHEN org_name = 'ESCHERICHIA COLI'
                OR org_name = 'PSEUDOMONAS AERUGINOSA'
                OR org_name = 'KLEBSIELLA PNEUMONIAE'
                OR org_name = 'PROTEUS MIRABILIS'
                OR org_name = 'ENTEROBACTER CLOACAE COMPLEX'
                OR org_name = 'KLEBSIELLA OXYTOCA'
                OR org_name = 'SERRATIA MARCESCENS'
                OR org_name = 'CORYNEBACTERIUM SPECIES (DIPHTHEROIDS)'
                OR org_name = 'ENTEROBACTER CLOACAE'
                OR org_name = 'CITROBACTER FREUNDII COMPLEX'
                OR org_name = 'ENTEROBACTER AEROGENES'
                OR org_name = 'ACINETOBACTER BAUMANNII COMPLEX'
                OR org_name = 'MORGANELLA MORGANII'
                OR org_name = 'BACTEROIDES FRAGILIS GROUP'
                OR org_name = '"NON-FERMENTER, NOT PSEUDOMONAS AERUGINOSA"' -- sic
                OR org_name = 'LACTOBACILLUS SPECIES'
                OR org_name = 'STENOTROPHOMONAS MALTOPHILIA'
                OR org_name = 'PANTOEA SPECIES'
                OR org_name = 'PSEUDOMONAS PUTIDA'
                OR org_name = 'CITROBACTER KOSERI'
                OR org_name = 'CHRYSEOBACTERIUM INDOLOGENES'
                OR org_name = 'ACINETOBACTER SP.'
                OR org_name = 'ENTEROBACTER ASBURIAE'
                OR org_name = 'SALMONELLA SPECIES'
                OR org_name = 'AEROMONAS HYDROPHILA' -- stopped here
                THEN 'Gram Negative' -- Gram Positives
                WHEN org_name = 'STAPH AUREUS COAG +'
                OR org_name = 'STAPHYLOCOCCUS, COAGULASE NEGATIVE'
                OR org_name = 'STAPHYLOCOCCUS EPIDERMIDIS'
                OR org_name = 'ENTEROCOCCUS FAECIUM'
                OR org_name = 'ENTEROCOCCUS FAECALIS'
                OR org_name = 'VIRIDANS STREPTOCOCCI'
                OR org_name = 'STAPHYLOCOCCUS HOMINIS'
                OR org_name = 'STREPTOCOCCUS PNEUMONIAE'
                OR org_name = 'BETA STREPTOCOCCUS GROUP B'
                OR org_name = 'STREPTOCOCCUS ANGINOSUS (MILLERI) GROUP'
                OR org_name = 'STAPHYLOCOCCUS CAPITIS'
                OR org_name = 'STAPHYLOCOCCUS LUGDUNENSIS'
                OR org_name = 'STAPHYLOCOCCUS HAEMOLYTICUS'
                OR org_name = 'STREPTOCOCCUS MITIS/ORALIS'
                OR org_name = 'BETA STREPTOCOCCUS GROUP A'
                OR org_name = 'ANAEROBIC GRAM POSITIVE COCCUS(I)'
                OR org_name = 'BACILLUS SPECIES; NOT ANTHRACIS'
                OR org_name = 'STAPHYLOCOCCUS WARNERI'
                OR org_name = 'ENTEROCOCCUS SP.'
                OR org_name = 'PROBABLE MICROCOCCUS SPECIES'
                OR org_name = 'ABIOTROPHIA/GRANULICATELLA SPECIES'
                OR org_name = 'BETA STREPTOCOCCUS GROUP G'
                OR org_name = 'GRAM POSITIVE RODS'
                OR org_name = 'STREPTOCOCCUS GALLOLYTICUS SSP. PASTEURIANUS (STREPTOCOCCUS BOVIS)'
                OR org_name = 'STREPTOCOCCUS SANGUINIS'
                OR org_name = 'GRAM POSITIVE COCCUS(COCCI)'
                OR org_name = 'PROPIONIBACTERIUM ACNES'
                OR org_name = 'STREPTOCOCCUS ANGINOSUS'
                OR org_name = 'STREPTOCOCCUS BOVIS ' -- sic
                OR org_name = 'ENTEROCOCCUS CASSELIFLAVUS' THEN 'Gram Positive' -- Fungi
                WHEN org_name = 'CANDIDA ALBICANS'
                OR org_name = 'CANDIDA GLABRATA'
                OR org_name = 'CANDIDA PARAPSILOSIS'
                OR org_name = 'CANDIDA TROPICALIS' THEN 'Fungal'
                WHEN org_name IS NULL THEN 'No Growth'
                ELSE 'Growth Uncategorized'
            END AS result,
            org_name,
            test_name,
            charttime
        FROM mimiciv_hosp.microbiologyevents
        WHERE spec_type_desc = 'BLOOD CULTURE'
            AND org_name IS DISTINCT
        FROM 'CANCELLED'
            AND charttime IS NOT NULL
            AND test_name IS DISTINCT
        FROM 'Anaerobic Bottle Gram Stain'
            AND test_name IS DISTINCT
        FROM 'Aerobic Bottle Gram Stain'
        GROUP BY subject_id,
            org_name,
            test_name,
            charttime
    )
    SELECT bcr.subject_id,
        id.stay_id,
        bcr.result,
        bcr.charttime
    FROM bloodculture_results bcr
        LEFT JOIN mimiciv_derived.icustay_detail id ON id.subject_id = bcr.subject_id
        AND id.icu_intime <= bcr.charttime
        AND id.icu_outtime >= bcr.charttime
    WHERE stay_id IS NOT NULL
);
CREATE INDEX IF NOT EXISTS bcresults_sid_charttime ON mimiciv_local.bcresults(stay_id, charttime);