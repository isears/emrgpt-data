-- Demographics / comorbidities by stay_id
DROP TABLE IF EXISTS mimiciv_local.staticfeats;
CREATE TABLE mimiciv_local.staticfeats AS (
    SELECT i.stay_id,
        i.admission_age AS age,
        i.gender,
        c.myocardial_infarct,
        c.congestive_heart_failure,
        c.peripheral_vascular_disease,
        c.cerebrovascular_disease,
        c.dementia,
        c.chronic_pulmonary_disease,
        c.rheumatic_disease,
        c.peptic_ulcer_disease,
        c.mild_liver_disease,
        c.diabetes_without_cc,
        c.diabetes_with_cc,
        c.paraplegia,
        c.renal_disease,
        c.malignant_cancer,
        c.severe_liver_disease,
        c.metastatic_solid_tumor,
        c.aids,
        h.height,
        w.weight_admit AS weight
    FROM mimiciv_derived.icustay_detail i
        LEFT JOIN mimiciv_derived.charlson c ON i.hadm_id = c.hadm_id
        LEFT JOIN mimiciv_derived.first_day_height h ON h.stay_id = i.stay_id
        LEFT JOIN mimiciv_derived.first_day_weight w ON w.stay_id = i.stay_id
);
CREATE UNIQUE INDEX IF NOT EXISTS staticfeats_sid ON mimiciv_local.staticfeats(stay_id);