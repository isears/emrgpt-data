DROP TABLE IF EXISTS mimiciv_local.splits;
CREATE TABLE mimiciv_local.splits AS (
    SELECT stay_id,
        random() * 100 > 90 AS testset
    FROM mimiciv_derived.icustay_detail
);
CREATE UNIQUE INDEX splits_sid ON mimiciv_local.splits(stay_id);