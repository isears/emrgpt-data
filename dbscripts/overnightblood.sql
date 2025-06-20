DROP TABLE IF EXISTS mimiciv_local.overnight_blood;
CREATE TABLE mimiciv_local.overnight_blood AS (
    WITH first_blood_product AS (
        SELECT DISTINCT on (stay_id) stay_id,
            starttime AS blood_time,
            itemid
        FROM mimiciv_icu.inputevents
        WHERE itemid = 225168
            OR itemid = 226368
            OR itemid = 227070
        ORDER BY stay_id,
            starttime ASC
    ),
    study_group AS (
        SELECT s.stay_id,
            fbp.blood_time
        FROM mimiciv_local.splits s
            LEFT JOIN first_blood_product fbp ON s.stay_id = fbp.stay_id
        WHERE (s.testset = true)
            AND (
                (fbp.blood_time IS NULL)
                OR (
                    -- Exclude stays that received OR / PACU blood
                    fbp.itemid != 226368
                    AND fbp.itemid != 227070 -- Exclude stays that received blood during the day shift
                    AND (
                        (
                            extract(
                                HOUR
                                FROM fbp.blood_time
                            ) > 19
                            OR extract(
                                HOUR
                                FROM fbp.blood_time
                            ) < 7
                        )
                    )
                )
            )
        ORDER BY stay_id
    ),
    handoffs AS (
        SELECT stay_id,
            icu_intime,
            icu_outtime,
            generate_series(
                time_bucket('12 hours', icu_intime, '19:00:00'::time),
                time_bucket('12 hours', icu_outtime, '19:00:00'::time),
                '12 hours'::interval
            ) AS shift_starttime
        FROM mimiciv_derived.icustay_detail
    )
    SELECT s.stay_id,
        s.blood_time,
        h.shift_starttime,
        coalesce(
            (blood_time - shift_starttime) <= '12 hours'::interval,
            false
        ) AS blood_given
    FROM study_group s
        LEFT JOIN handoffs h ON h.stay_id = s.stay_id
    WHERE extract(
            HOUR
            FROM h.shift_starttime
        ) = 19
        AND (
            s.blood_time IS NULL
            OR s.blood_time > h.shift_starttime
        ) -- Exclude stays where blood admin happened within first 12 hrs of icu stay
        AND (
            s.blood_time IS NULL
            OR s.blood_time > h.icu_intime + '12 hours'::interval
        )
        AND (h.icu_intime < h.shift_starttime)
);
CREATE INDEX IF NOT EXISTS overnight_blood_sid_shiftstart ON mimiciv_local.overnight_blood(stay_id, shift_starttime);