# NOTE: unsafe sql query construction
from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    Engine,
    column,
    literal_column,
    text,
    select,
    extract,
    literal,
    null,
    cast,
    String,
    DOUBLE_PRECISION,
    NUMERIC,
    INTEGER,
    TEXT,
    VARCHAR,
    and_,
    union_all,
    case,
)
from sqlalchemy.sql import values, func, alias, lateral, true
import os
from dataclasses import dataclass, field
from typing import Literal
from sqlalchemy.dialects import postgresql
import sys
import datetime
from typing import Optional


@dataclass
class TableTokenizationSpec:
    table_name: str
    event_type: Literal["infusion", "onetime"]
    ignore_cols: list[str] = field(default_factory=list)
    modulated_cols: dict = field(default_factory=dict)
    needs_alignment: bool = False
    schema: str = "mimiciv_derived"

    def __post_init__(self):
        # columns we never want to tokenize
        self.ignore_cols += [
            "subject_id",
            "hadm_id",
            "stay_id",
            "specimen_id",
            "charttime",
            "starttime",
            "endtime",
            "stoptime",
        ]

    def get_numeric_columns(self, table: Table):
        return [
            i.name
            for i in table.c
            if (isinstance(i.type, DOUBLE_PRECISION) or isinstance(i.type, NUMERIC))
            and i.name not in self.ignore_cols
            and i.name not in self.modulated_cols.keys()
            and i.name not in self.modulated_cols.values()
        ]

    def get_categorical_columns(self, table: Table):
        return [
            i.name
            for i in table.c
            if (
                isinstance(i.type, TEXT)
                or isinstance(i.type, INTEGER)
                or isinstance(i.type, VARCHAR)
            )
            and i.name not in self.ignore_cols
            and i.name not in self.modulated_cols.keys()
            and i.name not in self.modulated_cols.values()
        ]


TTSs = [
    TableTokenizationSpec(
        "vitalsign",
        "onetime",
        ["mbp", "sbp_ni", "dbp_ni", "mbp_ni"],
        {"temperature": "temperature_site"},
    ),
    TableTokenizationSpec("crrt", "onetime"),
    # Don't need NED once inputevents fully integrated
    # TableTokenizationSpec("norepinephrine_equivalent_dose", "infusion"),
    TableTokenizationSpec("chemistry", "onetime", ["aniongap"], needs_alignment=True),
    TableTokenizationSpec("complete_blood_count", "onetime", needs_alignment=True),
    TableTokenizationSpec("blood_differential", "onetime", needs_alignment=True),
    # TODO: specimen column should be a modulator column for all other columns in bg
    TableTokenizationSpec("bg", "onetime", needs_alignment=True),
    # TODO: categorical infusion-types
    # TableTokenizationSpec("antibiotic", "infusion"),
    TableTokenizationSpec("cardiac_marker", "onetime", needs_alignment=True),
    TableTokenizationSpec("coagulation", "onetime", needs_alignment=True),
    TableTokenizationSpec("enzyme", "onetime", needs_alignment=True),
    TableTokenizationSpec("icp", "onetime"),
    TableTokenizationSpec("urine_output", "onetime"),
    TableTokenizationSpec("ventilator_setting", "onetime"),
    TableTokenizationSpec("inflammation", "onetime", needs_alignment=True),
    # TODO: modulated infusion-types
    # TableTokenizationSpec(
    #     "invasive_line", "infusion", modulated_cols={"line_site": "line_type"}
    # ),
    # TODO: may be able to use some of these ignored columns
    TableTokenizationSpec(
        "rhythm",
        "onetime",
        ["ectopy_frequency", "ectopy_type_secondary", "ectopy_frequency_secondary"],
        needs_alignment=True,
    ),
    TableTokenizationSpec("bcresults", "onetime", schema="mimiciv_local"),
]

# 10 for deciles, 100 for percentiles, etc.
PERCENTILE_MULTIPLIER = 10


def build_table_stmt_onetime(tts: TableTokenizationSpec, table: Table):

    numeric_cols = tts.get_numeric_columns(table)
    categorical_cols = tts.get_categorical_columns(table)

    tokenization_data_expr = [
        (literal(f"{tts.table_name}.{cname}"), table.c[cname], table.c[cname] == None)
        for cname in numeric_cols
    ]

    tokenization_data_expr += [
        (
            func.concat(
                literal(f"{tts.table_name}.{cname}."), cast(table.c[cname], TEXT)
            ),
            cast(None, DOUBLE_PRECISION),
            table.c[cname] == None,
        )
        for cname in categorical_cols
    ]

    tokenization_data_expr += [
        (
            func.concat(
                literal(f"{tts.table_name}.{cname}."), cast(table.c[mod_cname], String)
            ),
            table.c[cname],
            table.c[cname] == None,
        )
        for cname, mod_cname in tts.modulated_cols.items()
    ]

    tokens = values(
        column("token_label"), column("token_value"), column("token_null")
    ).data(tokenization_data_expr)
    tokens = lateral(tokens).alias("tokens")

    return (
        select(
            table.c.stay_id,
            table.c.charttime,
            tokens.c.token_label,
            tokens.c.token_value,
        )
        .select_from(table.join(tokens, true()))
        .where(~tokens.c.token_null)
    ).cte(f"{tts.table_name}_tokenized")


def build_table_stmt_infusion(ttd: TableTokenizationSpec, table: Table):
    numeric_cols = tts.get_numeric_columns(table)
    categorical_cols = tts.get_categorical_columns(table)

    assert len(categorical_cols) == 0, "Categorical infusion events not yet supported"
    assert len(ttd.modulated_cols) == 0, "Modulated infusion events not yet supported"

    tokenization_data_expr = list()

    for ncol in numeric_cols:
        # TODO: divide by time
        tokenization_data_expr.append(
            (
                table.c.starttime,
                literal(f"{tts.table_name}.rate"),
                table.c[ncol]
                / (extract("epoch", table.c.endtime - table.c.starttime) / 3600),
            )
        )

        tokenization_data_expr.append(
            (table.c.endtime, literal(f"{tts.table_name}.rate"), 0.0)
        )

    tokens = values(
        column("charttime"), column("token_label"), column("token_value")
    ).data(tokenization_data_expr)
    tokens = lateral(tokens).alias("tokens")

    return (
        select(
            table.c.stay_id,
            tokens.c.charttime,
            tokens.c.token_label,
            tokens.c.token_value,
        )
        .select_from(table.join(tokens, true()))
        .cte(f"{tts.table_name}_tokenized")
    )


def do_alignment(tts: TableTokenizationSpec, table: Table, icustays: Table):
    cte = (
        select(icustays.c.stay_id, *[i for i in table.columns])
        .select_from(
            table.join(
                icustays,
                and_(
                    table.c.subject_id == icustays.c.subject_id,
                    table.c.charttime >= icustays.c.icu_intime,
                    table.c.charttime <= icustays.c.icu_outtime,
                ),
            )
        )
        .where(icustays.c.stay_id != None)
    ).cte(f"{tts.table_name}_aligned")

    return cte


if __name__ == "__main__":
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "")
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    dbname = os.environ.get("PGDATABASE", "mimiciv")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    metadata = MetaData()

    icustays = Table(
        "icustay_detail", metadata, autoload_with=engine, schema="mimiciv_derived"
    )

    # Generate event tokens
    ctes_for_union = list()

    for tts in TTSs:
        table = Table(tts.table_name, metadata, autoload_with=engine, schema=tts.schema)

        if tts.needs_alignment:
            alignment_cte = do_alignment(tts, table, icustays)
            table = alignment_cte

        if tts.event_type == "onetime":
            ctes_for_union.append(build_table_stmt_onetime(tts, table))  # type: ignore
        elif tts.event_type == "infusion":
            ctes_for_union.append(build_table_stmt_infusion(tts, table))  # type: ignore

    # Generate special tokens (hr, admission, discharge, death)
    hour_cte = (
        select(
            icustays.c.stay_id,
            func.generate_series(
                func.date_trunc("hour", icustays.c.icu_intime),
                func.date_trunc("hour", icustays.c.icu_outtime),
                "1 hour",
            ).label("charttime"),
            func.concat(
                "hour.",
                extract(
                    "hour",
                    func.generate_series(
                        func.date_trunc("hour", icustays.c.icu_intime),
                        func.date_trunc("hour", icustays.c.icu_outtime),
                        "1 hour",
                    ),
                ),
            ).label("token_label"),
            cast(None, DOUBLE_PRECISION).label("token_value"),
        )
        .select_from(icustays)
        .cte("hour_events")
    )

    admission_cte = (
        select(
            icustays.c.stay_id,
            icustays.c.icu_intime.label("charttime"),
            literal("admission").label("token_label"),
            cast(None, DOUBLE_PRECISION).label("token_value"),
        )
        .select_from(icustays)
        .cte("admission_events")
    )

    discharge_cte = (
        select(
            icustays.c.stay_id,
            icustays.c.icu_outtime.label("charttime"),
            literal("discharge").label("token_label"),
            cast(None, DOUBLE_PRECISION).label("token_value"),
        )
        .select_from(icustays)
        .cte("discharge_events")
    )

    mort_cte = (
        select(
            icustays.c.stay_id,
            icustays.c.dischtime.label("charttime"),
            literal("mort").label("token_label"),
            cast(None, DOUBLE_PRECISION).label("token_value"),
        )
        .select_from(icustays)
        .where(icustays.c.hospital_expire_flag == 1)
        .cte("mort_events")
    )

    ctes_for_union += [hour_cte, admission_cte, discharge_cte, mort_cte]

    # Union all subqueries together
    union_cte = union_all(
        *[
            select(
                cte.c.stay_id,
                cte.c.charttime,
                cte.c.token_label,
                cte.c.token_value,
            )
            for cte in ctes_for_union
        ]
    ).cte("union_tokenized")

    # Token value discretization
    # TODO: currently deciles, consider full percentile
    # TODO: this discretizes to the int range 0 - 10 (inclusive) so effectively 11 bins
    # Bin 10 will only be the bin of MAXIMUM measurements over the enitre dataset
    # Bin 10 will be much smaller than the others, often times only one row will be descritized to 10
    # This is obviously not ideal
    # Additionally, a lot of 'interesting' things happen at the 10th and 90th percentiles
    # Linear discretization may not be as effective as a discretization scheme
    # That would add more detail at the tails of the distribution (?sigmoid)
    token_value_cte = (
        select(
            union_cte.c.stay_id,
            union_cte.c.charttime,
            union_cte.c.token_label,
            union_cte.c.token_value,
            func.floor(
                func.percent_rank().over(
                    partition_by=union_cte.c.token_label,
                    order_by=union_cte.c.token_value,
                )
                * PERCENTILE_MULTIPLIER
            )
            .cast(INTEGER)
            .label("token_value_disc"),
        )
        .order_by("stay_id", "charttime")
        .cte("token_values")
    )

    # Do input events seperately
    inputevents = Table(
        "inputevents", metadata, autoload_with=engine, schema="mimiciv_icu"
    )
    d_items = Table("d_items", metadata, autoload_with=engine, schema="mimiciv_icu")

    meds_cte = (
        select(
            inputevents.c.stay_id,
            func.generate_series(
                inputevents.c.starttime,
                inputevents.c.endtime,
                "1 hour",
            ).label("charttime"),
            d_items.c.label.label("token_label"),
            inputevents.c.amountuom.label("uom_label"),
            case(
                (
                    (inputevents.c.endtime - inputevents.c.starttime)
                    > datetime.timedelta(hours=1),
                    inputevents.c.amount
                    / (
                        extract(
                            "epoch", inputevents.c.endtime - inputevents.c.starttime
                        )
                        / 3600
                    ),
                ),
                else_=inputevents.c.amount,
            ).label("dose"),
        )
        .select_from(inputevents)
        .join(d_items, d_items.c.itemid == inputevents.c.itemid)
    ).cte("meds")

    med_values_cte = select(
        meds_cte.c.stay_id,
        meds_cte.c.charttime,
        meds_cte.c.token_label,
        meds_cte.c.uom_label,
        meds_cte.c.dose,
        func.floor(
            func.percent_rank().over(
                partition_by=(meds_cte.c.token_label, meds_cte.c.uom_label),
                order_by=meds_cte.c.dose,
            )
            * PERCENTILE_MULTIPLIER
        )
        .cast(INTEGER)
        .label("token_value_disc"),
    ).cte("med_values")

    med_derived_events_combined_cte = union_all(
        select(
            med_values_cte.c.stay_id,
            med_values_cte.c.charttime,
            med_values_cte.c.token_label,
            med_values_cte.c.dose.label("token_value"),
            med_values_cte.c.token_value_disc,
            med_values_cte.c.uom_label,
        ).select_from(med_values_cte),
        select(
            token_value_cte.c.stay_id,
            token_value_cte.c.charttime,
            token_value_cte.c.token_label,
            token_value_cte.c.token_value,
            token_value_cte.c.token_value_disc,
            literal(None).label("uom_label"),
        ).select_from(token_value_cte),
    ).cte("med_derived_events_combined")

    # Convert to raw event stream rather than token, value pair stream
    numbered_events_cte = (
        select(
            med_derived_events_combined_cte.c.stay_id,
            med_derived_events_combined_cte.c.charttime,
            med_derived_events_combined_cte.c.token_label,
            med_derived_events_combined_cte.c.token_value,
            med_derived_events_combined_cte.c.token_value_disc,
            med_derived_events_combined_cte.c.uom_label,
            func.row_number()
            .over(
                partition_by=(
                    med_derived_events_combined_cte.c.stay_id,
                    med_derived_events_combined_cte.c.charttime,
                ),
                order_by=(
                    med_derived_events_combined_cte.c.token_label,
                    med_derived_events_combined_cte.c.token_value_disc,
                ),
            )
            .label("event_idx"),
        )
        .select_from(med_derived_events_combined_cte)
        .cte("numbered_events")
    )

    token_stream_cte = (
        union_all(
            select(
                numbered_events_cte.c.stay_id,
                numbered_events_cte.c.charttime,
                numbered_events_cte.c.token_label.label("token"),
                numbered_events_cte.c.event_idx,
                literal(1).label("sort_order"),
            ).select_from(numbered_events_cte),
            select(
                numbered_events_cte.c.stay_id,
                numbered_events_cte.c.charttime,
                numbered_events_cte.c.uom_label.label("token"),
                numbered_events_cte.c.event_idx,
                literal(2).label("sort_order"),
            )
            .select_from(numbered_events_cte)
            .where(numbered_events_cte.c.uom_label != None),
            select(
                numbered_events_cte.c.stay_id,
                numbered_events_cte.c.charttime,
                func.concat(
                    literal("magnitude."),
                    cast(numbered_events_cte.c.token_value_disc, TEXT),
                ).label("token"),
                numbered_events_cte.c.event_idx,
                literal(3).label("sort_order"),
            )
            .select_from(numbered_events_cte)
            .where(numbered_events_cte.c.token_value != None),
        )
        .order_by("stay_id", "charttime", "event_idx", "sort_order")
        .cte("token_stream")
    )

    unique_tokens_cte = (
        select(token_stream_cte.c.token).select_from(token_stream_cte).group_by("token")
    ).cte("unique_tokens")

    d_tokens_cte = (
        select(
            func.row_number()
            .over(order_by=unique_tokens_cte.c.token)
            .label("token_id"),
            unique_tokens_cte.c.token,
        )
        .select_from(unique_tokens_cte)
        .cte("d_tokens")
    )

    stmt = select(
        token_stream_cte.c.stay_id,
        token_stream_cte.c.charttime,
        d_tokens_cte.c.token_id,
        token_stream_cte.c.token,
    ).join(d_tokens_cte, d_tokens_cte.c.token == token_stream_cte.c.token)

    print(f"-- Do not edit directly: autogenerated sql")
    print("DROP TABLE IF EXISTS mimiciv_local.tokenevents;")
    print("CREATE TABLE mimiciv_local.tokenevents AS (")
    print(
        stmt.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    print(");")
    print(
        "CREATE INDEX IF NOT EXISTS sid_time ON mimiciv_local.tokenevents(stay_id, charttime);"
    )

    d_tokens = (
        select(column("token_id"), column("token"))
        .select_from(text("mimiciv_local.tokenevents"))
        .group_by(column("token_id"), column("token"))
    )

    print("DROP TABLE IF EXISTS mimiciv_local.d_tokens;")
    print("CREATE TABLE mimiciv_local.d_tokens AS (")
    print(
        d_tokens.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    print(");")
    print(
        "CREATE UNIQUE INDEX IF NOT EXISTS token_id ON mimiciv_local.d_tokens(token_id);"
    )
