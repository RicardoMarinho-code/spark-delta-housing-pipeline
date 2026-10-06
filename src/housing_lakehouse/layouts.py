"""Source file layouts and the per-column LGPD catalog.

Source layouts keep the original Brazilian column names: they mirror the official CadÚnico extract
and the agencies' files, so real files load unchanged. Each column declares its type, its category
under the LGPD (Brazil's data protection law) and the action applied before data leaves bronze.
From silver on, every column has an English name (`target`).

The catalog is the single source of truth for the privacy policy: silver applies it, tests enforce
its invariants (no identifier leaves bronze in clear text) and the docs are generated from it
(`housing catalog`). Domain codes quoted in the notes follow the CadÚnico form v7 dictionary;
check the official dictionary before production use.
"""

from __future__ import annotations

from dataclasses import dataclass

# Data category (LGPD art. 5)
IDENTIFIER = "identifier"  # directly identifies a person: name, ID number, address
PERSONAL = "personal"  # personal data (art. 5, I)
SENSITIVE = "sensitive"  # sensitive personal data (art. 5, II and art. 11)
OPERATIONAL = "operational"  # not personal data

# Action applied on the way from bronze to silver
KEEP = "keep"
DROP = "drop"
PSEUDONYMIZE = "pseudonymize"
GENERALIZE = "generalize"

MINIMIZATION_NOTE = "Not needed for the purpose (data minimization, art. 6, III)"
COMMUNITY_NOTE = "Identifies a small community (re-identification risk); the yes/no flag is enough"


@dataclass(frozen=True)
class Column:
    name: str
    target: str | None = None
    dtype: str = "str"  # str | int | dec | date
    category: str = PERSONAL
    action: str = KEEP
    document: str | None = None  # cpf | nis: normalized and validated before pseudonymization
    treatment: str | None = None  # zip5 | age, for GENERALIZE
    note: str = ""

    @property
    def target_name(self) -> str:
        return self.target or self.name.lower()


@dataclass(frozen=True)
class Layout:
    name: str
    columns: tuple[Column, ...]

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)


def _drop_id(name: str, note: str = "") -> Column:
    return Column(name, category=IDENTIFIER, action=DROP, note=note)


def _drop(name: str, dtype: str = "str", category: str = PERSONAL, note: str = MINIMIZATION_NOTE) -> Column:
    return Column(name, dtype=dtype, category=category, action=DROP, note=note)


def _int(name: str, target: str, category: str = PERSONAL, note: str = "") -> Column:
    return Column(name, target, "int", category, note=note)


def _dec(name: str, target: str) -> Column:
    return Column(name, target, "dec")


def _date(name: str, target: str) -> Column:
    return Column(name, target, "date")


# CadÚnico family file: 66 columns, in the order of the official extract.
FAMILY = Layout(
    "cadunico_family",
    (
        _int("CD_IBGE_CADASTRO", "municipality_code", OPERATIONAL),
        Column(
            "CO_FAMILIAR_FAM",
            "family_id",
            category=IDENTIFIER,
            action=PSEUDONYMIZE,
            note="Family code becomes an HMAC-SHA256 pseudonym",
        ),
        _date("DT_CADASTRO_FAM", "registration_date"),
        _date("DT_ATUALIZACAO_FAM", "last_update_date"),
        _int(
            "CO_EST_CADASTRAL_FAM", "registry_status", note="1 in progress, 2 no civil record, 3 registered, 4 removed"
        ),
        _dec("VL_RENDA_MEDIA_FAM", "reported_per_capita_income"),
        _drop("CO_MODALIDADE_OPER_FAM", "int", OPERATIONAL),
        _drop("CO_FORMA_COLETA_FAM", "int", OPERATIONAL),
        _drop_id("NO_LOCALIDADE_FAM", "Address"),
        _drop_id("NO_TIP_LOGRADOURO_FAM", "Address"),
        _drop_id("NO_TIT_LOGRADOURO_FAM", "Address"),
        _drop_id("NO_LOGRADOURO_FAM", "Address"),
        _drop_id("NU_LOGRADOURO_FAM", "Address"),
        _drop_id("DS_COMPLEMENTO_FAM", "Address"),
        _drop_id("DS_COMPLEMENTO_ADIC_FAM", "Address"),
        Column(
            "NU_CEP_LOGRADOURO_FAM",
            "zip_prefix",
            action=GENERALIZE,
            treatment="zip5",
            note="Reduced to the first 5 digits (area, not address)",
        ),
        _drop("CO_UNIDADE_TERRITORIAL_FAM"),
        _drop("NO_UNIDADE_TERRITORIAL_FAM"),
        _drop_id("DS_REFERENCIA_LOCAL_FAM", "Free-text location"),
        _drop_id("NO_ENTREVISTADOR_FAM", "Interviewer's personal data"),
        _drop_id("NU_CPF_ENTREVISTADOR_FAM", "Interviewer's personal data"),
        _drop_id("DS_OBS_ENTREVISTADOR_FAM", "Free text: may contain any personal data"),
        _drop("IN_FAM_ALTERADA_V7", "int", OPERATIONAL),
        _int("CO_LOCAL_DOMIC_FAM", "area_type", note="1 urban, 2 rural"),
        _int("CO_ESPECIE_DOMIC_FAM", "dwelling_kind", note="1 permanent, 2 improvised, 3 collective"),
        _int("QT_COMODOS_DOMIC_FAM", "rooms"),
        _int("QT_COMODOS_DORMITORIO_FAM", "bedrooms"),
        _int("CO_MATERIAL_PISO_FAM", "floor_material", note="1 dirt floor"),
        _int("CO_MATERIAL_DOMIC_FAM", "wall_material", note="5 bare wattle and daub, 6 scrap wood, 7 straw, 8 other"),
        _int("CO_AGUA_CANALIZADA_FAM", "piped_water", note="1 yes, 2 no"),
        _int("CO_ABASTE_AGUA_DOMIC_FAM", "water_source"),
        _int("CO_BANHEIRO_DOMIC_FAM", "has_bathroom", note="1 yes, 2 no"),
        _int(
            "CO_ESCOA_SANITARIO_DOMIC_FAM", "sewage_disposal", note="3 rudimentary pit, 4 ditch, 5 river/sea, 6 other"
        ),
        _int(
            "CO_DESTINO_LIXO_DOMIC_FAM", "garbage_disposal", note="3 burned/buried, 4 vacant lot, 5 river/sea, 6 other"
        ),
        _int("CO_ILUMINACAO_DOMIC_FAM", "lighting", note="4 oil/kerosene/gas, 5 candle, 6 other"),
        _int("CO_CALCAMENTO_DOMIC_FAM", "street_paving"),
        _int("IN_FAMILIA_INDIGENA_FAM", "is_indigenous", SENSITIVE, "Ethnic origin: kept to target the policy"),
        _drop("CO_POVO_INDIGENA_FAM", "int", SENSITIVE, COMMUNITY_NOTE),
        _drop("NO_POVO_INDIGENA_FAM", "str", SENSITIVE, COMMUNITY_NOTE),
        _drop("CO_INDIGENA_RESIDE_FAM", "int", SENSITIVE, COMMUNITY_NOTE),
        _drop("CO_RESERVA_INDIGENA_FAM", "int", SENSITIVE, COMMUNITY_NOTE),
        _drop("NO_RESERVA_INDIGENA_FAM", "str", SENSITIVE, COMMUNITY_NOTE),
        _int("IN_FAMILIA_QUILOMBOLA_FAM", "is_quilombola", SENSITIVE, "Ethnic origin: kept to target the policy"),
        _drop("CO_COMUNIDADE_QUILOMBOLA_FAM", "int", SENSITIVE, COMMUNITY_NOTE),
        _drop("NO_COMUNIDADE_QUILOMBOLA_FAM", "str", SENSITIVE, COMMUNITY_NOTE),
        _int("QT_PESSOAS_DOMIC_FAM", "people_in_dwelling"),
        _int("QT_FAMILIAS_DOMIC_FAM", "families_in_dwelling"),
        _int("QT_PESSOA_INTER_0_17_ANOS_FAM", "members_0_17"),
        _int("QT_PESSOA_INTER_18_59_ANOS_FAM", "members_18_59"),
        _int("QT_PESSOA_INTER_60_ANOS_FAM", "members_60_plus"),
        _dec("VL_DESP_ENERGIA_FAM", "expense_energy"),
        _dec("VL_DESP_AGUA_ESGOTO_FAM", "expense_water_sewage"),
        _dec("VL_DESP_GAS_FAM", "expense_gas"),
        _dec("VL_DESP_ALIMENTACAO_FAM", "expense_food"),
        _dec("VL_DESP_TRANSPOR_FAM", "expense_transport"),
        _dec("VL_DESP_ALUGUEL_FAM", "expense_rent"),
        _dec("VL_DESP_MEDICAMENTOS_FAM", "expense_medicine"),
        _drop("NO_ESTAB_ASSIST_SAUDE_FAM", "str", SENSITIVE, "Link to a health service; not needed"),
        _drop("NU_ESTBO_SAUDE", "str", SENSITIVE, "Link to a health service; not needed"),
        _drop("NO_CENTRO_ASSIST_FAM"),
        _drop("CO_CENTRO_ASSIST_FAM"),
        _int("IN_PARC_MDS_FAM", "mds_partner_flag"),
        _dec("VL_RENDA_TOTAL_FAM", "total_income"),
        _int("QT_MEMBRO_FAMILIA", "family_members"),
        _drop_id("CO_CTA_ENERG_UNID_CONSUM_FAM", "Power bill account number identifies the household"),
        _int("DS_MARC_BPC", "receives_bpc", SENSITIVE, "Receives BPC (elderly or disabled person benefit)"),
    ),
)

# CadÚnico person file (subset of the full layout with the fields this project uses).
PERSON = Layout(
    "cadunico_person",
    (
        _int("CD_IBGE_CADASTRO", "municipality_code", OPERATIONAL),
        Column("CO_FAMILIAR_FAM", "family_id", category=IDENTIFIER, action=PSEUDONYMIZE),
        Column("CO_CHV_NATURAL_PESSOA", "person_id", category=IDENTIFIER, action=PSEUDONYMIZE),
        _date("DT_CADASTRAMENTO_MEMB", "registration_date"),
        _int("CO_EST_CADASTRAL_MEMB", "registry_status"),
        _drop_id("NO_PESSOA"),
        Column("NU_NIS_PESSOA", "nis_pseudo", category=IDENTIFIER, action=PSEUDONYMIZE, document="nis"),
        _drop_id("NO_APELIDO_PESSOA"),
        _int("CO_SEXO_PESSOA", "sex"),
        Column(
            "DT_NASC_PESSOA",
            "age",
            "date",
            action=GENERALIZE,
            treatment="age",
            note="Birth date becomes age in years at the reference date",
        ),
        _int("CO_PARENTESCO_RF_PESSOA", "relationship_to_head", note="1 head of family"),
        _int("CO_RACA_COR_PESSOA", "race_color", SENSITIVE, "Racial origin (art. 11): kept for equity analysis"),
        _drop_id("NO_COMPLETO_MAE_PESSOA"),
        _drop_id("NO_COMPLETO_PAI_PESSOA"),
        Column(
            "NU_CPF_PESSOA",
            "cpf_pseudo",
            category=IDENTIFIER,
            action=PSEUDONYMIZE,
            document="cpf",
            note="Same pseudonym as the CPF in the beneficiary files: enables the join without exposing the CPF",
        ),
        _drop_id("NU_IDENTIDADE_PESSOA"),
        _drop_id("NU_TITULO_ELEITOR_PESSOA"),
        _int("CO_DEFICIENCIA_MEMB", "has_disability", SENSITIVE, "Health data (art. 11)"),
        _int("CO_SABE_LER_ESCREVER_MEMB", "literate"),
        _int("IN_FREQUENTA_ESCOLA_MEMB", "attends_school"),
        _int("CO_TRABALHOU_SEMANA_MEMB", "worked_last_week"),
        _int("CO_CART_ASSINADA_MEMB", "formal_job"),
        _int("IN_DORMIR_RUA_MEMB", "sleeps_on_street", note="High vulnerability: restricted access"),
        _int("DS_MARC_BPC", "receives_bpc", SENSITIVE, "Receives BPC (elderly or disabled person benefit)"),
    ),
)

# Housing program beneficiaries (unified layout from the financial agents).
BENEFICIARIES = Layout(
    "mcmv_beneficiaries",
    (
        Column("PROGRAMA", "program", category=OPERATIONAL, note="FAR, FDS, PNHR or FGTS"),
        Column("NU_CPF_BENEFICIARIO", "cpf_pseudo", category=IDENTIFIER, action=PSEUDONYMIZE, document="cpf"),
        _drop_id("NO_BENEFICIARIO"),
        Column("DT_NASCIMENTO", "age", "date", action=GENERALIZE, treatment="age"),
        _dec("VR_RENDA_FAMILIAR", "declared_income"),
        _int("CO_MUNICIPIO_IBGE", "municipality_code", OPERATIONAL),
        Column("CO_CEP_IMOVEL", "zip_prefix", action=GENERALIZE, treatment="zip5"),
        Column("DT_CONTRATACAO", "contract_date", "date", OPERATIONAL),
        _int("CO_FAIXA_RENDA", "income_band", OPERATIONAL),
    ),
)

# Public reference data (IBGE and Fundação João Pinheiro): no personal data.
MUNICIPALITIES = Layout(
    "ref_municipalities",
    (
        _int("CO_MUNICIPIO_IBGE", "municipality_code", OPERATIONAL),
        Column("NO_MUNICIPIO", "municipality_name", category=OPERATIONAL),
        Column("SG_UF", "state", category=OPERATIONAL),
        _int("CO_UF", "state_code", OPERATIONAL),
        Column("NO_REGIAO", "region", category=OPERATIONAL),
        _int("QT_POPULACAO", "population", OPERATIONAL),
        _int("QT_DOMICILIOS", "households", OPERATIONAL),
    ),
)

FJP_DEFICIT = Layout(
    "ref_fjp_deficit",
    (
        _int("CO_MUNICIPIO_IBGE", "municipality_code", OPERATIONAL),
        _int("NU_ANO", "year", OPERATIONAL),
        _int("QT_DEFICIT_TOTAL", "deficit_total", OPERATIONAL),
        _int("QT_DEFICIT_PRECARIOS", "deficit_precarious", OPERATIONAL),
        _int("QT_DEFICIT_COABITACAO", "deficit_cohabitation", OPERATIONAL),
        _int("QT_DEFICIT_ONUS", "deficit_rent_burden", OPERATIONAL),
        _int("QT_DEFICIT_ADENSAMENTO", "deficit_overcrowding", OPERATIONAL),
    ),
)

LAYOUTS: dict[str, Layout] = {
    layout.name: layout for layout in (FAMILY, PERSON, BENEFICIARIES, MUNICIPALITIES, FJP_DEFICIT)
}
