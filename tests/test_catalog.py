"""LGPD policy invariants: the catalog is the source of truth, so the tests protect the catalog."""

from housing_lakehouse.governance.catalog import published_columns, render_markdown, unity_catalog_tag_sql
from housing_lakehouse.layouts import (
    DROP,
    FAMILY,
    GENERALIZE,
    IDENTIFIER,
    KEEP,
    LAYOUTS,
    PERSON,
    PSEUDONYMIZE,
)

# Column order of the official CadÚnico family extract (the same one the legacy load used)
ORIGINAL_FAMILY_HEADER = (
    "CD_IBGE_CADASTRO;CO_FAMILIAR_FAM;DT_CADASTRO_FAM;DT_ATUALIZACAO_FAM;CO_EST_CADASTRAL_FAM;"
    "VL_RENDA_MEDIA_FAM;CO_MODALIDADE_OPER_FAM;CO_FORMA_COLETA_FAM;NO_LOCALIDADE_FAM;NO_TIP_LOGRADOURO_FAM;"
    "NO_TIT_LOGRADOURO_FAM;NO_LOGRADOURO_FAM;NU_LOGRADOURO_FAM;DS_COMPLEMENTO_FAM;DS_COMPLEMENTO_ADIC_FAM;"
    "NU_CEP_LOGRADOURO_FAM;CO_UNIDADE_TERRITORIAL_FAM;NO_UNIDADE_TERRITORIAL_FAM;DS_REFERENCIA_LOCAL_FAM;"
    "NO_ENTREVISTADOR_FAM;NU_CPF_ENTREVISTADOR_FAM;DS_OBS_ENTREVISTADOR_FAM;IN_FAM_ALTERADA_V7;CO_LOCAL_DOMIC_FAM;"
    "CO_ESPECIE_DOMIC_FAM;QT_COMODOS_DOMIC_FAM;QT_COMODOS_DORMITORIO_FAM;CO_MATERIAL_PISO_FAM;CO_MATERIAL_DOMIC_FAM;"
    "CO_AGUA_CANALIZADA_FAM;CO_ABASTE_AGUA_DOMIC_FAM;CO_BANHEIRO_DOMIC_FAM;CO_ESCOA_SANITARIO_DOMIC_FAM;"
    "CO_DESTINO_LIXO_DOMIC_FAM;CO_ILUMINACAO_DOMIC_FAM;CO_CALCAMENTO_DOMIC_FAM;IN_FAMILIA_INDIGENA_FAM;"
    "CO_POVO_INDIGENA_FAM;NO_POVO_INDIGENA_FAM;CO_INDIGENA_RESIDE_FAM;CO_RESERVA_INDIGENA_FAM;NO_RESERVA_INDIGENA_FAM;"
    "IN_FAMILIA_QUILOMBOLA_FAM;CO_COMUNIDADE_QUILOMBOLA_FAM;NO_COMUNIDADE_QUILOMBOLA_FAM;QT_PESSOAS_DOMIC_FAM;"
    "QT_FAMILIAS_DOMIC_FAM;QT_PESSOA_INTER_0_17_ANOS_FAM;QT_PESSOA_INTER_18_59_ANOS_FAM;QT_PESSOA_INTER_60_ANOS_FAM;"
    "VL_DESP_ENERGIA_FAM;VL_DESP_AGUA_ESGOTO_FAM;VL_DESP_GAS_FAM;VL_DESP_ALIMENTACAO_FAM;VL_DESP_TRANSPOR_FAM;"
    "VL_DESP_ALUGUEL_FAM;VL_DESP_MEDICAMENTOS_FAM;NO_ESTAB_ASSIST_SAUDE_FAM;NU_ESTBO_SAUDE;NO_CENTRO_ASSIST_FAM;"
    "CO_CENTRO_ASSIST_FAM;IN_PARC_MDS_FAM;VL_RENDA_TOTAL_FAM;QT_MEMBRO_FAMILIA;CO_CTA_ENERG_UNID_CONSUM_FAM;DS_MARC_BPC"
)


def test_family_layout_matches_the_official_file():
    assert FAMILY.names == ORIGINAL_FAMILY_HEADER.split(";")
    assert len(FAMILY.names) == 66


def test_unique_source_and_target_names():
    for layout in LAYOUTS.values():
        assert len(set(layout.names)) == len(layout.names), layout.name
        targets = [name for name, _, _ in published_columns(layout)]
        assert len(set(targets)) == len(targets), layout.name


def test_every_silver_column_has_an_english_snake_case_name():
    for layout in LAYOUTS.values():
        for name, _, _ in published_columns(layout):
            assert name == name.lower() and name.isascii() and " " not in name, f"{layout.name}.{name}"


def test_no_identifier_leaves_bronze_in_clear_text():
    for layout in LAYOUTS.values():
        for column in layout.columns:
            if column.category == IDENTIFIER:
                assert column.action in (DROP, PSEUDONYMIZE), f"{layout.name}.{column.name}"


def test_actions_have_consistent_parameters():
    for layout in LAYOUTS.values():
        for c in layout.columns:
            if c.action == GENERALIZE:
                assert c.treatment in ("zip5", "age"), c.name
            if c.document:
                assert c.action == PSEUDONYMIZE, c.name


def test_direct_personal_data_is_dropped():
    dropped = {c.name for c in PERSON.columns if c.action == DROP}
    assert {"NO_PESSOA", "NO_COMPLETO_MAE_PESSOA", "NU_IDENTIDADE_PESSOA", "NU_TITULO_ELEITOR_PESSOA"} <= dropped
    assert PERSON.column("NU_CPF_PESSOA").action == PSEUDONYMIZE
    assert PERSON.column("DT_NASC_PESSOA").action == GENERALIZE


def test_cpf_uses_the_same_domain_in_both_datasets():
    """The CadÚnico × beneficiaries join depends on the same pseudonym for the same CPF."""
    person_cpf = PERSON.column("NU_CPF_PESSOA")
    beneficiary_cpf = LAYOUTS["mcmv_beneficiaries"].column("NU_CPF_BENEFICIARIO")
    assert person_cpf.document == beneficiary_cpf.document == "cpf"
    assert person_cpf.target_name == beneficiary_cpf.target_name == "cpf_pseudo"


def test_markdown_lists_every_column():
    text = render_markdown()
    for layout in LAYOUTS.values():
        assert f"## {layout.name}" in text
        for column in layout.names:
            assert f"`{column}`" in text


def test_unity_catalog_tags():
    statements = unity_catalog_tag_sql("lakehouse.silver.cadunico_person", PERSON)
    assert any("`cpf_pseudo`" in s and "'pseudonymize'" in s for s in statements)
    assert any("`race_color`" in s and "'sensitive'" in s for s in statements)
    assert not any("no_pessoa" in s for s in statements)
    assert all(s.startswith("ALTER TABLE lakehouse.silver.cadunico_person") for s in statements)
    assert any(f"'{KEEP}'" in s for s in statements)
