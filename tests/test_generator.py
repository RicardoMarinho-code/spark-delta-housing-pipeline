import re
from datetime import date

import pytest

from housing_lakehouse.governance.documents import cpf_is_valid_py, ibge_is_valid_py
from housing_lakehouse.layers.bronze import LayoutError, reference_date_from_filename, validate_header
from housing_lakehouse.layouts import BENEFICIARIES, FAMILY, MUNICIPALITIES, PERSON
from housing_lakehouse.synthetic.generator import generate


def test_writes_files_in_the_source_layouts(tmp_path):
    summary = generate(tmp_path, n_municipalities=30, n_families=600, months=2, seed=1, defect_rate=0.05)
    names = sorted(p.name for p in tmp_path.iterdir())
    assert names == [
        "ARQ_FAMILIA_11082026_SYNTHETIC.TXT",
        "ARQ_FAMILIA_11092026_SYNTHETIC.TXT",
        "ARQ_PESSOA_11082026_SYNTHETIC.TXT",
        "ARQ_PESSOA_11092026_SYNTHETIC.TXT",
        "BENEFICIARIOS_MCMV_11092026.csv",
        "DEFICIT_FJP.csv",
        "MUNICIPIOS.csv",
    ]
    assert summary["families_11092026"] >= 600

    for file, layout, encoding in (
        ("ARQ_FAMILIA_11092026_SYNTHETIC.TXT", FAMILY, "iso-8859-1"),
        ("ARQ_PESSOA_11092026_SYNTHETIC.TXT", PERSON, "iso-8859-1"),
        ("BENEFICIARIOS_MCMV_11092026.csv", BENEFICIARIES, "utf-8"),
        ("MUNICIPIOS.csv", MUNICIPALITIES, "utf-8"),
    ):
        header = (tmp_path / file).read_text(encoding=encoding).splitlines()[0]
        validate_header(header, layout, ";")


def test_data_has_defects_and_valid_documents(tmp_path):
    generate(tmp_path, n_municipalities=30, n_families=800, months=1, seed=2, defect_rate=0.05)
    lines = (tmp_path / "ARQ_FAMILIA_11092026_SYNTHETIC.TXT").read_text(encoding="iso-8859-1").splitlines()[1:]
    fields = [line.split(";") for line in lines]
    assert any(len(f) != 66 for f in fields), "expected a truncated row"
    assert any("\x00" in line for line in lines), "expected \\x00 in a ZIP code"
    assert any(f[0] == "9999999" for f in fields if len(f) == 66)

    municipalities = (tmp_path / "MUNICIPIOS.csv").read_text(encoding="utf-8").splitlines()[1:]
    assert all(ibge_is_valid_py(m.split(";")[0]) for m in municipalities)

    persons = (tmp_path / "ARQ_PESSOA_11092026_SYNTHETIC.TXT").read_text(encoding="iso-8859-1").splitlines()[1:]
    cpf_idx = PERSON.names.index("NU_CPF_PESSOA")
    cpfs = [p.split(";")[cpf_idx] for p in persons if p.split(";")[cpf_idx]]
    valid = sum(cpf_is_valid_py(c) for c in cpfs)
    assert 0.9 < valid / len(cpfs) < 1.0


def test_generator_is_deterministic(tmp_path):
    generate(tmp_path / "a", n_municipalities=10, n_families=100, months=1, seed=9)
    generate(tmp_path / "b", n_municipalities=10, n_families=100, months=1, seed=9)
    for name in ("ARQ_FAMILIA_11092026_SYNTHETIC.TXT", "BENEFICIARIOS_MCMV_11092026.csv"):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()


def test_reference_date_from_file_name():
    assert reference_date_from_filename("ARQ_FAMILIA_11092026_SB8.TXT") == date(2026, 9, 11)
    assert reference_date_from_filename("BENEFICIARIOS_MCMV_30092026.csv") == date(2026, 9, 30)
    assert reference_date_from_filename("MUNICIPIOS.csv") is None
    assert reference_date_from_filename("ARQ_99999999.TXT") is None


def test_header_with_layout_change():
    header = ";".join(FAMILY.names[:-1] + ["NEW_COLUMN"])
    with pytest.raises(LayoutError, match=re.escape("NEW_COLUMN")):
        validate_header(header, FAMILY, ";")
    with pytest.raises(LayoutError, match="order"):
        validate_header(";".join(reversed(FAMILY.names)), FAMILY, ";")
    validate_header("﻿" + ";".join(n.lower() for n in FAMILY.names) + "\r", FAMILY, ";")
