"""Generates synthetic files in the CadÚnico, beneficiary and public reference layouts.

Nothing here comes from real data: names and addresses come from Faker; CPF and NIS numbers are
generated with valid check digits to exercise validation; municipalities have IBGE-formatted codes
but fictitious names. Each municipality has a latent precariousness level that drives its families
and its "official" deficit, so CadÚnico indicators actually explain the deficit, as in real life.

Defects are injected on purpose (negative income, impossible date, unknown municipality, truncated
row, duplicates, invalid CPF, \\x00 in the ZIP code) to exercise cleaning, rules and quarantine.
"""

from __future__ import annotations

import math
import random
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from faker import Faker

from housing_lakehouse.governance.documents import format_cpf, generate_cpf, generate_nis, ibge_check_digit
from housing_lakehouse.housing_rules import band_for_income
from housing_lakehouse.layouts import BENEFICIARIES, FAMILY, FJP_DEFICIT, MUNICIPALITIES, PERSON

STATES = [  # code, abbreviation, region, approximate population share (%)
    (11, "RO", "North", 0.9), (12, "AC", "North", 0.4), (13, "AM", "North", 1.9), (14, "RR", "North", 0.3),
    (15, "PA", "North", 4.0), (16, "AP", "North", 0.4), (17, "TO", "North", 0.7),
    (21, "MA", "Northeast", 3.3), (22, "PI", "Northeast", 1.6), (23, "CE", "Northeast", 4.3),
    (24, "RN", "Northeast", 1.6), (25, "PB", "Northeast", 1.9), (26, "PE", "Northeast", 4.5),
    (27, "AL", "Northeast", 1.5), (28, "SE", "Northeast", 1.1), (29, "BA", "Northeast", 6.9),
    (31, "MG", "Southeast", 10.0), (32, "ES", "Southeast", 1.9), (33, "RJ", "Southeast", 7.9),
    (35, "SP", "Southeast", 21.9),
    (41, "PR", "South", 5.6), (42, "SC", "South", 3.7), (43, "RS", "South", 5.3),
    (50, "MS", "Center-West", 1.4), (51, "MT", "Center-West", 1.8), (52, "GO", "Center-West", 3.5),
    (53, "DF", "Center-West", 1.4),
]  # fmt: skip
REGION_PRECARIOUSNESS = {"North": 0.55, "Northeast": 0.50, "Southeast": 0.25, "South": 0.20, "Center-West": 0.30}
DEFAULT_BANDS = {1: 2850.0, 2: 4700.0, 3: 8600.0, 4: 12000.0}
FAMILY_DEFECTS = (
    "negative_income",
    "unknown_municipality",
    "invalid_date",
    "invalid_type",
    "truncated_row",
    "duplicate",
)


@dataclass
class Municipality:
    code: int
    name: str
    state: str
    state_code: int
    region: str
    population: int
    households: int
    precariousness: float
    renters: float


@dataclass
class Family:
    municipality: Municipality
    fields: dict[str, str]
    members: list[dict[str, str]] = field(default_factory=list)
    rural: bool = False
    in_deficit: bool = False
    total_income: float = 0.0


def _date(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def _dec(v: float) -> str:
    return f"{v:.2f}".replace(".", ",")  # Brazilian files use a decimal comma


def _clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


class Generator:
    def __init__(self, seed: int = 42, defect_rate: float = 0.01) -> None:
        self.rng = random.Random(seed)
        self.fake = Faker("pt_BR")
        self.fake.seed_instance(seed)
        self.defect_rate = defect_rate
        self._next_family = 10_000_000_000
        self._next_person = 100_000_000_000

    # ------------------------------------------------------------ municipalities
    def municipalities(self, n: int) -> list[Municipality]:
        rng = self.rng
        chosen = list(STATES) if n >= len(STATES) else []
        chosen += rng.choices(STATES, weights=[s[3] for s in STATES], k=n - len(chosen))
        codes: set[str] = set()
        names: set[str] = set()
        out = []
        for state_code, state, region, _ in chosen:
            base = f"{state_code}{rng.randrange(1, 10_000):04d}"
            while base in codes:
                base = f"{state_code}{rng.randrange(1, 10_000):04d}"
            codes.add(base)
            name = self.fake.city()
            while name in names:
                name = f"{name} {rng.choice(['do Norte', 'do Sul', 'da Serra', 'Nova', 'de Cima'])}"
            names.add(name)
            population = int(_clamp(rng.lognormvariate(math.log(18_000), 1.1), 1_500, 3_000_000))
            out.append(
                Municipality(
                    code=int(base + str(ibge_check_digit(base))),
                    name=name,
                    state=state,
                    state_code=state_code,
                    region=region,
                    population=population,
                    households=int(population / rng.uniform(2.7, 3.2)),
                    precariousness=_clamp(rng.gauss(REGION_PRECARIOUSNESS[region], 0.12), 0.02, 0.95),
                    renters=_clamp(rng.gauss(0.22 + (0.10 if population > 100_000 else 0), 0.06), 0.05, 0.6),
                )
            )
        return out

    # ------------------------------------------------------------ families and persons
    def family(self, mun: Municipality, reference: date, new: bool = False) -> Family:
        rng, fake, p = self.rng, self.fake, mun.precariousness
        n_members = rng.choices(range(1, 9), weights=[18, 20, 22, 18, 10, 6, 4, 2])[0]
        cohabits = rng.random() < 0.04 + 0.12 * p
        people = n_members + (rng.randint(1, 4) if cohabits else 0)
        bedrooms = max(1, min(people, rng.choices([1, 2, 3, 4], weights=[35, 40, 20, 5])[0]))
        improvised = rng.random() < 0.01 + 0.06 * p
        makeshift = rng.random() < 0.02 + 0.22 * p
        rural = rng.random() < 0.10 + 0.25 * p
        per_capita = 0.0 if rng.random() < 0.18 else max(0.0, rng.gauss(520 - 300 * p, 220))
        total_income = round(per_capita * n_members, 2)
        pays_rent = not rural and rng.random() < mun.renters
        rent = 0.0
        if pays_rent:
            rent = round(total_income * rng.uniform(0.1, 0.6) if total_income > 0 else rng.uniform(150, 450), 2)
            rent = max(rent, 150.0)
        burden = pays_rent and rent > 0.3 * total_income
        crowded = pays_rent and people / bedrooms > 3

        if new:
            registered = reference - timedelta(days=rng.randint(2, 25))
            updated = registered
        else:
            registered = reference - timedelta(days=rng.randint(60, 3650))
            updated = registered + timedelta(days=rng.randint(0, (reference - registered).days - 1))

        f = dict.fromkeys(FAMILY.names, "")
        f.update(
            {
                "CD_IBGE_CADASTRO": str(mun.code),
                "CO_FAMILIAR_FAM": str(self._new_family_code()),
                "DT_CADASTRO_FAM": _date(registered),
                "DT_ATUALIZACAO_FAM": _date(updated),
                "CO_EST_CADASTRAL_FAM": str(rng.choices([3, 1, 4], weights=[92, 5, 3])[0]),
                "VL_RENDA_MEDIA_FAM": _dec(per_capita),
                "CO_MODALIDADE_OPER_FAM": str(rng.choice([1, 2])),
                "CO_FORMA_COLETA_FAM": str(rng.choice([1, 2])),
                "NO_LOCALIDADE_FAM": fake.bairro().upper(),
                "NO_TIP_LOGRADOURO_FAM": rng.choice(["RUA", "AVENIDA", "TRAVESSA", "QUADRA"]),
                "NO_LOGRADOURO_FAM": fake.street_name().upper(),
                "NU_LOGRADOURO_FAM": str(rng.randint(1, 3000)),
                "DS_COMPLEMENTO_FAM": rng.choice(["", "CASA", "FUNDOS", f"APTO {rng.randint(1, 400)}"]),
                "NU_CEP_LOGRADOURO_FAM": re.sub(r"\D", "", fake.postcode()),
                "DS_REFERENCIA_LOCAL_FAM": rng.choice(["", "PROXIMO A ESCOLA", "PERTO DO POSTO", "AO LADO DA IGREJA"]),
                "NO_ENTREVISTADOR_FAM": fake.name().upper(),
                "NU_CPF_ENTREVISTADOR_FAM": generate_cpf(rng),
                "IN_FAM_ALTERADA_V7": "0",
                "CO_LOCAL_DOMIC_FAM": "2" if rural else "1",
                "CO_ESPECIE_DOMIC_FAM": "2" if improvised else "1",
                "QT_COMODOS_DOMIC_FAM": str(bedrooms + rng.randint(1, 4)),
                "QT_COMODOS_DORMITORIO_FAM": str(bedrooms),
                "CO_MATERIAL_PISO_FAM": "1" if rng.random() < 0.02 + 0.15 * p else str(rng.choice([2, 3, 4, 5])),
                "CO_MATERIAL_DOMIC_FAM": str(
                    rng.choice([5, 6, 7, 8]) if makeshift else rng.choices([1, 2, 3, 4], [55, 30, 10, 5])[0]
                ),
                "CO_AGUA_CANALIZADA_FAM": "2" if rng.random() < 0.05 + 0.30 * p else "1",
                "CO_ABASTE_AGUA_DOMIC_FAM": str(rng.choices([1, 2, 3, 4], [70, 18, 7, 5])[0]),
                "CO_BANHEIRO_DOMIC_FAM": "2" if rng.random() < 0.03 + 0.20 * p else "1",
                "CO_ESCOA_SANITARIO_DOMIC_FAM": str(
                    1 if rng.random() < 0.65 - 0.45 * p else rng.choice([2, 3, 3, 4, 5, 6])
                ),
                "CO_DESTINO_LIXO_DOMIC_FAM": str(
                    rng.choice([1, 2]) if rng.random() < 0.85 - 0.35 * p else rng.choice([3, 4, 5, 6])
                ),
                "CO_ILUMINACAO_DOMIC_FAM": str(
                    rng.choice([1, 2, 3]) if rng.random() < 0.97 - 0.10 * p else rng.choice([4, 5, 6])
                ),
                "CO_CALCAMENTO_DOMIC_FAM": str(rng.choice([1, 2, 3])),
                "IN_FAMILIA_INDIGENA_FAM": "1" if rng.random() < 0.01 else "2",
                "IN_FAMILIA_QUILOMBOLA_FAM": "1" if rng.random() < 0.01 else "2",
                "QT_PESSOAS_DOMIC_FAM": str(people),
                "QT_FAMILIAS_DOMIC_FAM": str(rng.choice([2, 2, 3]) if cohabits else 1),
                "VL_DESP_ENERGIA_FAM": _dec(rng.uniform(40, 220)),
                "VL_DESP_AGUA_ESGOTO_FAM": _dec(rng.uniform(0, 120)),
                "VL_DESP_GAS_FAM": _dec(rng.uniform(0, 130)),
                "VL_DESP_ALIMENTACAO_FAM": _dec(rng.uniform(150, 900)),
                "VL_DESP_TRANSPOR_FAM": _dec(rng.uniform(0, 250)),
                "VL_DESP_ALUGUEL_FAM": _dec(rent),
                "VL_DESP_MEDICAMENTOS_FAM": _dec(rng.uniform(0, 150)),
                "IN_PARC_MDS_FAM": "0",
                "VL_RENDA_TOTAL_FAM": _dec(total_income),
                "QT_MEMBRO_FAMILIA": str(n_members),
                "CO_CTA_ENERG_UNID_CONSUM_FAM": str(rng.randint(10**9, 10**10 - 1)),
                "DS_MARC_BPC": "1" if rng.random() < 0.05 else "0",
            }
        )
        family = Family(
            municipality=mun,
            fields=f,
            rural=rural,
            in_deficit=improvised or makeshift or cohabits or burden or crowded,
            total_income=total_income,
        )
        family.members = [self._person(family, reference, i == 0) for i in range(n_members)]
        ages = [int(m["_age"]) for m in family.members]
        f["QT_PESSOA_INTER_0_17_ANOS_FAM"] = str(sum(a < 18 for a in ages))
        f["QT_PESSOA_INTER_18_59_ANOS_FAM"] = str(sum(18 <= a < 60 for a in ages))
        f["QT_PESSOA_INTER_60_ANOS_FAM"] = str(sum(a >= 60 for a in ages))
        return family

    def _person(self, family: Family, reference: date, head: bool) -> dict[str, str]:
        rng, fake = self.rng, self.fake
        age = rng.randint(18, 75) if head else rng.randint(0, 17) if rng.random() < 0.55 else rng.randint(18, 85)
        birth = reference - timedelta(days=age * 365 + rng.randint(0, 364))
        adult = age >= 18
        sex = rng.choice([1, 2])
        first = fake.first_name_male() if sex == 1 else fake.first_name_female()
        disability = rng.random() < 0.06
        p = dict.fromkeys(PERSON.names, "")
        p.update(
            {
                "CD_IBGE_CADASTRO": family.fields["CD_IBGE_CADASTRO"],
                "CO_FAMILIAR_FAM": family.fields["CO_FAMILIAR_FAM"],
                "CO_CHV_NATURAL_PESSOA": str(self._new_person_code()),
                "DT_CADASTRAMENTO_MEMB": family.fields["DT_CADASTRO_FAM"],
                "CO_EST_CADASTRAL_MEMB": "3",
                "NO_PESSOA": f"{first} {fake.last_name()} {fake.last_name()}".upper(),
                "NU_NIS_PESSOA": generate_nis(rng) if rng.random() < 0.97 else "",
                "CO_SEXO_PESSOA": str(sex),
                "DT_NASC_PESSOA": _date(birth),
                "CO_PARENTESCO_RF_PESSOA": "1" if head else str(rng.choice([2, 3, 3, 3, 4, 5, 6, 7])),
                "CO_RACA_COR_PESSOA": str(rng.choices([1, 2, 3, 4, 5], weights=[38, 12, 1, 48, 1])[0]),
                "NO_COMPLETO_MAE_PESSOA": f"{fake.first_name_female()} {fake.last_name()}".upper(),
                "NO_COMPLETO_PAI_PESSOA": f"{fake.first_name_male()} {fake.last_name()}".upper()
                if rng.random() < 0.6
                else "",
                "NU_CPF_PESSOA": generate_cpf(rng) if rng.random() < (0.97 if adult else 0.6) else "",
                "NU_IDENTIDADE_PESSOA": str(rng.randint(1_000_000, 99_999_999)) if adult else "",
                "NU_TITULO_ELEITOR_PESSOA": str(rng.randint(10**11, 10**12 - 1)) if adult else "",
                "CO_DEFICIENCIA_MEMB": "1" if disability else "2",
                "CO_SABE_LER_ESCREVER_MEMB": "1" if age >= 7 and rng.random() < 0.9 else "2",
                "IN_FREQUENTA_ESCOLA_MEMB": "1" if 6 <= age <= 17 and rng.random() < 0.92 else "2",
                "CO_TRABALHOU_SEMANA_MEMB": ("1" if rng.random() < 0.45 else "2") if adult else "",
                "CO_CART_ASSINADA_MEMB": ("1" if rng.random() < 0.3 else "2") if adult else "",
                "IN_DORMIR_RUA_MEMB": "0",
                "DS_MARC_BPC": "1" if disability and rng.random() < 0.4 else "0",
                "_age": str(age),
            }
        )
        return p

    def _new_family_code(self) -> int:
        self._next_family += self.rng.randint(1, 50)
        return self._next_family

    def _new_person_code(self) -> int:
        self._next_person += self.rng.randint(1, 50)
        return self._next_person

    # ------------------------------------------------------------ defects
    def family_rows(self, family: Family) -> list[list[str]]:
        rng = self.rng
        values = [family.fields[n] for n in FAMILY.names]
        idx = {n: i for i, n in enumerate(FAMILY.names)}
        if rng.random() < 0.02:  # the \x00 that legacy loads had to strip
            values[idx["NU_CEP_LOGRADOURO_FAM"]] += "\x00"
        if rng.random() >= self.defect_rate:
            return [values]
        defect = rng.choice(FAMILY_DEFECTS)
        if defect == "negative_income":
            values[idx["VL_RENDA_TOTAL_FAM"]] = "-" + _dec(rng.uniform(10, 500))
        elif defect == "unknown_municipality":
            values[idx["CD_IBGE_CADASTRO"]] = "9999999"
        elif defect == "invalid_date":
            values[idx["DT_ATUALIZACAO_FAM"]] = "31/02/2026"
        elif defect == "invalid_type":
            values[idx["QT_PESSOAS_DOMIC_FAM"]] = "dois"
        elif defect == "truncated_row":
            return [values[:-5]]
        elif defect == "duplicate":
            older = list(values)
            older[idx["DT_ATUALIZACAO_FAM"]] = family.fields["DT_CADASTRO_FAM"]
            older[idx["VL_RENDA_TOTAL_FAM"]] = _dec(family.total_income * 0.8)
            return [older, values]
        return [values]

    def person_row(self, person: dict[str, str]) -> list[str]:
        rng = self.rng
        values = [person[n] for n in PERSON.names]
        idx = {n: i for i, n in enumerate(PERSON.names)}
        if rng.random() < self.defect_rate:
            defect = rng.choice(["invalid_cpf", "invalid_nis", "future_birth", "missing_key"])
            if defect == "invalid_cpf" and values[idx["NU_CPF_PESSOA"]]:
                cpf = values[idx["NU_CPF_PESSOA"]]
                values[idx["NU_CPF_PESSOA"]] = cpf[:10] + str((int(cpf[10]) + 1) % 10)
            elif defect == "invalid_nis" and values[idx["NU_NIS_PESSOA"]]:
                nis = values[idx["NU_NIS_PESSOA"]]
                values[idx["NU_NIS_PESSOA"]] = nis[:10] + str((int(nis[10]) + 1) % 10)
            elif defect == "future_birth":
                values[idx["DT_NASC_PESSOA"]] = "01/01/2099"
            elif defect == "missing_key":
                values[idx["CO_CHV_NATURAL_PESSOA"]] = ""
        return values

    # ------------------------------------------------------------ beneficiaries and references
    def beneficiaries(
        self, families: list[Family], municipalities: list[Municipality], reference: date, bands: dict[int, float]
    ) -> list[list[str]]:
        rng, fake = self.rng, self.fake
        candidates = [f for f in families if f.members[0]["NU_CPF_PESSOA"] and f.fields["CO_EST_CADASTRAL_FAM"] == "3"]
        target = int(len(families) * 0.07)
        weights = [5.0 if f.in_deficit else 1.0 for f in candidates]
        chosen = {id(f): f for f in rng.choices(candidates, weights=weights, k=target * 2)} if candidates else {}
        rows: list[list[str]] = []

        def contract(program: str, cpf: str, name: str, birth: str, income: float, mun: Municipality, zip_code: str):
            band = 1 if program in ("FAR", "FDS", "PNHR") else band_for_income(income, bands)
            if rng.random() < 0.04:  # declared income above the band ceiling
                income = bands[band] * rng.uniform(1.1, 1.6)
            signed = reference - timedelta(days=rng.randint(30, 900))
            document = format_cpf(cpf) if rng.random() < 0.3 else cpf  # files arrive with and without mask
            values = [program, document, name, birth, _dec(income), str(mun.code), zip_code, _date(signed), str(band)]
            if rng.random() < self.defect_rate:
                defect = rng.choice(["invalid_cpf", "invalid_program", "negative_income"])
                if defect == "invalid_cpf":
                    values[1] = cpf[:10] + str((int(cpf[10]) + 1) % 10)
                elif defect == "invalid_program":
                    values[0] = "XYZ"
                else:
                    values[4] = "-100,00"
            return values

        for family in list(chosen.values())[:target]:
            head = family.members[0]
            program = "PNHR" if family.rural else rng.choices(["FAR", "FDS", "FGTS"], weights=[50, 10, 40])[0]
            income = max(family.total_income * rng.uniform(0.95, 1.25), 0.0)
            args = (head["NU_CPF_PESSOA"], head["NO_PESSOA"], head["DT_NASC_PESSOA"], income, family.municipality,
                    family.fields["NU_CEP_LOGRADOURO_FAM"])  # fmt: skip
            rows.append(contract(program, *args))
            if rng.random() < 0.015:  # same CPF in two contracts
                rows.append(contract(rng.choice(["FAR", "FGTS"]), *args))

        population_weights = [m.population for m in municipalities]
        for _ in range(int(target * 0.5)):  # contracts of people outside CadÚnico (FGTS, bands 2 to 4)
            mun = rng.choices(municipalities, weights=population_weights)[0]
            birth = reference - timedelta(days=rng.randint(20, 65) * 365)
            rows.append(
                contract(
                    "FGTS",
                    generate_cpf(rng),
                    fake.name().upper(),
                    _date(birth),
                    rng.uniform(2000, 11000),
                    mun,
                    re.sub(r"\D", "", fake.postcode()),
                )
            )
        return rows

    def fjp_deficit(self, municipalities: list[Municipality], year: int = 2022) -> list[list[str]]:
        rng = self.rng
        rows = []
        for m in municipalities:
            relative = _clamp(
                0.025 + 0.11 * m.precariousness + 0.08 * (m.renters - 0.25) + rng.gauss(0, 0.006), 0.005, 0.30
            )
            total = round(relative * m.households)
            precarious = round(total * _clamp(0.15 + 0.35 * m.precariousness, 0, 0.7))
            cohabitation = round(total * 0.25)
            overcrowding = round(total * 0.05)
            burden = max(total - precarious - cohabitation - overcrowding, 0)
            rows.append([str(x) for x in (m.code, year, total, precarious, cohabitation, burden, overcrowding)])
        return rows


def _write(path: Path, header: list[str], rows: list[list[str]], encoding: str) -> None:
    with path.open("w", encoding=encoding, errors="replace", newline="") as file:
        file.write(";".join(header) + "\n")
        for row in rows:
            file.write(";".join(row) + "\n")


def _months_before(d: date, months: int) -> date:
    year, month = d.year, d.month - months
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, min(d.day, 28))


def generate(
    output: str | Path,
    n_municipalities: int = 200,
    n_families: int = 20_000,
    months: int = 2,
    seed: int = 42,
    defect_rate: float = 0.01,
    last_date: date = date(2026, 9, 11),
    bands: dict[int, float] | None = None,
) -> dict[str, int]:
    """Writes `months` monthly family and person files, beneficiaries and references into `output`."""
    bands = bands or DEFAULT_BANDS
    target = Path(output)
    target.mkdir(parents=True, exist_ok=True)
    g = Generator(seed, defect_rate)
    rng = g.rng
    municipalities = g.municipalities(n_municipalities)
    weights = [m.population * (0.15 + 0.35 * m.precariousness) for m in municipalities]
    new_per_month = int(n_families * 0.02)
    references = [_months_before(last_date, months - 1 - k) for k in range(months)]

    families = [
        g.family(m, references[0])
        for m in rng.choices(municipalities, weights=weights, k=n_families - new_per_month * (months - 1))
    ]
    summary = {"municipalities": len(municipalities)}
    for k, ref in enumerate(references):
        if k > 0:
            for f in rng.sample(families, int(len(families) * 0.04)):  # registry updates during the month
                f.total_income = round(f.total_income * rng.uniform(0.7, 1.3), 2)
                f.fields["VL_RENDA_TOTAL_FAM"] = _dec(f.total_income)
                f.fields["DT_ATUALIZACAO_FAM"] = _date(ref - timedelta(days=rng.randint(1, 25)))
            families += [
                g.family(m, ref, new=True) for m in rng.choices(municipalities, weights=weights, k=new_per_month)
            ]
        suffix = ref.strftime("%d%m%Y")
        family_rows = [row for f in families for row in g.family_rows(f)]
        person_rows = [g.person_row(p) for f in families for p in f.members]
        _write(target / f"ARQ_FAMILIA_{suffix}_SYNTHETIC.TXT", FAMILY.names, family_rows, "iso-8859-1")
        _write(target / f"ARQ_PESSOA_{suffix}_SYNTHETIC.TXT", PERSON.names, person_rows, "iso-8859-1")
        summary[f"families_{suffix}"] = len(family_rows)
        summary[f"persons_{suffix}"] = len(person_rows)

    last = references[-1]
    beneficiaries = g.beneficiaries(families, municipalities, last, bands)
    _write(target / f"BENEFICIARIOS_MCMV_{last:%d%m%Y}.csv", BENEFICIARIES.names, beneficiaries, "utf-8")
    summary["beneficiaries"] = len(beneficiaries)
    _write(
        target / "MUNICIPIOS.csv",
        MUNICIPALITIES.names,
        [
            [str(m.code), m.name, m.state, str(m.state_code), m.region, str(m.population), str(m.households)]
            for m in municipalities
        ],  # fmt: skip
        "utf-8",
    )
    _write(target / "DEFICIT_FJP.csv", FJP_DEFICIT.names, g.fjp_deficit(municipalities), "utf-8")
    return summary
