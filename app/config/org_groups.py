from __future__ import annotations


DEFAULT_ORG_GROUPS = {
    "terror": [
        {"name": "PKK", "aliases": ["PKK", "KCK", "HPG", "YPG", "PYD", "TAK"]},
        {"name": "DHKP-C", "aliases": ["DHKP-C", "Dev-Sol"]},
        {"name": "FETÖ", "aliases": ["FETÖ", "PDY"]},
        {"name": "DEAŞ", "aliases": ["DEAŞ", "IŞİD", "ISIS", "ISIL"]},
        {"name": "MLKP", "aliases": ["MLKP"]},
        {"name": "TİKKO/TKP-ML", "aliases": ["TİKKO", "TKP-ML"]},
        {"name": "El-Kaide", "aliases": ["El-Kaide", "Al-Qaeda", "Al Qaida"]},
        {"name": "Hizbullah", "aliases": ["Hizbullah"]},
    ],
    "organized_crime": [
        {"name": "Daltons", "aliases": ["Daltons", "Daltonlar"]},
        {"name": "Sarallar", "aliases": ["Sarallar"]},
        {"name": "Cübbeliler", "aliases": ["Cübbeliler"]},
        {"name": "Gündoğmuşlar", "aliases": ["Gündoğmuşlar"]},
        {"name": "Anucurlar", "aliases": ["Anucurlar"]},
        {"name": "RedKitler", "aliases": ["RedKitler", "Red Kitler"]},
    ],
}


def get_seed_org_group_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for group_name, entries in DEFAULT_ORG_GROUPS.items():
        for entry in entries:
            rows.append(
                {
                    "group_type": group_name,
                    "canonical_name": entry["name"],
                    "aliases": list(entry["aliases"]),
                    "is_enabled": True,
                    "notes": None,
                }
            )
    return rows
