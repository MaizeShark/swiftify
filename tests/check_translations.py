"""Check catalog validity, placeholders, and coverage of literal qsTr calls."""
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate translation: {key}")
        result[key] = value
    return result


catalog = json.loads((root / "translations/ru.json").read_text(encoding="utf-8"), object_pairs_hook=unique_object)
for source, translated in catalog.items():
    forms = translated if isinstance(translated, list) else [translated]
    if isinstance(translated, list):
        assert len(forms) == 3, source
    for form in forms:
        assert isinstance(form, str) and form.strip(), source
        assert sorted(re.findall(r"%(?:n|[1-9]\d*)", source)) == sorted(
            re.findall(r"%(?:n|[1-9]\d*)", form)
        ), f"Placeholders differ: {source}"

missing = []
for path in (root / "qml").rglob("*.qml"):
    for match in re.finditer(r'qsTr\(("(?:[^"\\]|\\.)*")', path.read_text(encoding="utf-8")):
        source = json.loads(match.group(1))
        if source not in catalog:
            missing.append(f"{path.relative_to(root)}: {source}")
if missing:
    raise SystemExit("Missing Russian translations:\n" + "\n".join(missing))
print(f"Validated {len(catalog)} Russian translations and QML coverage.")
