import json

import pytest

from build_public_release import scan_public_text


@pytest.mark.parametrize("path", ["settings.json", "tests/test_credentials.py", ".env.example"])
@pytest.mark.parametrize("style", ["json", "environment"])
def test_public_scan_rejects_key_literals_in_every_directory(path, style):
    synthetic = "0x" + "a1" * 32
    content = (json.dumps({"private_key": synthetic}) if style == "json"
               else "API_PRIVATE_KEY=" + synthetic)
    findings = scan_public_text(path, content)
    assert findings == [{"file": path, "kind": "literal_private_key"}]
    assert synthetic not in json.dumps(findings)


@pytest.mark.parametrize("separator", ["/", chr(92), chr(92) * 2])
def test_public_scan_rejects_arbitrary_windows_user_paths(separator):
    path = separator.join(["D:", "Users", "example-person", "project", "config.json"])
    assert scan_public_text("notes.md", path) == [
        {"file": "notes.md", "kind": "personal_absolute_path"}
    ]


def test_public_scan_accepts_empty_templates_and_relative_paths():
    content = json.dumps({"private_key": "", "proxy": "", "state_dir": "state"})
    assert scan_public_text("wallet.example.json", content) == []
