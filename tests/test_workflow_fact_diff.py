import json
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).parent.parent / ".github/workflows/update_schedules.yml"


def load_fact_diff_script() -> str:
    text = WORKFLOW.read_text(encoding="utf-8")
    match = re.search(
        r"id: fact_diff\n\s+run: \|\n(.*?)(?=\n      - name:)", text, re.S
    )
    assert match, "fact_diff step not found"
    return textwrap.dedent(match.group(1))


@unittest.skipUnless(shutil.which("jq") and shutil.which("git"), "потрібні jq і git")
class FactDiffStepTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.repo = Path(self._tmp.name)
        self.output = self.repo / "github_output"
        self.output.touch()
        self.git("init", "-q")
        self.git("config", "user.email", "t@example.com")
        self.git("config", "user.name", "t")
        (self.repo / "out").mkdir()

    def git(self, *args: str) -> None:
        subprocess.run(["git", *args], cwd=self.repo, check=True)

    def commit_json(self, data: dict) -> None:
        (self.repo / "out/cek.json").write_text(json.dumps(data))
        self.git("add", "out/cek.json")
        self.git("commit", "-q", "-m", "init")

    def write_json(self, data: dict) -> None:
        (self.repo / "out/cek.json").write_text(json.dumps(data))

    def run_step(self) -> str:
        subprocess.run(
            ["bash", "-e", "-c", load_fact_diff_script()],
            cwd=self.repo,
            check=True,
            env={"GITHUB_OUTPUT": str(self.output), "PATH": "/usr/bin:/bin"},
            capture_output=True,
        )
        return self.output.read_text().strip()

    def test_empty_data_is_not_changed(self) -> None:
        self.commit_json({"fact": {"data": {"1": {}}, "today": 1}})
        self.write_json({"fact": {"data": {}, "today": 1}})
        self.assertEqual(self.run_step(), "changed=false")

    def test_empty_data_without_previous_commit_is_not_changed(self) -> None:
        self.write_json({"fact": {"data": {}, "today": 1}})
        self.assertEqual(self.run_step(), "changed=false")

    def test_missing_fact_is_not_changed(self) -> None:
        self.commit_json({"fact": {"data": {"1": {}}, "today": 1}})
        self.write_json({})
        self.assertEqual(self.run_step(), "changed=false")

    def test_changed_data(self) -> None:
        self.commit_json({"fact": {"data": {"1": {}}, "today": 1}})
        self.write_json({"fact": {"data": {"2": {}}, "today": 1}})
        self.assertEqual(self.run_step(), "changed=true")

    def test_unchanged_data(self) -> None:
        data = {"fact": {"data": {"1": {}}, "today": 1}}
        self.commit_json(data)
        self.assertEqual(self.run_step(), "changed=false")

    def test_first_run_with_data_is_changed(self) -> None:
        self.write_json({"fact": {"data": {"1": {}}, "today": 1}})
        self.assertEqual(self.run_step(), "changed=true")


if __name__ == "__main__":
    unittest.main()
