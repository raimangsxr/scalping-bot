from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]


def test_ci_image_is_loaded_for_local_sbom_scan() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    image_steps = workflow["jobs"]["image"]["steps"]
    build = next(step for step in image_steps if step.get("uses") == "docker/build-push-action@v6")
    sbom = next(step for step in image_steps if step.get("uses") == "anchore/sbom-action@v0")

    assert build["with"]["load"] is True
    assert sbom["with"]["image"] == "scalping-bot:${{ github.sha }}"
