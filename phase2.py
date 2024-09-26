import json
import os
import pathlib
from dataclasses import dataclass
from itertools import count
from typing import Iterable

import httpx

from helpers import get_api_client, MANIFEST_FILENAME


@dataclass(frozen=True)
class ManifestData:
    dataset_id: str
    exclude: list[str]
    execution_id: str
    keep: dict[str, str]
    new: list[str]
    new_version_name: str
    new_version_uri: str
    old_version_id: str
    old_version_uri: str


def read_manifest() -> ManifestData:
    manifest_input_dir = (
        pathlib.Path(os.environ.get("VH_INPUTS_DIR", "/valohai/inputs")) / "manifest"
    )
    for pth in manifest_input_dir.glob("*.json"):
        return ManifestData(**json.loads(pth.read_text()))
    raise FileNotFoundError("No manifest file found")


def get_execution_output_datums(
    api_client: httpx.Client,
    execution_id: str,
) -> Iterable[dict]:
    limit = 100
    for page in count(0):
        offset = page * limit
        resp = api_client.get(
            "/api/v0/data/",
            params={
                "ordering": "id",
                "limit": limit,
                "offset": offset,
                "output_execution": execution_id,
                "exclude": "output_execution,project",
                "purged": "false",
            },
        )
        resp.raise_for_status()
        results = resp.json()["results"]
        if not results:
            break
        yield from results


def main() -> None:
    manifest = read_manifest()
    with get_api_client() as api_client:
        datum_ids_from_phase1_execution = set()
        for datum in get_execution_output_datums(api_client, manifest.execution_id):
            if datum["name"] == MANIFEST_FILENAME:
                continue
            datum_ids_from_phase1_execution.add(datum["id"])
        datum_ids_to_keep = set(manifest.keep.values())
        datum_ids = datum_ids_from_phase1_execution | datum_ids_to_keep
        create_version_api_payload = {
            "previous_version": manifest.old_version_id,
            "name": manifest.new_version_name,
            "dataset": manifest.dataset_id,
            "files": [{"datum": datum_id} for datum_id in datum_ids],
        }
        print(
            f"Creating dataset version {manifest.new_version_name!r} with {len(datum_ids)} files",
        )
        resp = api_client.post(
            "/api/v0/dataset-versions/",
            json=create_version_api_payload,
            timeout=30,
        )
        if resp.status_code < 400:
            print("OK: Created dataset version", resp.json()["id"])
        else:
            raise ValueError(f"Failed to create dataset version: {resp.text}")


if __name__ == "__main__":
    main()
