import datetime
import hashlib
import json
import os
import pathlib
import shutil
import tempfile
from collections.abc import Iterator

import httpx
from attr import dataclass

from helpers import get_api_client, MANIFEST_FILENAME


@dataclass(frozen=True)
class LatestVersionData:
    dataset_dict: dict
    version_dict: dict | None = None


def get_latest_version_data(
    api_client: httpx.Client,
    dataset_id: str,
) -> LatestVersionData:
    """Return the info for the latest version in the dataset."""
    dataset_resp = api_client.get(f"/api/v0/datasets/{dataset_id}/")
    dataset_resp.raise_for_status()
    dataset_data = dataset_resp.json()
    latest_version_info = dataset_data.get("latest_version")
    if latest_version_info is None:
        return LatestVersionData(dataset_dict=dataset_data)
    latest_version_id = latest_version_info["id"]
    latest_version_resp = api_client.get(
        f"/api/v0/dataset-versions/{latest_version_id}/",
    )
    latest_version_resp.raise_for_status()
    return LatestVersionData(
        dataset_dict=dataset_data,
        version_dict=latest_version_resp.json(),
    )


def get_change_reason(local_path: pathlib.Path, datum: dict) -> str | None:
    local_stat = local_path.stat()
    if local_stat.st_size != datum["size"]:
        return "size changed"
    for hash_algo in ("md5", "sha1", "sha256"):
        if datum_hash := datum.get(hash_algo):
            with local_path.open("rb") as f:
                hasher = getattr(hashlib, hash_algo)
                local_hash = hasher(f.read()).hexdigest()
            if local_hash != datum_hash:
                return f"{hash_algo} changed"
    return None


@dataclass(frozen=True)
class CompareResult:
    new_name_to_path: dict[str, pathlib.Path]
    old_name_to_datum_id: dict[str, str]
    exclude_names: set[str]


def compare_dir_to_dataset_version(
    content_dir: pathlib.Path,
    version_files: list[dict],
) -> CompareResult:
    content_files = {p.name: p for p in content_dir.iterdir() if p.is_file()}
    old_version_name_to_datum = {
        file["datum"]["name"]: file["datum"] for file in version_files
    }

    new_name_to_path = {}
    old_name_to_datum_id = {}
    exclude_names = set()

    for name, path in content_files.items():
        if name not in old_version_name_to_datum:
            new_name_to_path[name] = path
            print(f"{name}: new file, will to upload")

    for name, datum_info in old_version_name_to_datum.items():
        local_file_path = content_files.get(name)
        if not local_file_path:
            print(f"{name}: not in local directory, will not keep in dataset")
            exclude_names.add(name)
            continue
        change_reason = get_change_reason(local_file_path, datum_info)
        if change_reason:
            print(f"{name}: changed – {change_reason}, need to upload")
            new_name_to_path[name] = local_file_path
            # Make sure we don't keep the old version by any chance
            exclude_names.add(name)
        else:
            print(f"{name}: no changes detected. Keeping in dataset")
            old_name_to_datum_id[name] = datum_info["id"]

    return CompareResult(
        new_name_to_path=new_name_to_path,
        old_name_to_datum_id=old_name_to_datum_id,
        exclude_names=exclude_names,
    )


def do_dataset_compare_and_move_content(
    api_client: httpx.Client,
    dataset_id: str,
    content_dir: pathlib.Path,
) -> None:
    output_dir = pathlib.Path("/valohai/outputs")
    lvd = get_latest_version_data(api_client, dataset_id)
    dataset_name = lvd.dataset_dict["name"]
    timestamp = datetime.datetime.now(datetime.UTC)
    new_version_name = timestamp.strftime("%Y%m%d-%H%M%S")
    new_version_uri = f"dataset://{dataset_name}/{new_version_name}"
    old_version_dict = lvd.version_dict
    if old_version_dict:  # Have an old version?
        old_version_uri = f"dataset://{dataset_name}/{old_version_dict['name']}"
    else:
        old_version_uri = None

    print("Old version URI:", old_version_uri)
    print("New version URI:", new_version_uri)

    cr = compare_dir_to_dataset_version(
        content_dir,
        version_files=old_version_dict["files"] if old_version_dict else [],
    )

    # Generate a manifest file
    manifest_data = {
        "dataset_id": dataset_id,
        "exclude": sorted(cr.exclude_names),
        "execution_id": os.environ.get("VH_EXECUTION_ID"),
        "keep": cr.old_name_to_datum_id,
        "new": sorted(cr.new_name_to_path),
        "new_version_name": new_version_name,
        "new_version_uri": new_version_uri,
        "old_version_id": old_version_dict["id"] if old_version_dict else None,
        "old_version_uri": old_version_uri,
    }
    (output_dir / MANIFEST_FILENAME).write_text(json.dumps(manifest_data))

    # Move the new files into place
    for name, path in cr.new_name_to_path.items():
        output_path = output_dir / name
        if path != output_path:
            shutil.move(path, output_path)


def generate_new_content(
    new_file_names: list[str],
    new_file_contents: list[str],
) -> pathlib.Path:
    # This would be replaced by your own code that retrieves and converts the new content
    content_dir = pathlib.Path(tempfile.mkdtemp())

    def content_gen() -> Iterator[str]:
        while True:
            yield from new_file_contents

    contents = content_gen()
    for name in new_file_names:
        (content_dir / name).write_text(next(contents))
    return content_dir


def main() -> None:
    params_file = (
        pathlib.Path(os.environ.get("VH_CONFIG_DIR", "/valohai/config"))
        / "parameters.json"
    )
    if params_file.is_file():
        params = json.loads(params_file.read_text())
    else:
        params = {}
    print("Parameters:", params)
    dataset_id = params.get("dataset-id")

    if not dataset_id:
        raise ValueError("No dataset ID provided")

    content_dir = generate_new_content(
        new_file_names=params.get(
            "new-file-names",
            ["file1.csv", "file2.csv", "file3.csv"],
        ),
        new_file_contents=params.get("new-file-contents", ["foofoo", "barbar"]),
    )

    with get_api_client() as api_client:
        do_dataset_compare_and_move_content(api_client, dataset_id, content_dir)


if __name__ == "__main__":
    main()
