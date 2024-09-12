# dataset-update-example

This is a small example of how to update a Valohai dataset version
from an external source with only the files changed since the latest version.

> [!NOTE]
> This is meant as an "user-space" proof-of-concept to validate whether this approach
> would work; the plan is to provide a more fluent API to do this on Valohai's side.

The conceptual flow this is targeting is

```mermaid
graph TD
    src[External source] -..-> dl(Download data)
    dl --> cvt(Convert files into temp directory)
    api[Valohai API] -. Dataset version content .-> diff(Compute diff between temp directory and current dataset version)
    cvt --> diff
    diff -->|new and changed files|cp(Copy files to Valohai output directory, attach metadata)
    diff -->|version metadata| cp
```

The Valohai platform will then read the metadata sidecar from the uploaded outputs and
attach those outputs into a new dataset version, heeding the `"from"` and `"exclude"` clauses.

At present, diffing is done based on file size and checksums when available.
This can be changed as necessary.

The example project does not have any external source,
but instead uses the `new-file-names` and `new-file-contents`
parameters to simulate it. The values of `new-file-contents` are repeated in a round-robin
manner over the files in `new-file-names`.

## Usage (example)

1. Set up a Valohai project with the environment variable `VH_API_TOKEN` set to a valid API token.
2. Set up a dataset accessible to that project; take a note of that dataset's ID (and substitute it in the commands below).
3. Link this directory to that project.
4. Run an initial execution:
   - `vh exec run --adhoc Update --open-browser--dataset-id=DATASET_ID`
   - This will create a first version in the dataset with the files `file1.csv`, `file2.csv`, `file3.csv`.
   - The contents are `foofoo`, `barbar`, `foofoo`, respectively.
5. Run another execution:
   - `vh exec run --adhoc Update --open-browser --new-file-names=file1.csv --new-file-names=file3.csv --dataset-id=DATASET_ID`
   - This will create a second version in the dataset with the files `file1.csv` and `file3.csv`. `file2.csv` is dropped.
   - The contents of `file1.csv` will be `foofoo`, and the contents of `file3.csv` will be `barbar`.
   - This means that the contents of `file1.csv` are unchanged, and the contents of `file3.csv` are new.
6. Run another execution:
   - `vh exec run --adhoc Update --open-browser --new-file-names=file1.csv --new-file-names=file3.csv --new-file-contents=barbar --dataset-id=DATASET_ID`
   - This will create a third version in the dataset with the files `file1.csv` and `file3.csv`, both having the contents `barbar`.
