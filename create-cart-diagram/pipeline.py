from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from openhexa.sdk import Dataset, current_run, parameter, pipeline, workspace
from pathways.typing.mermaid import create_cart_diagram
from pathways.typing.tree import build_tree, merge_trees, parse_rpart


@pipeline("create-cart-diagram", name="Create CART diagram")
@parameter(
    "cart_outputs",
    name="CART outputs",
    help="OpenHEXA dataset containing JSON CART outputs",
    type=Dataset,
    required=True,
)
@parameter(
    "version_name",
    name="Dataset version",
    help="You can specify the dataset version to use. If not specified, latest version is used.",
    type=str,
    required=False,
)
@parameter(
    "output_dir",
    name="Output directory",
    help="If not specified, outputs will be saved into `workspace/typing/data/output/cart_diagram`",
    default="typing/data/cart-diagram",
    type=str,
    required=False,
)
def generate_cart_diagram(
    cart_outputs: Dataset,
    version_name: str,
    output_dir: str,
) -> None:
    """Create a CART diagram from CART outputs."""
    data = load_dataset(dataset=cart_outputs, version_name=version_name)

    output_dir = Path(
        workspace.files_path,
        output_dir,
        data["version"],
        datetime.now().astimezone().strftime("%Y-%m-%d_%H:%M:%S"),
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    if "urban" and "rural" in data:
        generate_mermaid(
            urban_cart=data["urban"],
            rural_cart=data["rural"],
            output_dir=output_dir,
            version_name=data["version"],
        )
    else:
        generate_mermaid_single(
            single_cart=data["single"], output_dir=output_dir, version_name=data["version"]
        )


def load_dataset(dataset: Dataset, version_name: str | None = None) -> dict:
    """Load urban and rural JSON files from dataset.

    Parameters
    ----------
    dataset : Dataset
        The dataset containing the urban and rural JSON files.
    version_name : str, optional
        The name of the dataset version to use. If not specified, the latest version is used.
    """
    ds: Dataset = None

    # if a dataset version has been specified, use it
    # use the latest dataset version by default
    if version_name:
        for version in dataset.versions:
            if version.name == version_name:
                ds = version
                break

            if ds is None:
                msg = f"Dataset version `{version_name}` not found"
                current_run.log_error(msg)
                raise FileNotFoundError(msg)

    else:
        ds = dataset.latest_version

    # load urban & rural json files from dataset
    urban: dict | None = None
    rural: dict | None = None
    single: dict | None = None
    for f in ds.files:
        if f.filename.endswith("cart_urban.json"):
            urban = json.loads(f.read().decode())
        if f.filename.endswith("cart_rural.json"):
            rural = json.loads(f.read().decode())
        if f.filename == "cart.json":
            single = json.loads(f.read().decode())

    if urban is None and not single:
        msg = "Urban JSON file not found in dataset"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)
    if rural is None and not single:
        msg = "Rural JSON file not found in dataset"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)
    if not rural and not urban and not single:
        msg = "No CART JSON files found in dataset"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    if urban and rural:
        return {"urban": urban, "rural": rural, "version": ds.name}
    return {"single": single, "version": ds.name}


def generate_mermaid(
    urban_cart: dict, rural_cart: dict, output_dir: Path, version_name: str
) -> None:
    """Generate a mermaid diagram from urban and rural CART outputs.

    Both trees are merged into a single tree before generating the diagram.

    Parameters
    ----------
    urban_cart : dict
        The urban CART output (nodes as list of dicts)
    rural_cart : dict
        The rural CART output (nodes as list of dicts)
    output_dir : Path
        The output directory to save the diagram
    version_name : str
        The name of the dataset version
    """
    rural = parse_rpart(
        nodes=rural_cart["nodes"],
        ylevels=rural_cart["ylevels"],
        xlevels=rural_cart["xlevels"],
        csplit=rural_cart["csplit"],
    )
    current_run.log_info("Successfully parsed rural CART")

    urban = parse_rpart(
        nodes=urban_cart["nodes"],
        ylevels=urban_cart["ylevels"],
        xlevels=urban_cart["xlevels"],
        csplit=urban_cart["csplit"],
    )
    current_run.log_info("Successfully parsed urban CART")

    root_rural = build_tree(rural, strata="rural")
    root_urban = build_tree(urban, strata="urban")
    root = merge_trees(root_rural, root_urban)
    current_run.log_info("Successfully rebuilt CART tree")

    mermaid = create_cart_diagram(root=root)
    fp = output_dir / f"{version_name}.txt"
    with fp.open("w") as f:
        f.write(mermaid)

    current_run.add_file_output(fp.as_posix())


def generate_mermaid_single(single_cart: dict, output_dir: Path, version_name: str) -> None:
    """Generate a mermaid diagram from a single CART output.

    Parameters
    ----------
    single_cart : dict
        The single CART output (nodes as list of dicts)
    output_dir : Path
        The output directory to save the diagram
    version_name : str
        The name of the dataset version
    """

    single = parse_rpart(
        nodes=single_cart["nodes"],
        ylevels=single_cart["ylevels"],
        xlevels=single_cart["xlevels"],
        csplit=single_cart["csplit"],
    )
    current_run.log_info("Successfully parsed CART")

    root = build_tree(single, strata="urban")
    current_run.log_info("Successfully rebuilt CART tree")

    mermaid = create_cart_diagram(root=root)
    fp = output_dir / f"{version_name}.txt"
    with fp.open("w") as f:
        f.write(mermaid)

    current_run.add_file_output(fp.as_posix())
