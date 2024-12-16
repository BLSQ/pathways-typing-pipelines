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
    "add_choice_labels",
    name="Add choice labels to links",
    help="Add valid choice labels to relationship arrows instead of yes/no",
    type=bool,
    required=False,
    default=False,
)
@parameter(
    "add_node_id",
    name="Add node indexes to mermaid shapes",
    help="Add node indexes to mermaid shapes in addition to split rule",
    type=bool,
    required=False,
    default=True,
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
    urban, rural, version = load_dataset(dataset=cart_outputs, version_name=version_name)

    output_dir = Path(
        workspace.files_path,
        output_dir,
        version,
        datetime.now().astimezone().strftime("%Y-%m-%d_%H:%M:%S"),
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    generate_mermaid(
        urban_cart=urban,
        rural_cart=rural,
        output_dir=output_dir,
        version_name=version,
    )


def load_dataset(
    dataset: Dataset, version_name: str | None = None
) -> tuple[list[dict], list[dict], str]:
    """Load urban and rural JSON files from dataset.

    Parameters
    ----------
    dataset : Dataset
        The dataset containing the urban and rural JSON files.
    version_name : str, optional
        The name of the dataset version to use. If not specified, the latest version is used.

    Return
    ------
    list[dict]
        The urban JSON-like CART data
    list[dict]
        The rural JSON-like CART data
    str
        The name of the dataset version
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
    urban: list[dict] = None
    rural: list[dict] = None
    for f in ds.files:
        if f.filename.endswith("cart_urban.json"):
            urban = json.loads(f.read().decode())
        if f.filename.endswith("cart_rural.json"):
            rural = json.loads(f.read().decode())

    if urban is None:
        msg = "Urban JSON file not found in dataset"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)
    if rural is None:
        msg = "Rural JSON file not found in dataset"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    return urban, rural, ds.name


def generate_mermaid(
    urban_cart: list[dict], rural_cart: list[dict], output_dir: Path, version_name: str
) -> None:
    """Generate a mermaid diagram from urban and rural CART outputs.

    Both trees are merged into a single tree before generating the diagram.

    Parameters
    ----------
    urban_cart : list[dict]
        The urban CART output (nodes as list of dicts)
    rural_cart : list[dict]
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
