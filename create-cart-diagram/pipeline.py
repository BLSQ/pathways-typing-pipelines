import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from openhexa.sdk import Dataset, current_run, parameter, pipeline, workspace
from pathways.typing.mermaid import cart_diagram
from pathways.typing.tree import build_binary_tree, merge_trees


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
    help="You can optionally specify the dataset version to use. If not specified, the latest version will be used.",
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
    type=str,
    required=False,
)
def create_cart_diagram(
    cart_outputs: Dataset,
    version_name: Optional[str],
    add_choice_labels: Optional[bool],
    add_node_id: Optional[bool],
    output_dir: Optional[str],
):
    """Create a CART diagram from CART outputs."""

    urban, rural, version = load_dataset(
        dataset=cart_outputs, version_name=version_name
    )

    if output_dir:
        output_dir = Path(workspace.files_path, output_dir)
    else:
        output_dir = Path(
            workspace.files_path,
            "typing",
            "data",
            "output",
            "cart_diagram",
            version,
            datetime.now().strftime("%Y-%m-%d_%H:%M:%S"),
        )
    output_dir.mkdir(parents=True, exist_ok=True)

    generate_diagram(
        urban_cart=urban,
        rural_cart=rural,
        output_dir=output_dir,
        version_name=version,
    )


def load_dataset(
    dataset: Dataset, version_name: str | None = None
) -> Tuple[list[dict], list[dict], str]:
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
        if f.filename.endswith("urban.json"):
            urban = json.loads(f.read().decode())
        if f.filename.endswith("rural.json"):
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


def generate_diagram(
    urban_cart: list[dict], rural_cart: list[dict], output_dir: Path, version_name: str
):
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
    try:
        urban = build_binary_tree(urban_cart, strata="urban")
        current_run.log_info("Successfully built urban tree")
    except Exception as e:
        current_run.log_error("Could not build urban tree")
        raise e

    try:
        rural = build_binary_tree(rural_cart, strata="rural")
        current_run.log_info("Successfully built rural tree")
    except Exception as e:
        current_run.log_error("Could not build rural tree")
        raise e

    try:
        root = merge_trees(urban, rural)
        current_run.log_info("Successfully merged urban and rural trees")
    except Exception as e:
        current_run.log_error("Could not merge urban and rural trees")
        raise e

    try:
        mermaid = cart_diagram(root)
        current_run.log_info("Successfully generated mermaid diagram")
    except Exception as e:
        current_run.log_error("Could not generate mermaid diagram")
        raise e

    fp = output_dir / f"{version_name}_diagram.txt"
    with open(fp, "w") as f:
        f.write(mermaid)

    current_run.add_file_output(fp.absolute().as_posix())
