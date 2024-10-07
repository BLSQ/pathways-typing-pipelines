import json
from datetime import datetime
from pathlib import Path

from openhexa.sdk import current_run, parameter, pipeline, workspace
from pathways.typing.config import (
    get_choices,
    get_options,
    get_questions,
    read_spreadsheet,
    validate_config,
)
from pathways.typing.customize import calculate, split
from pathways.typing.mermaid import cart_diagram, form_diagram
from pathways.typing.tree import (
    build_binary_tree,
    build_xlsform,
    choices_worksheet,
    merge_trees,
    survey_worksheet,
    validate_xlsform,
)


@pipeline("create-xlsform", name="Create XLSForm")
@parameter(
    "config_spreadsheet",
    name="Configuration spreadsheet",
    help="Configuration spreadsheet URL in Google Sheets. Spreadsheet has to be shared publicly.",
    type=str,
    default="https://docs.google.com/spreadsheets/d/1BZPUBuF8sbLsegljCbYGeOXW6kYZmwaljQvIT8FC4DY/edit?usp=sharing",
    required=True,
)
@parameter(
    "src_cart_urban",
    name="CART output (urban)",
    help="JSON cart output for urban strata",
    type=str,
    default="data/sen/frame_urban.json",
    required=True,
)
@parameter(
    "src_cart_rural",
    name="CART output (rural)",
    help="JSON cart output for rural strata",
    type=str,
    default="data/sen/frame_rural.json",
    required=True,
)
@parameter(
    "output_dir",
    name="Output directory",
    help="Output directory where generated form is saved",
    type=str,
    default="data/sen",
    required=True,
)
def create_xlsform(
    config_spreadsheet: str, src_cart_urban: str, src_cart_rural: str, output_dir: str
):
    """Build XLSForm from CART outputs and configuration spreadsheet."""
    src_cart_urban = Path(workspace.files_path, src_cart_urban)
    src_cart_rural = Path(workspace.files_path, src_cart_rural)
    output_dir = Path(workspace.files_path, output_dir)

    config = load_configuration(url=config_spreadsheet)
    generate_form(config, src_cart_urban, src_cart_rural, output_dir)


def load_configuration(url: str) -> dict:
    """Load configuration from Google Sheets."""
    con = workspace.custom_connection("google-sheets")
    credentials = json.loads(con.credentials, strict=False)

    config = {}

    spreadsheet = read_spreadsheet(url, credentials)
    config["questions"] = get_questions(spreadsheet)
    config["choices"] = get_choices(spreadsheet)
    config["options"] = get_options(spreadsheet)

    return config


def generate_form(
    config: dict, src_cart_urban: Path, src_cart_rural: Path, output_dir: Path
):
    """Build XLSForm from CART outputs and configuration spreadsheet."""
    version = datetime.now().strftime("%Y%m%d%H%M%S")

    # load CART outputs as binary trees
    with open(src_cart_rural) as f:
        cart_rural = json.load(f)
        current_run.log_info(
            "Successfully loaded CART output for rural strata ({} nodes)".format(
                len(cart_rural)
            )
        )
    root_rural = build_binary_tree(cart_rural, strata="rural")
    with open(src_cart_urban) as f:
        cart_urban = json.load(f)
        current_run.log_info(
            "Successfully loaded CART output for urban strata ({} nodes)".format(
                len(cart_urban)
            )
        )
    root_urban = build_binary_tree(cart_urban, strata="urban")

    validate_config(config_data=config, cart_urban=cart_urban, cart_rural=cart_rural)

    # merge both binary trees into a single one, and set node attributes from config
    root = merge_trees(root_rural, root_urban)
    for node in root.preorder():
        node.from_config(
            questions_config=config["questions"], choices_config=config["choices"]
        )
        node.relevant = node.xpath_condition()

    # generate mermaid diagram for CART
    mermaid_cart = cart_diagram(root)
    fpath = output_dir / version / "mermaid_diagram_cart.txt"
    fpath.parent.mkdir(parents=True, exist_ok=True)
    with open(fpath, "w") as f:
        f.write(mermaid_cart)
    current_run.add_file_output(str(fpath.absolute()))
    current_run.log_info("Generated mermaid diagram for CART")

    n_nodes = sum([1 for _ in root.preorder()])
    current_run.log_info(
        "Merged rural and urban trees into a single tree with {} nodes".format(n_nodes)
    )

    # apply custom options from config spreadsheet
    for option in config["options"]:
        if option["option"] == "split":
            root = split(
                root=root,
                questions_config=config["questions"],
                choices_config=config["choices"],
                **option["config"],
            )

        elif option["option"] == "calculate":
            root = calculate(
                root=root,
                questions_config=config["questions"],
                choices_config=config["choices"],
                **option["config"],
            )

    # generate mermaid diagram for typing form
    mermaid_form = form_diagram(root)
    fpath = output_dir / version / "mermaid_diagram_form.txt"
    with open(fpath, "w") as f:
        f.write(mermaid_form)
    current_run.add_file_output(str(fpath.absolute()))
    current_run.log_info("Generated mermaid diagram for typing form")

    # build xlsform and validate with pyxform
    fpath = output_dir / version / f"form_{version}.xlsx"
    survey = survey_worksheet(root)
    choices = choices_worksheet(root)
    build_xlsform(survey, choices, fpath)
    validate_xlsform(fpath)
    current_run.log_info(f"XLSForm successfully generated at {fpath}")
    current_run.add_file_output(str(fpath.absolute()))


if __name__ == "__main__":
    create_xlsform()
