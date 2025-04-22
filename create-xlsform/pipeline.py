from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import polars as pl
import xlsxwriter
from openhexa.sdk import Dataset, current_run, parameter, pipeline, workspace
from pathways.typing.config import get_config, read_google_spreadsheet
from pathways.typing.mermaid import create_form_diagram
from pathways.typing.options import (
    add_segment_notes,
    apply_options,
    enforce_relevance,
    set_choice_filters,
    skip_duplicate_questions,
)
from pathways.typing.tree import (
    build_tree,
    create_node_question,
    get_choices_rows,
    get_settings_rows,
    get_survey_rows,
    get_xlsform_relevance,
    merge_trees,
    parse_rpart,
)


@pipeline("create-xlsform")
@parameter(
    "config_spreadsheet",
    name="Configuration spreadsheet",
    help="Configuration spreadsheet URL in Google Sheets",
    type=str,
    default="https://docs.google.com/spreadsheets/d/1BZPUBuF8sbLsegljCbYGeOXW6kYZmwaljQvIT8FC4DY/edit?usp=sharing",
    required=True,
)
@parameter(
    "cart_outputs",
    name="CART outputs",
    help="OpenHEXA dataset containing JSON CART outputs",
    type=Dataset,
    required=True,
)
@parameter(
    "version_name",
    name="CART outputs (version)",
    help="You can specify the dataset version to use. If not specified, latest version is used.",
    type=str,
    required=False,
)
@parameter(
    "merge_duplicate_questions",
    name="Merge duplicate questions",
    help="Merge duplicate questions in the generated XLSForm",
    type=bool,
    required=False,
    default=True,
)
@parameter(
    "skip_unavailable_choices",
    name="Skip unavailable choices",
    help="Skip unavailable choices in the generated XLSForm",
    type=bool,
    required=False,
    default=False,
)
@parameter(
    "typing_tool_version",
    name="Typing tool version",
    help="Full version string of the generated typing tool",
    type=str,
    required=True,
)
@parameter(
    "output_dir",
    name="Output directory",
    help="Output directory where generated form is saved",
    type=str,
    default="typing/data/xlsform",
    required=True,
)
def create_xlsform(
    config_spreadsheet: str,
    cart_outputs: Dataset,
    version_name: str,
    merge_duplicate_questions: bool,
    skip_unavailable_choices: bool,
    typing_tool_version: str,
    output_dir: str,
) -> None:
    """Build XLSForm from CART outputs and configuration spreadsheet."""
    data = load_dataset(dataset=cart_outputs, version_name=version_name)
    config = load_configuration(url=config_spreadsheet)
    generate_form(
        config=config,
        cart_data=data,
        merge_duplicate_questions=merge_duplicate_questions,
        skip_unavailable_choices=skip_unavailable_choices,
        output_dir=output_dir,
    )


@create_xlsform.task
def load_dataset(dataset: Dataset, version_name: str | None = None) -> dict:
    """Load urban and rural JSON files from dataset.

    Parameters
    ----------
    dataset : Dataset
        The dataset containing the urban and rural JSON files.
    version_name : str, optional
        The name of the dataset version to use. If not specified, the latest version is used.

    Return
    ------
    dict
        The urban and rural JSON-like CART data with strata as key.
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

    return {"urban": urban, "rural": rural, "version": ds.name}


@create_xlsform.task
def load_configuration(url: str) -> dict:
    """Load configuration from Google Sheets."""
    con = workspace.custom_connection("google-sheets")
    credentials = json.loads(con.credentials, strict=False)
    spreadsheet = read_google_spreadsheet(url=url, credentials=credentials)
    return get_config(spreadsheet)


@create_xlsform.task
def generate_form(
    config: dict,
    cart_data: dict,
    merge_duplicate_questions: bool,
    skip_unavailable_choices: bool,
    output_dir: Path,
) -> None:
    """Build XLSForm from CART outputs and configuration spreadsheet."""
    rural_cart = cart_data["rural"]
    urban_cart = cart_data["urban"]
    version = cart_data["version"]

    output_dir = Path(
        workspace.files_path,
        output_dir,
        version,
        datetime.now().astimezone().strftime("%Y-%m-%d_%H-%M-%S"),
    )
    output_dir.mkdir(parents=True, exist_ok=True)

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

    for node in root.preorder():
        node.question = create_node_question(
            node,
            questions_config=config["questions"],
            choices_config=config["choices"],
            segments_config=config["segments"],
        )
    current_run.log_info("Initialized node questions")

    for node in root.preorder():
        relevant = get_xlsform_relevance(node)
        node.question.conditions = [relevant] if relevant else []
    current_run.log_info("Initialized node relevance rules")

    root = apply_options(
        root,
        options_config=config["options"],
        questions_config=config["questions"],
        choices_config=config["choices"],
    )
    current_run.log_info("Applied custom options")

    root = add_segment_notes(
        root, settings_config=config["settings"], segments_config=config["segments"]
    )
    current_run.log_info("Added segment notes")

    root = enforce_relevance(root)
    current_run.log_info("Enforced relevance rules")

    if skip_unavailable_choices:
        root = set_choice_filters(root)
        current_run.log_info("Filtered available choices")

    if merge_duplicate_questions:
        root = skip_duplicate_questions(root)
        current_run.log_info("Merged duplicate questions")

    rows = get_survey_rows(root, typing_group_label={"label::English (en)": "Typing"})
    survey = pl.DataFrame(rows, infer_schema_length=1000)

    rows = get_choices_rows(root)
    choices = pl.DataFrame(rows, infer_schema_length=1000)

    rows = get_settings_rows(settings_config=config["settings"])
    settings = pl.DataFrame(rows, infer_schema_length=1000)

    dst_file = Path(output_dir, f"{version}.xlsx")

    with xlsxwriter.Workbook(dst_file) as wb:
        survey.write_excel(wb, worksheet="survey")
        choices.write_excel(wb, worksheet="choices")
        settings.write_excel(wb, worksheet="settings")

    current_run.log_info(f"Successfully generated XLSForm at {dst_file}")
    current_run.add_file_output(dst_file.as_posix())

    mermaid = create_form_diagram(root, skip_notes=True)
    fp = output_dir / f"{version}.txt"
    with fp.open("w") as f:
        f.write(mermaid)
    current_run.add_file_output(fp.as_posix())


if __name__ == "__main__":
    create_xlsform()
