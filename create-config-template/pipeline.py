"""Create form configuration templates for the typing tool.

Template generation is based on:
    - CART outputs
    - Segmentation outputs
"""

import json
from datetime import datetime
from io import BytesIO
from pathlib import Path

import polars as pl
import xlsxwriter
from openhexa.sdk import Dataset, current_run, parameter, pipeline, workspace
from pathways.typing.template import (
    get_unique_values,
    get_variables,
    guess_data_types,
    write_choices,
    write_form_settings,
    write_options,
    write_questions,
    write_segments,
)


@pipeline("pw-create-config-template", name="Create configuration template")
@parameter("cart_outputs", name="CART", help="JSON CART outputs", type=Dataset, required=True)
@parameter(
    "segmentation_outputs",
    name="Segmentation",
    help="Segmentation outputs",
    type=Dataset,
    required=True,
)
@parameter(
    "output_dir",
    name="Output directory",
    help="Output directory of the configuration template",
    type=str,
    default="typing/data/configuration-template",
    required=True,
)
def create_config_template(
    cart_outputs: Dataset, segmentation_outputs: Dataset, output_dir: str
) -> None:
    """Create form configuration template."""
    segmentation = None
    ds = segmentation_outputs.latest_version
    for f in ds.files:
        if f.filename.endswith(".parquet"):
            segmentation = pl.read_parquet(BytesIO(f.read()))
    if segmentation is None:
        msg = "No segmentation file found"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    cart_rural = None
    ds = cart_outputs.latest_version
    for f in ds.files:
        if f.filename.endswith("cart_rural.json"):
            cart_rural = json.load(BytesIO(f.read()))
    if cart_rural is None:
        msg = "No cart_rural.json file found"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    cart_urban = None
    for f in ds.files:
        if f.filename.endswith("cart_urban.json"):
            cart_urban = json.load(BytesIO(f.read()))
    if cart_urban is None:
        msg = "No cart_urban.json file found"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    output_dir = Path(workspace.files_path, output_dir)
    output_dir = output_dir / datetime.now().astimezone().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_f = output_dir / f"{ds.name}.xlsx"

    variables = get_variables(cart_rural) + get_variables(cart_urban)
    variables = sorted(set(variables))

    unique_values = get_unique_values(segmentation, variables)
    dtypes = guess_data_types(unique_values)

    with xlsxwriter.Workbook(output_f) as workbook:
        write_questions(workbook, variables=variables, dtypes=dtypes)
        write_choices(workbook, variables=variables, unique_values=unique_values, dtypes=dtypes)
        write_options(workbook)
        write_segments(
            workbook, ylevels_rural=cart_rural["ylevels"], ylevels_urban=cart_urban["ylevels"]
        )
        write_form_settings(workbook)


if __name__ == "__main__":
    create_config_template()
