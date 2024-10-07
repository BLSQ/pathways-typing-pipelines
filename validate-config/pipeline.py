import json
from pathlib import Path

from openhexa.sdk import current_run, parameter, pipeline, workspace
from pathways.typing.config import (
    get_choices,
    get_options,
    get_questions,
    read_spreadsheet,
    validate_config,
)
from pathways.typing.exceptions import ConfigError


@pipeline("validate-config", name="Validate configuration")
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
def validate_config_spreadsheet(
    config_spreadsheet: str, src_cart_urban: str, src_cart_rural: str
):
    """Validate config spreadsheet."""
    src_cart_urban = Path(workspace.files_path, src_cart_urban)
    src_cart_rural = Path(workspace.files_path, src_cart_rural)

    config = load_configuration(url=config_spreadsheet)
    validate(config, src_cart_urban, src_cart_rural)


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


def validate(config: dict, src_cart_urban: Path, src_cart_rural: Path):
    """Build XLSForm from CART outputs and configuration spreadsheet."""
    with open(src_cart_rural) as f:
        cart_rural = json.load(f)
    with open(src_cart_urban) as f:
        cart_urban = json.load(f)

    try:
        validate_config(
            config_data=config, cart_urban=cart_urban, cart_rural=cart_rural
        )
    except ConfigError as e:
        current_run.log_error(e)
        raise


if __name__ == "__main__":
    validate_config_spreadsheet()
