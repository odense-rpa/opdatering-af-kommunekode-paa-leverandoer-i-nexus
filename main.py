import argparse
import asyncio
import logging
import sys
import json
import os

from automation_server_client import (
    AutomationServer,
    Workqueue,
    WorkItemError,
    Credential,
    WorkItem,
)
from kmd_nexus_client import (
    NexusClientManager,
)
from odk_tools.tracking import Tracker
from odk_tools.reporting import report
from process.config import get_excel_mapping, load_excel_mapping

nexus: NexusClientManager
tracker: Tracker

proces_navn = "Opdatering af kommunekode på leverandør i Nexus"


async def populate_queue(workqueue: Workqueue):
    logger = logging.getLogger(proces_navn)
    regler = get_excel_mapping()
    leverandører = nexus.organisationer.hent_leverandører()
    irrelevante_leverandører = regler.get("Irrelevante leverandører", [])
    
    aktive_leverandører = [item for item in leverandører if item.get("active") is True]

    for leverandør in aktive_leverandører:
        try:
            if leverandør.get("name") in irrelevante_leverandører:                
                continue

            leverandør_objekt = nexus.hent_fra_reference(leverandør)

            kø_data = {
                "leverandør_id": leverandør_objekt["id"],
                "kommunekode": leverandør_objekt["address"]["administrativeAreaCode"],
                "postnummer": leverandør_objekt["address"]["postalCode"],
            }

            workqueue.add_item(
                kø_data, f"{leverandør.get('id')} - {leverandør.get('name')}"
            )
        except Exception as e:
            logger.error(f"Failed to add item to workqueue: {kø_data}. Error: {e}")
            raise WorkItemError(
                f"Failed to add item to workqueue: {kø_data}. Error: {e}"
            )


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(proces_navn)
    postnummer_kommunekode_mapping = None
    postnummer_kommunekode_mapping_filsti = "postnumre_med_kommunekode.json"

    leverandører = nexus.organisationer.hent_leverandører()

    with open(postnummer_kommunekode_mapping_filsti, "r", encoding="utf-8") as file:
        postnummer_kommunekode_mapping = json.load(file)

    for item in workqueue:
        with item:
            data = item.data

            try:
                kommunekode = kontroller_kommunekode(
                    item, data, postnummer_kommunekode_mapping
                )

                if kommunekode is None:
                    continue

                leverandør = kontroller_leverandør(data, leverandører)

                if leverandør is None:
                    continue

                if leverandør["address"]["administrativeAreaCode"] != kommunekode:
                    leverandør["address"]["administrativeAreaCode"] = kommunekode
                    nexus.organisationer.opdater_leverandør(leverandør)
                    tracker.track_task(proces_navn)

            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))


def kontroller_kommunekode(item: WorkItem, data: dict, mapping: list) -> str | None:
    logger = logging.getLogger(proces_navn)
    if data["postnummer"] is None:
        logger.info(f"Leverandør {item.reference} har ikke et postnummer angivet.")
        try:
            report(
                report_id="opdatering_af_kommunekode_paa_leverandoer_i_nexus",
                group="Manglende postnummer",
                json={
                    "Leverandør": item.reference                                            
                }
            )            
        except Exception as e:
            logger.error(
                f"Fejl ved rapportering for leverandør {item.reference} uden postnummer: {e}"
            )
        finally:
            return None

    kommunekode = next(
        (
            list_item
            for list_item in mapping
            if str(list_item["Postnr"]) == data["postnummer"]
        ),
        None,
    )

    if kommunekode is None:
        logger.info(
            f"Leverandør {item.reference} har et postnummer uden tilknyttet kommunekode: {data['postnummer']}"
        )
        try:
            report(
                report_id="opdatering_af_kommunekode_paa_leverandoer_i_nexus",
                group="Postnummer uden kommunekode",
                json={
                    "Leverandør": item.reference,
                    "Postnummer": data["postnummer"],
                }
            )
        except Exception as e:
            logger.error(
                f"Fejl ved rapportering for leverandør {item.reference} med postnummer {data['postnummer']}: {e}"
            )
        finally:
            return None

    return str(kommunekode["Kommunenr"])


def kontroller_leverandør(data: dict, leverandører: list) -> dict | None:
    leverandør = next(
        (rel for rel in leverandører if rel["id"] == data["leverandør_id"]),
        None,
    )

    if leverandør is None:
        return None

    leverandør = nexus.hent_fra_reference(leverandør)
    return leverandør


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    credential = Credential.get_credential("KMD Nexus - produktion")
    tracking_credential = Credential.get_credential("Odense SQL Server")
    
    nexus = NexusClientManager(
        client_id=credential.username,
        client_secret=credential.password,
        instance=credential.data["instance"],
    )

    tracker = Tracker(
        username=tracking_credential.username, password=tracking_credential.password
    )

    # Parse command line arguments
    parser = argparse.ArgumentParser(description=proces_navn)
    parser.add_argument(
        "--excel-file",
        default="./Regelsæt.xlsx",
        help="Path to the Excel file containing mapping data (default: ./Regelsæt.xlsx)",
    )
    parser.add_argument(
        "--queue",
        action="store_true",
        help="Populate the queue with test data and exit",
    )
    args = parser.parse_args()

    # Validate Excel file exists
    if not os.path.isfile(args.excel_file):
        raise FileNotFoundError(f"Excel file not found: {args.excel_file}")

    # Load excel mapping data once on startup
    load_excel_mapping(args.excel_file)

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue("new")
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
