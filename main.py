import asyncio
import logging
import sys
import json

from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential
from kmd_nexus_client import (
    NexusClient,
    CitizensClient,
    OrganizationsClient   
)
from odk_tools.tracking import Tracker
from odk_tools.reporting import Reporter

nexus_client: NexusClient = None
citizens_client: CitizensClient = None
organizations_client: OrganizationsClient = None
tracker: Tracker = None
reporter: Reporter = None
logger = None
process_name = "Opdatering af kommunekode på leverandør i Nexus"

async def populate_queue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)
    leverandører = organizations_client.get_suppliers()    
    aktive_leverandører = [item for item in leverandører if item.get("active") is True]
    
    for leverandør in aktive_leverandører:
        try:
            leverandør_objekt = citizens_client.resolve_reference(leverandør)
            
            kø_data = {
                "leverandør_id": leverandør_objekt["id"],
                "kommunekode": leverandør_objekt["address"]["administrativeAreaCode"],
                "postnummer": leverandør_objekt["address"]["postalCode"]
            }

            workqueue.add_item(kø_data, f"{leverandør.get('id')} - {leverandør.get('name')}")
        except Exception as e:
            logger.error(f"Failed to add item to workqueue: {kø_data}. Error: {e}")
            raise WorkItemError(f"Failed to add item to workqueue: {kø_data}. Error: {e}")


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)    
    postnummer_kommunekode_mapping = None
    postnummer_kommunekode_mapping_filsti = "postnumre_med_kommunekode.json"

    leverandører = organizations_client.get_suppliers()
    
    with open(postnummer_kommunekode_mapping_filsti, "r", encoding="utf-8") as file:
        postnummer_kommunekode_mapping = json.load(file)

    for item in workqueue:
        with item:
            data = item.get_data_as_dict()            

            try:
                kommunekode = kontroller_kommunekode(item, data, postnummer_kommunekode_mapping)
                
                if kommunekode is None:
                    continue

                leverandør = kontroller_leverandør(data, leverandører)

                if leverandør is None:                    
                    continue
                
                if leverandør["address"]["administrativeAreaCode"] != kommunekode:
                    leverandør["address"]["administrativeAreaCode"] = kommunekode
                    organizations_client.update_supplier(leverandør)
                    tracker.track_task(process_name)
                
                pass
            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))

def kontroller_kommunekode(item: any, data: dict, mapping: list) -> str | None:
    if(data["postnummer"] is None):        
        reporter.report(
            process_name,
            "Manglende postnummer",
            {"Leverandør": item.reference},
        )
        return None

    kommunekode = next((list_item for list_item in mapping if str(list_item["Postnr"]) == data["postnummer"]), None)

    if kommunekode is None:
        reporter.report(            
            process_name,
            "Postnummer uden kommunekode",
            {"Leverandør": item.reference, "Postnummer": data["postnummer"]},
        )
        return None
    
    return str(kommunekode["Kommunenr"])

def kontroller_leverandør(data: dict, leverandører: list) -> dict | None:    
    leverandør = next(
        (
            rel
            for rel in leverandører
            if rel["id"] == data["leverandør_id"]
        ),
        None,
    )
    
    if leverandør is None:
        return None

    leverandør = citizens_client.resolve_reference(leverandør)
    return leverandør
    
if __name__ == "__main__":
    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    credential = Credential.get_credential("KMD Nexus - produktion")
    tracking_credential = Credential.get_credential("Odense SQL Server")
    reporting_credential = Credential.get_credential("RoboA")

    nexus_client = NexusClient(
        client_id=credential.username,
        client_secret=credential.password,
        instance=credential.get_data_as_dict()["instance"],
    )
    citizens_client = CitizensClient(nexus_client=nexus_client)
    organizations_client = OrganizationsClient(nexus_client=nexus_client)    

    tracker = Tracker(
        username=tracking_credential.username, password=tracking_credential.password
    )

    reporter = Reporter(
        username=reporting_credential.username, password=reporting_credential.password
    )

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue("new")
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
