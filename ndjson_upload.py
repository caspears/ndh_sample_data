
import sys
import os.path
import glob
import time
import errno
import argparse
import requests


from urllib.parse import urlparse
import json
from json import dumps, loads
from requests import post, get, delete
import sqlite3
from pandas import *
import uuid
from nameparser import HumanName

from fhirclient import client
import fhirclient.models.bundle as bundle
import fhirclient.models.capabilitystatement as CS
import fhirclient.models.organization as Organization
import fhirclient.models.endpoint as Endpoint
import fhirclient.models.consent as consent
import fhirclient.models.meta as meta
import fhirclient.models.identifier as id
import fhirclient.models.extension as ext
import fhirclient.models.codeableconcept as CC
import fhirclient.models.contactpoint as ContactPoint
import fhirclient.models.coding as coding
import fhirclient.models.fhirreference as ref
import fhirclient.models.operationoutcome as OperationOutcome


def get_json_files(directory) -> list[str]:
    """
    Recursively find all JSON and NDJSON files in the given directory.
    
    Args:
        directory (str): The directory path to search
        
    Returns:
        list: A list of file paths for all JSON and NDJSON files found
    """
    json_files = []
    
    # Walk through all subdirectories
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.json', '.ndjson')):
                json_files.append(os.path.join(root, file))
    
    # Sort the files for consistent processing order
    json_files.sort()
    return json_files


def main():

    print('....processing.....')


    fhirSettings = {}


    parser = argparse.ArgumentParser(description="""Update FHIR Server with data From ndjson file""")
    parser.add_argument("-s", "--server", type=fhir_url, help="Server URL to make changes to")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("-f", "--file", type=file_path, help="[ND]JSON File")
    input_group.add_argument("-d", "--directory", type=dir_path, help="Directory containing [ND]JSON Files")
    action_group = parser.add_mutually_exclusive_group(required=False)
    action_group.add_argument("-u", "--upload", help="Upload without validating", action='store_true')
    action_group.add_argument("-v", "--valid_upload", help="Upload after validating with no errors", action='store_true')
    action_group.add_argument("--delete", help="Delete Connectathon Resources from the National Directory FHIR Server", action='store_true')
    # TODO Add silent mode
    # Add create after validate without errors
    # Add support for wildcards (multiple files)
    # Support client vs server assigned IDs?
    # Support batches with batch size specified by user (will create a batch of records in a batch (or transaction???) bundle )
    args = parser.parse_args()

    fhirClient = None

    if(args.server != None):
        fhirSettings = {}
        fhirSettings['app_id'] = "Data Server Import App"
        fhirSettings['api_base'] = args.server
    
    args.server = args.server.rstrip("/")
    #conman_file = sys.argv[1]
    #if (args.delete):
        # TODO Delete  
        #deleteConnectathonRecordsOnServer(fhirClient)
    #else:
        
    #client.FHIRClient(settings=settings)
    ### Load NPI
    start = time.time()


    line_count = 0
    files_to_process = []

    # Determine which files to process
    if args.file:
        # Single file processing
        files_to_process = [args.file]
        print("Processing file:", args.file)
    elif args.directory:
        # Directory processing - recursively find all JSON and NDJSON files
        print("Processing directory:", args.directory)
        files_to_process = get_json_files(args.directory)
        print(f"Found {len(files_to_process)} JSON/NDJSON files to process")

    
    # Process all files
    for path in files_to_process:
        if path.endswith('.ndjson'):
            # Process NDJSON file line by line
            with open(path, 'r', encoding='utf-8-sig') as f:
                for line in f:
                    if(len(line) > 10):
                        process_resource(args, line)
                        line_count = line_count + 1
                        if(line_count % 5 == 0):
                            print("Processed line ", line_count)
        elif path.endswith('.json'):
            # Process JSON file as a single resource
            with open(path, 'r', encoding='utf-8-sig') as f:
                content = f.read().strip()
                if content:
                    process_resource(args, content)
                    line_count = line_count + 1
                    if(line_count % 5 == 0):
                        print("Processed line ", line_count)
    
    
    
    end = time.time()
    print("Execution time (in minutes)", (end - start)/60)




def process_resource(args, res):
    
    # TODO How to do dynamic datatyping
    jsondict=json.loads(res)
    if('resourceType' in jsondict):
        resourceType = jsondict['resourceType']
        id = ""
        if('id' in jsondict):
            id = jsondict['id']
        # TODO Need to add content headers
        if(args.valid_upload):
            outcome = requests.post(args.server + "/" + resourceType + "/$validate?mode=create", json = jsondict)
            if(outcome.status_code == 200):
                outcome = OperationOutcome.OperationOutcome(json.loads(outcome.content))
                hasError = False
                #print(json.dumps(outcome.as_json()))
                for issue in outcome.issue:
                    #OperationOutcomeIssue
                    #if issue\
                    if(issue.severity == "error"):
                        hasError = True
                        break
                # TODO, the writes should be batched up for efficiency
                # Need to add error handling and put outcomes and erred data somewhere to be managed
                if(hasError == False and args.valid_upload):
                    outcome = requests.put(args.server + "/" + resourceType + "/" + id, json = jsondict)
                    if((outcome.status_code == 200) or (outcome.status_code == 201)):
                        print("Validated Resource Put to server:" + resourceType + " - id:" + id)
                    else:
                        print("Error (" + str(outcome.status_code) + ") Put validated resource to server:" + resourceType + " - id:" + id)
                else:
                    print("Validation Issues:" + resourceType + " - id:" + id)
        
        if(args.upload):
            outcome = requests.put(args.server + "/" + resourceType + "/" + id, json = jsondict)
            #if((outcome.status_code == 200) or (outcome.status_code == 201)):
            #    print("Unvalidated Resource Put to server:" + resourceType + " - id:" + id)
            #else:
            #    print("Error (" + str(outcome.status_code) + ") Put unvalidated resource to server:" + resourceType + " - id:" + id)
            if((outcome.status_code != 200) and (outcome.status_code != 201)):
                print("Error (" + str(outcome.status_code) + ") Put unvalidated resource to server:" + resourceType + " - id:" + id)
                outcome = OperationOutcome.OperationOutcome(json.loads(outcome.content))
                print(json.dumps(outcome.as_json()))

    else:
        print("ERROR with line data")

    

def file_path(string):
    if os.path.isfile(string):
        return string
    else:
        raise argparse.ArgumentError(f'File: {string}, is not found')

def dir_path(string):
    if os.path.isdir(string):
        return string
    else:
        raise argparse.ArgumentError(f'Directory: {string}, is not found')


def fhir_url(arg):
    url = urlparse(arg)
    if all((url.scheme, url.netloc)):
        #Verify there is a capability statement
        
        try:
            cs_json = get(arg + "/metadata")
        except:
            raise argparse.ArgumentTypeError(f'Unable to retrieve CapabilityStatement from {arg}')
        #    print("Oops!  That was no valid number.  Try again...")
        
        try:
            server_cs = CS.CapabilityStatement(jsondict=json.loads(cs_json.content))
        except:
            raise argparse.ArgumentTypeError(f'Unable to retrieve valid CapabilityStatement from {arg}')
        
        return arg  # return url in case you need the parsed object
    raise argparse.ArgumentTypeError(f'Invalid URL: {arg}')

main()
