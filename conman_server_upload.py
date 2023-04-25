
import sys
import os.path
import glob
import time
import errno
import argparse

from urllib.parse import urlparse
import json
from json import dumps, loads
from requests import post, get, delete
import sqlite3
from pandas import *
import uuid
from nameparser import HumanName

from fhirclient import client
import fhirclient.r4models.bundle as bundle
import fhirclient.r4models.capabilitystatement as CS
import fhirclient.r4models.organization as Organization
import fhirclient.r4models.endpoint as Endpoint
import fhirclient.r4models.consent as consent
import fhirclient.r4models.meta as meta
import fhirclient.r4models.identifier as id
import fhirclient.r4models.extension as ext
import fhirclient.r4models.codeableconcept as CC
import fhirclient.r4models.contactpoint as ContactPoint
import fhirclient.r4models.coding as coding
import fhirclient.r4models.fhirreference as ref

# TODO meta.profile
# Finish Endpoint extensions like use case and proxy
# LastUpdated for all resources

### Globals
CONNECTATHON_RESOURCE_ID = "Connectathon"
ENDPOINT_TYPE_DICT = {"DIRECT": "direct-project",
                      "FHIR": "hl7-fhir-rest",
                      "CONNECT": "ihe-xds",
                      "REST": "rest-non-fhir",
                      "SOAP": "",
                      "WEB": "",
                      "OTHERS": ""}

ENDPOINT_TYPE_DISPLAY_DICT = {"DIRECT": "Direct Project",
                      "FHIR": "HL7 FHIR",
                      "CONNECT": "CONNECT - IHE XDS",
                      "REST": "REST (not FHIR)",
                      "SOAP": "SOAP",
                      "WEB": "Web",
                      "OTHERS": "Other"}

FHIR_VERSION = ["0.0.80", "0.0.81", "0.0.82", "1.0.1", "1.0.2", "3.0.0", "3.0.1", "4.0.0", "4.0.1"]
CONTACT_SYSTEMS = ["phone", "fax", "email", "pager", "url", "sms", "other"]

EXT_USAGE_RESTRICTION = 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-usage-restriction'

def main():

    print('....processing.....')


    currentOrgItem = 0
    currentPractItem = 0
    currentEPItem = 0
    write_to_server = False
    fhirSettings = {}
    fhirClient = None


    parser = argparse.ArgumentParser(description="""Update National Directory FHIR Server with Connectathon Endpoints""")
    parser.add_argument("-s", "--server", type=fhir_url, help="Server URL to make changes to")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-f", "--file", type=file_path, help="ConMan Servers Json File")
    group.add_argument("-d", "--delete", help="Delete Connectathon Resources from the National Directory FHIR Server", action='store_true')
    
    args = parser.parse_args()

    fhirClient = None

    if(args.server != None):
        fhirSettings = {}
        fhirSettings['app_id'] = "ConMan Server Import App"
        fhirSettings['api_base'] = args.server
        fhirClient = client.FHIRClient(settings = fhirSettings)
    

    #conman_file = sys.argv[1]
    if (args.delete):
        
        deleteConnectathonRecordsOnServer(fhirClient)
    else:
        
        #client.FHIRClient(settings=settings)
        ### Load NPI
        start = time.time()

        isExist = os.path.exists("output")
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs("output")

        
        with open(f'{args.file}', 'r', encoding='utf-8-sig') as f:
            r = f.read()
            # encoding for loads was deprecated in 3.9  https://bugs.python.org/issue39377
            if sys.version_info >= (3, 9):
                servers = loads(r)
            else:
                loads(r, encoding = 'utf-8')
        
        endpoints = []
        
        #print(servers)
        for server in servers:
            #print(server)
            endpoint = getFHIREndpoint(server)
            if(endpoint is not None):
                # write to server?
                endpoints.append(endpoint)
                f = open("output/Endpoint" + endpoint.id + ".json", "w")
                f.write(json.dumps(endpoint.as_json()))
                f.close()
                if(fhirClient):
                    try:
                        endpoint.update(fhirClient.server)
                        print(f"Created EndPoint, id: {endpoint.id}; name: {endpoint.name}")
                    except Exception as exp:
                        test = exp.response.content
                        print(f"Unable to create/update endpoint, id: {endpoint.id}; name: {endpoint.name}")
                
                
                
                #server['endpoint_id'] = endpoint.id
            #print(json.dumps(endpoint.as_json()))
                #organization = getFHIROrganization(server)
                # TODO, perhaps list the endpoints under a Connectathon organization? - https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-ndhEx-Organization.html
                #if(organization is not None):


        #Create aConnectathon Org Consent
        connectationConsent = consent.Consent()
        connectationConsent.meta = meta.Meta(jsondict=json.loads('{"profile" : [ "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Restriction"]}'))
        #consentMeta = meta.Meta()
        #consentMeta.profile = ['http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Restriction']
        #connectationConsent.meta = consentMeta
        connectationConsent.id = CONNECTATHON_RESOURCE_ID
        connectationConsent.status = 'active'
        #directory-privacy	http://hl7.org/fhir/us/ndh/CodeSystem/ConsentScopeNdhCS
        connectationConsent.scope = CC.CodeableConcept(jsondict=json.loads('{"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/ConsentScopeNdhCS", "code" : "directory-privacy", "display" : "Directory Privacy Consent"}]}'))
        connectationConsent.category = [CC.CodeableConcept(jsondict=json.loads('{"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/ConsentCategoryNdhCS", "code" : "DRC", "display" : "Directory Restriction"}]}'))]
        pol = consent.ConsentPolicy()
        pol.uri = "connectathon.html"
        connectationConsent.policy = [pol]
        #connectationConsent.policy = [consent.ConsentPolicy(jsondict=json.loads('"uri" : "connectathon.html"'))]
        connectationConsent.provision = consent.ConsentProvision()
        connectationConsent.provision.type = 'permit'
        
        connectationConsent.provision.actor = [consent.ConsentProvisionActor()]
        connectationConsent.provision.actor[0].role = CC.CodeableConcept(jsondict=json.loads('{"coding" : [{"system" : "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code" : "IRCP", "display" : "information recipient"}]}'))
        connectationConsent.provision.actor[0].reference = ref.FHIRReference(jsondict=json.loads('{"display" : "All"}'))
        connectationConsent.provision.purpose = [coding.Coding(jsondict=json.loads('{"system" : "http://terminology.hl7.org/CodeSystem/v3-ActReason", "code" : "SYSDEV", "display" : "system development"}')), coding.Coding(jsondict=json.loads('{"system" : "http://terminology.hl7.org/CodeSystem/v3-ActReason", "code" : "HTEST", "display" : "test health data"}'))]
        
        #consent.Consent.update(connectationConsent)
        if(fhirClient):
            try:
                connectationConsent.update(fhirClient.server)
                print(f"Created Connectathon Organization Usage Restriction Consent, id: {connectationConsent.id}")

            except:
                print(f"Unable to create/update Connectathon Organization Usage Restriction Consent, id: {connectationConsent.id}")
        
        #Endpoint.Endpoint.update
        f = open("output/Consent.json", "w")
        f.write(json.dumps(connectationConsent.as_json()))
        f.close()
        #connectationConsent.update(fhirClient.server)
        
        # TODO Write Conesnt
        #print(json.dumps(connectationConsent.as_json()))
        

        # Create a Connectathon Org with all of the endpoints listed. https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-ndhEx-Organization.html
        connectathonOrg = Organization.Organization() 
        #orgMeta = meta.Meta()
        #orgMeta.profile = ['http://hl7.org/fhir/us/ndh/StructureDefinition/ndhEx-Organization']
        #connectathonOrg.meta = orgMeta
        #connectathonOrg.meta = meta.Meta(jsondict=json.loads('{"profile" : [ "http://hl7.org/fhir/us/ndh/StructureDefinition/ndhEx-Organization"]}'))
        connectathonOrg.meta = meta.Meta(jsondict=json.loads('{"lastUpdated": "2023-04-08T00:39:04.111-07:00", "profile" : [ "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Organization"]}'))
        connectathonOrg.id = CONNECTATHON_RESOURCE_ID
        connectathonOrg.name = CONNECTATHON_RESOURCE_ID
        connectathonOrg.active = True
        
        connectathonOrg.extension = [ext.Extension(jsondict=json.loads('{"url" : "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-usage-restriction", "valueReference" : {"reference" : "Consent/'+CONNECTATHON_RESOURCE_ID+'", "display" : "Connectathon Organization Use Restriction"}}'))]

        org_type = CC.CodeableConcept(jsondict=json.loads('{"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/OrgTypeCS", "code" : "bus", "display" : "Non-Healthcare Business"}]}'))
        #org_type_coding = coding.Coding()
        #org_type_coding.system = 'http://hl7.org/fhir/us/ndh/CodeSystem/OrgTypeCS'
        #org_type_coding.code = 'bus'
        #org_type.coding = [org_type_coding]
        #org_type.coding = 
        #connectathonOrg.type = [CC.CodeableConcept(jsondict=json.loads('{"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/OrgTypeCS", "code" : "bus", "display" : "Non-Healthcare Business"}]}'))]

        org_description_ext = ext.Extension(jsondict=json.loads('{"url" : "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-org-description", "valueString" : "The purpose of this organization is to help support Connectathon attendees by making it easier to locate Connectathon server endpoints."}'))
        #org_usagerestriction_ext = ext.Extension(jsondict=json.loads('{"url" : "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-org-description", "valueReference" : {"reference" : "Consent/'+CONNECTATHON_RESOURCE_ID+'", "display" : "Connectathon Organization Use Restriction"}}'))
        #org_description_ext.url = 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-org-description'
        #org_description_ext.valueString = 'The purpose of this organization is to help support Connectathon attendees by making it easier to locate Connectathon server endpoints.'

        org_type.extension = [org_description_ext]
        
        connectathonOrg.type = [org_type]
        # http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-usage-restriction

        connectathonOrg.endpoint = []

        for endpoint in endpoints:
            endpoint_ref = ref.FHIRReference()
            endpoint_ref.reference = 'Endpoint/' + endpoint.id
            
            endpoint_ref.display = endpoint.identifier[0].value
            #print(endpoint_ref.display)
            connectathonOrg.endpoint.append(endpoint_ref)
        
        f = open("output/Org.json", "w")
        f.write(json.dumps(connectathonOrg.as_json()))
        f.close()

        if(fhirClient):
            try:
                connectathonOrg.update(fhirClient.server)
                print(f"Created Connectathon Organization, id: {connectathonOrg.id}, name: {connectathonOrg.name}")

            except:
                print(f"Unable to create/update Connectathon Organization, id: {connectathonOrg.id}, name: {connectathonOrg.name}")
        
        # TODO Write Organization
        #print(json.dumps(connectathonOrg.as_json()))



def deleteConnectathonRecordsOnServer(fhirClient):
    #connectathon_org = Organization.Organization()
    #import fhirclient.models.organization as org
    #search = p.Procedure.where(struct={'subject': 'hca-pat-1', 'status': 'completed'})
    #search = p.Procedure.where(struct={'subject': 'hca-pat-1', 'status': 'completed'})
    #search = org.Organization.where(struct={'name': CONNECTATHON_RESOURCE_ID})
    #connectathon_orgs = search.perform_resources(fhirClient.server)
    #for org in connectathon_orgs:
    #    print(json.dumps(org.as_json()))
    if(fhirClient):
        try:
            org = Organization.Organization.read(CONNECTATHON_RESOURCE_ID, fhirClient.server)
        except client.FHIRNotFoundException:
            print("Unable to find Connectathon Organization, id: " + CONNECTATHON_RESOURCE_ID)
            return
        
        #print(json.dumps(org.as_json()))
        
        # Get endpoints to delete
        endpoints = []
        if(org.endpoint):
            for endpoint in org.endpoint:
                endpoints.append(endpoint)
                #print(json.dumps(endpoint.as_json()))

        try:
            org.delete()
            print("Deleted Connectathon Organization, id: " + CONNECTATHON_RESOURCE_ID)

        except client.FHIRNotFoundException:
            print("Unable to delete Connectathon Organization and related resources, id: " + CONNECTATHON_RESOURCE_ID)
            return

        print("***Deleting Connectathon Organization Endpoints***")
        deleteEndpointsOnServer(fhirClient, endpoints)

        print("***Deleting Connectathon Organization Consent***")
        deleteConsentOnServer(fhirClient, org)
    
                    
    
    
def deleteConsentOnServer(fhirClient, org): 
    # Delete usage restriction Consent, if exists
    if(org.extension):
        for extension in org.extension:
            if(extension.url == EXT_USAGE_RESTRICTION):
                consent_ref = extension.valueReference.reference
                # Assuming Consent is on the same server
                consent_ref = consent_ref.replace('Consent/', '')
                if(len(consent_ref) > 0):
                    print(consent_ref)

                    try:
                        con = consent.Consent.read(consent_ref, fhirClient.server)
                        con.delete()
                        print("Deleted Connectathon Organization Consent, id: " + consent_ref)

                    except client.FHIRNotFoundException:
                        print("Unable to find Connectathon Organization Consent to delete, id: " + consent_ref)

def deleteEndpointsOnServer(fhirClient, endpoints):
    # Delete Endpoints
    if(len(endpoints) > 0):
        for endpoint in endpoints:
            if(endpoint.reference):
                endpoint_ref = endpoint.reference.replace('Endpoint/', '')
                # Assuming Endpoint is on the same server

                if(len(endpoint_ref) > 0):
                    #print(endpoint_ref)

                    try:
                        ep = Endpoint.Endpoint.read(endpoint_ref, fhirClient.server)
                        ep.delete()
                        print("Deleted Endpoint, id: " + endpoint_ref)

                    except client.FHIRNotFoundException:
                        print("Unable to find Connectathon Organization Consent to delete, id: " + endpoint_ref)
    
def getFHIREndpoint(server):
    endpoint = Endpoint.Endpoint()
    endpoint.id = str(uuid.uuid4())

    endpoint.meta = meta.Meta(jsondict=json.loads('{"lastUpdated": "2023-04-08T00:39:04.111-07:00", "profile" : [ "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Endpoint"]}'))

     
    conman_id = id.Identifier()
    conman_id.value = server['name']
    conman_id.system = "http://conman.clinfhir.com/connectathon.html"
    conman_id_status_ext = ext.Extension()
    conman_id_status_ext.url = 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-identifier-status'
    conman_id_status_ext.valueCode = 'active'
    conman_id.extension = [conman_id_status_ext]
    endpoint.identifier = [conman_id]
    endpoint.name = server['name']
    endpoint.status = 'active'

    endpoint_extensions = []

    '''
    Can't use use-case extension for Connectathon tracks  
    #'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-endpoint-usecase' Special Connectathon or testing use case [tracks]
    

    
    if(('tracks' in server) and (len(server['tracks']) > 0)):

        for track in server['tracks']:
            if(('name' in track) and (len(track['name']) > 0)):
                use_case_ext = ext.Extension()
                use_case_ext.url = 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-endpoint-usecase'

                use_case_type_ext = ext.Extension()
                use_case_type_ext.url = 'endpointUsecasetype'
                # Using direct Trust as CS to extend the VS that does not provide sufficient codes.
                use_case_coding = '{"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/NdhDirectTrustEndpointUsecaseCS", "code" : "support"}]}'
                use_case_type_ext.valueCodeableConcept = CC.CodeableConcept()
                use_case_coding = coding.Coding()
                #use_case_coding.system = "http://conman.clinfhir.com/connectathon.html"
                use_case_coding.display = track['name']
                use_case_type_ext.valueCodeableConcept.coding = [use_case_coding]
                 #jsondict=json.loads('{"url" : "endpointUsecasetype", "valueCodeableConcept" : {"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/NdhDirectTrustEndpointUsecaseCS", "code" : "support"}]}'))

                use_case_ext.extension = [use_case_type_ext]
                endpoint_extensions.append(use_case_ext)
                
    '''
    #'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-igsSupported' [igs]
    if(('igs' in server) and (len(server['igs']) > 0)):

        for ig in server['igs']:
            
            if(('name' in ig) and (len(ig['name']) > 0)):
                #print("IG Found - ", ig['name'])
                ig_supported_ext = ext.Extension()
                ig_supported_ext.url = 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-igsSupported'
                ig_supported_ext.extension = []

                ig_supported_type_ext = ext.Extension(jsondict=json.loads('{"url" : "igsSupportedType", "valueCodeableConcept" : {"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/IgTypeCS", "code" : "FHIR"}]}}'))
                
                
                ig_supported_ext.extension.append(ig_supported_type_ext)

                # cant support formatCode as that is IHE specific
                # TODO How to specify the actual supported IG? There seems to be a supported ig extension (https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-base-ext-supported-ig.html), but that only comes in via the non fhir use cases
                
                if(('version' in ig) and (len(ig['version']) > 0)):
                    ig_supported_version_ext = ext.Extension()
                    ig_supported_version_ext.url = 'versionCode'
                    ig_supported_version_ext.valueString = ig['version']
                    ig_supported_ext.extension.append(ig_supported_version_ext)

                endpoint_extensions.append(ig_supported_ext)

    
    #'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-associatedServers' proxy type when proxy is configured [proxy]
    
    if(('proxy' in server) and (len(server['proxy']) > 0)):
        associated_server_ext = ext.Extension()
        associated_server_ext.url = 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-associatedServers'
        associated_server_type_ext = ext.Extension(jsondict=json.loads('{"url" : "associatedServersType", "valueCodeableConcept" : {"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/NdhAssociatedServersTypeCS", "code" : "proxy-server"}]}}'))
        associated_server_url_ext = ext.Extension()
        associated_server_url_ext.url = 'serverURL'
        associated_server_url_ext.valueString = server['proxy']
        associated_server_ext.extension = [associated_server_type_ext, associated_server_url_ext]

        endpoint_extensions.append(associated_server_ext)
    


    # 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-endpoint-connection-type-version' (FHIR Version)
    if(('fhirVersion' in server) and (len(server['fhirVersion']) > 0)): 
        fhir_version_ext = ext.Extension()
        fhir_version_ext.url = 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-endpoint-connection-type-version'

        fhir_version_ext.valueCodeableConcept = CC.CodeableConcept()
        fhir_version_coding = coding.Coding()
        if(server['fhirVersion'] == "4.3.0"):
            fhir_version_coding.system = "http://hl7.org/fhir/us/ndh/CodeSystem/EndpointConnectionTypeVersionCS"
        elif(server['fhirVersion'] in FHIR_VERSION):
            fhir_version_coding.system = "http://hl7.org/fhir/FHIR-version"
        
        fhir_version_coding.code = server['fhirVersion']
        fhir_version_ext.valueCodeableConcept.coding = [fhir_version_coding]
            #jsondict=json.loads('{"url" : "endpointUsecasetype", "valueCodeableConcept" : {"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/NdhDirectTrustEndpointUsecaseCS", "code" : "support"}]}'))

        endpoint_extensions.append(fhir_version_ext)

        


    endpoint.extension = endpoint_extensions
    

    # Contacts
    #'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-igsSupported' [igs]
    if(('contacts' in server) and (len(server['contacts']) > 0)):

        for contact in server['contacts']:
            
            if(('details' in contact) and ('contact' in contact['details'])):
                endpoint.contact = []
                for detail in contact['details']['contact']:
                    current_contact = ContactPoint.ContactPoint()
                    if('value' in detail):
                        current_contact.value = detail['value']
                    if(('type' in detail) and (detail['type'] in CONTACT_SYSTEMS)):
                        current_contact.system = detail['type']
                    endpoint.contact.append(current_contact)
                



    
    connection_type = coding.Coding()
    connection_type.system = 'http://terminology.hl7.org/CodeSystem/endpoint-connection-type'
    if(('connectionType' in server) and (len(server['connectionType']) > 0) and('code' in server['connectionType'])):
        connection_type.system = server['connectionType'][0]['system']
        connection_type.code = server['connectionType'][0]['code']
        connection_type.version = server['connectionType'][0]['version']
        connection_type.display = server['connectionType'][0]['display']
    else:
        # Default connectionType
        connection_type.code = 'hl7-fhir-rest'
        connection_type.display = 'HL7 FHIR'

    endpoint.connectionType = connection_type
    
    #endpoint.contact

    payload_type = CC.CodeableConcept()
    payload_type_coding = coding.Coding()
    if(('payloadType' in server) and (len(server['payloadType']) > 0) and('code' in server['payloadType'])):
        payload_type_coding.system = server['payloadType'][0]['system']
        payload_type_coding.code = server['payloadType'][0]['code']
        payload_type_coding.version = server['payloadType'][0]['version']
        payload_type_coding.display = server['payloadType'][0]['display']
    else:
        payload_type_coding.system = 'http://hl7.org/fhir/us/ndh/CodeSystem/EndpointPayloadTypeCS'
        payload_type_coding.code = 'NA'
        payload_type_coding.display = 'Not Applicable'
    
    payload_type.coding = [payload_type_coding]
    endpoint.payloadType = [payload_type]
    
    #endpoint.payloadMimeType
    if('address' in server):
        endpoint.address = server['address']
    else:
        endpoint.address = 'http://100::/64'

    # Consent restriction from ext Endpoint.usage-restriction - https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-ndh-Restriction.html where provision.purpose of use is https://terminology.hl7.org/4.0.0/ValueSet-v3-PurposeOfUse.html SYSDEV
    # state - active
    # scope directory-privacy http://hl7.org/fhir/us/ndh/CodeSystem/ConsentScopeNdhCS
    # category - DRC	http://hl7.org/fhir/us/ndh/CodeSystem/ConsentCategoryNdhCS

    # TODO check min requirements and return None if not met.

    
    return endpoint


def file_path(string):
    if os.path.isfile(string):
        return string
    else:
        raise argparse.ArgumentError(f'File: {string}, is not found')


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
