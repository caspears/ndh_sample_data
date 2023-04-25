'''
Coordinate information downloaded from https://simplemaps.com/data/us-cities
Usage: python3 ndh_staging_to_json.py [output folder]
Dependecies: 
    fhirclient
    ...

TODO To install all dependencies: pip3 install -r requirements.txt
to run on windows: python -m pip ...

NOTE: this requires the r4models to be installed in the fhirclient pip site-package, to be installed in [installdir]/lib/python/site-packages/fhirclient
Email Eric Haas for these models

'''
import sys
import os
import os.path
from os import path
import argparse
import sqlite3

from nameparser import HumanName
#import random
#import hashlib

import fhirclient.r4models.organization as Organization
import fhirclient.r4models.practitioner as Practitioner
import fhirclient.r4models.practitionerrole as PractitionerRole
import fhirclient.r4models.location as Location
import fhirclient.r4models.endpoint as Endpoint
import fhirclient.r4models.meta as Meta
import fhirclient.r4models.fhirreference as ref
import fhirclient.r4models.humanname as hn
import fhirclient.r4models.address as add
import fhirclient.r4models.contactpoint as cp
import fhirclient.r4models.identifier as id
import fhirclient.r4models.extension as ext
import fhirclient.r4models.codeableconcept as CC
import fhirclient.r4models.coding as coding
import fhirclient.r4models.fhirdate as D
from json import dumps, loads
from pandas import *
from datetime import datetime, date
import re

import display_values

# CONSTANTS
URL_NDH_IDENTIFIER_STATUS = 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-identifier-status'
URL_GEOLOCATION = 'http://hl7.org/fhir/StructureDefinition/geolocation'
URL_NEWPATIENTS = 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-newpatients'

ENDPOINT_TYPE_CS_DICT = {"direct-project": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                      "hl7-fhir-rest": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                      "ihe-xds": "http://hl7.org/fhir/us/ndh/CodeSystem/EndpointConnectionTypeCS",
                      "rest-non-fhir": "http://hl7.org/fhir/us/ndh/CodeSystem/EndpointConnectionTypeCS"}

email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'

f_now = D.FHIRDate(str(date.today()))



# TODO - Support for a states filter
# Support location address type (which means the Location will need to be entity specific, 
# which means the preprocess DB schema and processing will need to change) or create new uuid 
# (which would be difficult or require restructuring of write to be done in primary resource (pract/org) order)

# Location telecom
# Organization telecom
# Do not use orphaned endpoints?
# Generate narrative (from fhirClient or other library?) 


def main():
    
    parser = argparse.ArgumentParser(description="""NPPES Preprocessed DB to FHIR NDJSON for National Directory Import""")
    parser.add_argument('dir_path', type=path_arg, help="Directory to write the NDJSON files")
    #parser.add_argument("-s", "--states", nargs='+', help="States to include (practicing address only)", required=False)
    
    
    args = parser.parse_args()

    #dir_path = sys.argv[1]
    dir_path = args.dir_path

    #conn = sqlite3.connect('nppes_AllDCArea.db')
    conn = sqlite3.connect('nppes.db')
    conn.row_factory = dict_factory
    organization_cur = conn.cursor()
    practitioner_cur = conn.cursor()
    practitioner_role_cur = conn.cursor()
    location_cur = conn.cursor()
    entity_location_cur = conn.cursor()
    endpoint_cur = conn.cursor()

    # How many resources to store in the outputBuffer before writing
    write_size = 1000
    outputBuffer = ""


    
    processLocations = True
    processEndpoints = True
    processOrganizations = True
    processPractitioners = True
    processPractitionerRoles = True
    

    currentLocItem = 0
    currentOrgItem = 0
    currentPractItem = 0
    currentPractRolItem = 0
    currentEPItem = 0
    
    

    print('....processing.....')

    now = datetime.now()
    file_name_prefix = now.strftime("%Y%m%d_%H%M%S")
    lastUpdated = now.strftime("%Y-%m-%dT%H:%M:%S.000-05:00")

    org_filename = file_name_prefix + "_Organization.ndjson"
    pract_filename = file_name_prefix + "_Practitioner.ndjson"
    practrol_filename = file_name_prefix + "_PractitionerRole.ndjson"
    loc_filename = file_name_prefix + "_Location.ndjson"
    ep_filename = file_name_prefix + "_EndPoint.ndjson"

    # Export Location Resources

    
    if(processLocations):
        f = open(os.path.join(dir_path, loc_filename), "w")
        
        location_cur.execute('SELECT * FROM Location')
        for row in location_cur:
            currentLocItem = currentLocItem + 1

            location = Location.Location()
            extensions = []
            location.meta = Meta.Meta(jsondict=loads('{"lastUpdated": "' + lastUpdated + '", "profile" : [ "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Location"]}'))
            location.id = row['id']
            location.status = "active"
            location.address = getAddress(row)

            geoLocation = getGeoLocation(row)
            if(geoLocation != None):
                extensions.append(geoLocation)

            

            
            # For new patients value, make a calculable and repeatable algorithm to identify a potential value. THis is a hash to a single digit which then can be mapped out to new patient VS values
            hash_val = test = abs(hash(row['first_line'])) % (10 ** 1)
            if(hash_val < len(display_values.accepting_patients)):
                new_patients = ext.Extension()
                new_patients.url = URL_NEWPATIENTS
                accepting_patients = ext.Extension()
                accepting_patients.url = 'acceptingPatients'
                accepting_patients.valueCodeableConcept =  CC.CodeableConcept(jsondict=loads('{"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/AcceptingPatientsCS", "code" : "' + list(display_values.accepting_patients.keys())[hash_val] + '", "display" : "' + display_values.accepting_patients[list(display_values.accepting_patients.keys())[hash_val]] + '"}]}'))
                new_patients.extension = [accepting_patients]

                extensions.append(new_patients)

            # TODO Hours of operation?

            location.extension = extensions
            
            outputBuffer = outputBuffer + dumps(location.as_json()) + "\n"
            if(currentLocItem % write_size == 0):
                f.write(outputBuffer)
                outputBuffer = ""
                # TODO Write data to file
                print("Wrote", currentLocItem, "Location records to", loc_filename)
        
        if(outputBuffer != ""):
            f.write(outputBuffer)
            outputBuffer = ""
        f.close()
        print("Wrote a total of", currentLocItem, "Location records to", loc_filename)
    
    # Export EndPoint Resources
    
    if(processEndpoints):
        f = open(os.path.join(dir_path, ep_filename), "w")
        
        endpoint_cur.execute('SELECT * FROM Endpoint')
        for row in endpoint_cur:
            currentEPItem = currentEPItem + 1
            endpoint = Endpoint.Endpoint()
            extensions = []
            endpoint.meta = Meta.Meta(jsondict=loads('{"lastUpdated": "' + lastUpdated + '", "profile" : [ "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Endpoint"]}'))
            endpoint.id = row['id']
            endpoint.status = "active"
            if(('connection_type' in row) and (row['connection_type'] != None) and (row['connection_type'] != "")):
                connectionType_code = coding.Coding()
                connectionType_code.code = row['connection_type']
                if(ENDPOINT_TYPE_CS_DICT[connectionType_code.code]):
                    connectionType_code.system = ENDPOINT_TYPE_CS_DICT[connectionType_code.code]
                if(('connection_type_description' in row) and (row['connection_type_description'] != None) and (row['connection_type_description'] != "")):
                    connectionType_code.display = row['connection_type_description']
                endpoint.connectionType = connectionType_code
            elif(re.fullmatch(email_regex, row['url'])):
                endpoint.connectionType = coding.Coding(jsondict=loads('{"system" : "http://terminology.hl7.org/CodeSystem/endpoint-connection-type", "code": "secure-email", "display": "Secure email"}'))
            elif(('/fhir/' in str(row['url']).lower()) or ('/r4/' in str(row['url']).lower()) or ('/stu3/' in str(row['url']).lower()) or ('/dstu2/' in str(row['url']).lower())):
                endpoint.connectionType = coding.Coding(jsondict=loads('{"system" : "http://terminology.hl7.org/CodeSystem/endpoint-connection-type", "code": "hl7-fhir-rest", "display": "HL7 FHIR"}'))
            else:
                # Connection type required
                # TODO Log issue
                continue
            if(('name' in row) and (row['name'] != None) and (row['name'] != "")):
                endpoint.name = row['name']
            
            endpoint.payloadType = [CC.CodeableConcept(jsondict=loads('{"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/EndpointPayloadTypeCS", "code" : "NA", "display" : "Not Applicable"}]}'))]
                    
            endpoint.address = row['url']

            endpoint.extension = extensions

            outputBuffer = outputBuffer + dumps(endpoint.as_json()) + "\n"
            if(currentEPItem % write_size == 0):
                f.write(outputBuffer)
                outputBuffer = ""
                # TODO Write data to file
                print("Wrote", currentEPItem, "Endpoint records to", ep_filename)
        if(outputBuffer != ""):
            f.write(outputBuffer)
            outputBuffer = ""

        f.close()
        print("Wrote a total of", currentEPItem, "Endpoint records to", ep_filename)

    # Export Organization Resources
    
    if(processOrganizations):
        f = open(os.path.join(dir_path, org_filename), "w")
        
        organization_cur.execute('SELECT * FROM Organization')
        for row in organization_cur:
            currentOrgItem = currentOrgItem + 1

            organization = Organization.Organization()
            extensions = []
            organization.meta = Meta.Meta(jsondict=loads('{"lastUpdated": "' + lastUpdated + '", "profile" : [ "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Organization"]}'))
            organization.id = row['id']
            organization.identifier = [id.Identifier(jsondict=loads('{"use": "official", "system": "http://hl7.org/fhir/sid/us-npi", "value": "' + row['npi'] + '"}'))]
            organization.active = True
            organization.type = [CC.CodeableConcept(jsondict=loads('{"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/OrgTypeCS", "code" : "prvgrp", "display": "Provider Group"}]}'))]

            organization.name = row['name']
            # Load address and phone number from Location using Entity_Location
            
            
            location_cur.execute('SELECT * from Location as l INNER JOIN Entity_Location as el ON l.id = el.location_id WHERE el.entity_id=? AND el.entity_type = 2;', (organization.id,))
            locations = location_cur.fetchall()
            #Get organization Locations
            addresses = []
            for location in locations:
                address = getAddress(location)
                if(address != None):
                    geoLocation = getGeoLocation(location)
                    if(geoLocation != None):
                        address.extension = [geoLocation]
                    addresses.append(address)
                
            if(len(addresses) > 0):
                organization.address = addresses

            telecoms = getTelecoms(row)
            if((telecoms != None) and (len(telecoms) > 0)):
                organization.telecom = telecoms

            organization.extension = extensions

            #Organization endpoints
            endpoint_cur.execute('SELECT * from Endpoint WHERE npi=?;', (row['npi'],))
            endpoints = endpoint_cur.fetchall()
            #Get organization Locations
            endpoint_list = []
            for endpoint in endpoints:
                if(('id' in endpoint) and (endpoint['id'] != None) and (endpoint['id'] != "")):
                    endpoint_ref = ref.FHIRReference(jsondict=loads('{"reference": "Endpoint/' + endpoint['id'] + '", "type" : "Endpoint"}'))
                    endpoint_list.append(endpoint_ref)
                
            if(len(endpoint_list) > 0):
                organization.endpoint = endpoint_list



            outputBuffer = outputBuffer + dumps(organization.as_json()) + "\n"
            if(currentOrgItem % write_size == 0):
                f.write(outputBuffer)
                outputBuffer = ""
                # TODO Write data to file
                print("Wrote", currentOrgItem, "Organization records to", org_filename)
        
        if(outputBuffer != ""):
            f.write(outputBuffer)
            outputBuffer = ""
        f.close()
        print("Wrote a total of", currentOrgItem, "Organization records to", org_filename)


    # Export Practitioner Resources
    
    if(processPractitioners):
        f = open(os.path.join(dir_path, pract_filename), "w")
        
        practitioner_cur.execute('SELECT * FROM Practitioner')
        for row in practitioner_cur:
            currentPractItem = currentPractItem + 1

            practitioner = Practitioner.Practitioner()
            extensions = []
            practitioner.meta = Meta.Meta(jsondict=loads('{"lastUpdated": "' + lastUpdated + '", "profile" : [ "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Practitioner"]}'))
            practitioner.id = row['id']
            practitioner.identifier = [id.Identifier(jsondict=loads('{"use": "official", "system": "http://hl7.org/fhir/sid/us-npi", "value": "' + row['npi'] + '"}'))]
            practitioner.active = True
            #practitioner.type = [CC.CodeableConcept(jsondict=loads('{"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/OrgTypeCS", "code" : "prvgrp", "display": "Provider Group"}]}'))]

            # Load Practitioner Endpoint extensions
            practitioner.name = getHumanNames(row)
            # Load address and phone number from Location using Entity_Location
            
            location_cur.execute('SELECT * from Location as l INNER JOIN Entity_Location as el ON l.id = el.location_id WHERE el.entity_id=? AND el.entity_type = 1;', (practitioner.id,))
            locations = location_cur.fetchall()
            #Get organization Locations
            addresses = []
            for location in locations:
                address = getAddress(location)
                if(address != None):
                    geoLocation = getGeoLocation(location)
                    if(geoLocation != None):
                        address.extension = [geoLocation]
                    addresses.append(address)
                
            if(len(addresses) > 0):
                practitioner.address = addresses

            telecoms = getTelecoms(row)
            if((telecoms != None) and (len(telecoms) > 0)):
                practitioner.telecom = telecoms

            practitioner.extension = extensions



            outputBuffer = outputBuffer + dumps(practitioner.as_json()) + "\n"
            if(currentPractItem % write_size == 0):
                f.write(outputBuffer)
                outputBuffer = ""
                # TODO Write data to file
                print("Wrote ", currentPractItem, "Practitioner records to", pract_filename)
        
        if(outputBuffer != ""):
            f.write(outputBuffer)
            outputBuffer = ""
        f.close()
        print("Wrote a total of", currentPractItem, "Practitioner records to", pract_filename)


    if(processPractitionerRoles):
        f = open(os.path.join(dir_path, practrol_filename), "w")
        
        practitioner_cur.execute('SELECT * FROM PractitionerRole')
        for row in practitioner_cur:
            currentPractRolItem = currentPractRolItem + 1

            practitionerrole = PractitionerRole.PractitionerRole()
            extensions = []
            practitionerrole.meta = Meta.Meta(jsondict=loads('{"lastUpdated": "' + lastUpdated + '", "profile" : [ "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-PractitionerRole"]}'))
            practitionerrole.id = row['id']
            
            practitionerrole.active = True
            # TODO Get practitioner and organization name for display
            if(('practitioner_id' in row) and (row['practitioner_id'] != None) and (row['practitioner_id'] != "")):
                practitionerrole.practitioner = ref.FHIRReference(jsondict=loads('{"reference": "Practitioner/' + row['practitioner_id'] + '", "type" : "Practitioner"}'))

            
            if(('organization_id' in row) and (row['organization_id'] != None) and (row['organization_id'] != "")):
                practitionerrole.organization = ref.FHIRReference(jsondict=loads('{"reference": "Organization/' + row['organization_id'] + '", "type" : "Organization"}'))
            #practitioner.type = [CC.CodeableConcept(jsondict=loads('{"coding" : [{"system" : "http://hl7.org/fhir/us/ndh/CodeSystem/OrgTypeCS", "code" : "prvgrp", "display": "Provider Group"}]}'))]

            specialties = []
            for key, val in row.items():
                if 'specialty_' in key:
                    if((val != None) and (val != "")):
                        if(val in display_values.specialty_display):
                            specialty = CC.CodeableConcept(jsondict=loads('{"coding" : [{"system" : "http://nucc.org/provider-taxonomy", "code" : "' + val + '", "display" : "' + display_values.specialty_display[val] + '"}]}'))
                            specialties.append(specialty)
            
            if(len(specialties) > 0):
                practitionerrole.specialty = specialties

            if(('location_id' in row) and (row['location_id'] != None) and (row['location_id'] != "")):
                practitionerrole.location = [ref.FHIRReference(jsondict=loads('{"reference": "Location/' + row['location_id'] + '", "type" : "Location"}'))]

            telecoms = getTelecoms(row)
            if((telecoms != None) and (len(telecoms) > 0)):
                practitionerrole.telecom = telecoms

            practitionerrole.extension = extensions


            #PractitionerRole endpoints
            endpoints = []
            # If the location is associated with a location (an endpoint that is for this practitionerRole with a matching NPI), use that one, 
            #   otherwise, if this is an organization associated PractitonerRole, use the organization endpoints, 
            #       otherwise use the Practitioner endpoints
            if(('location_id' in row) and (row['location_id'] != None) and (row['location_id'] != "")):
                """SELECT id from Entity_Location WHERE entity_type = '4' AND location_id = ?"""
                entity_location_cur.execute("SELECT id from Entity_Location WHERE entity_type = '4' AND location_id = ?", (row['location_id'],))
                endpoints = entity_location_cur.fetchall()

            if(len(endpoints) == 0):
                if(('organization_id' in row) and (row['organization_id'] != None) and (row['organization_id'] != "")):
                    #TODO CONNECT ENDPOINT WITH ORGANIZATION THROUGH ENTITY LOCATION
                    endpoint_cur.execute('SELECT ep.id from Endpoint as ep INNER JOIN Organization as org ON org.npi = ep.npi WHERE org.id = ?', (row['organization_id'],))
                    endpoints = endpoint_cur.fetchall()
                elif(('practitioner_id' in row) and (row['practitioner_id'] != None) and (row['practitioner_id'] != "")):
                    endpoint_cur.execute('SELECT ep.id from Endpoint as ep INNER JOIN Practitioner as pract ON pract.npi = ep.npi WHERE pract.id = ?', (row['practitioner_id'],))
                    endpoints = endpoint_cur.fetchall()

            endpoint_list = []
            for endpoint in endpoints:
                if(('id' in endpoint) and (endpoint['id'] != None) and (endpoint['id'] != "")):
                    endpoint_ref = ref.FHIRReference(jsondict=loads('{"reference": "Endpoint/' + endpoint['id'] + '", "type" : "Endpoint"}'))
                    endpoint_list.append(endpoint_ref)
                
            if(len(endpoint_list) > 0):
                practitionerrole.endpoint = endpoint_list


            outputBuffer = outputBuffer + dumps(practitionerrole.as_json()) + "\n"
            if(currentPractRolItem % write_size == 0):
                f.write(outputBuffer)
                outputBuffer = ""
                # TODO Write data to file
                print("Wrote", currentPractRolItem, "PractitionerRole records to", practrol_filename)
        
        if(outputBuffer != ""):
            f.write(outputBuffer)
            outputBuffer = ""
        f.close()
        print("Wrote a total of", currentPractRolItem, "PractitionerRole records to", practrol_filename)
    
def getHumanNames(data):
    names = []
    if((('last_name' in data) and (data['last_name'] != None) and (data['last_name'] != "")) or (('alias_last_name' in data) and (data['alias_last_name'] != None) and (data['alias_last_name'] != ""))):
        if(('last_name' in data) and (data['last_name'] != None) and (data['last_name'] != "")):
            name = hn.HumanName()

            name.family = data['last_name']
            
            if(('middle_name' in data) and (data['middle_name'] != None) and (data['middle_name'] != "")):
                name.given = [data['first_name'], data['middle_name']]
            else:
                name.given = [data['first_name']]

            
            if(('prefix_name' in data) and (data['prefix_name'] != None) and (data['prefix_name'] != "")):
                name.prefix = [data['prefix_name']]

            suffixList = []
            if(('suffix_name' in data) and (data['suffix_name'] != None) and (data['suffix_name'] != "")):
                name.suffix =[data['suffix_name']]
            #if(credential != ''):
            #    suffixList.append(credential)
            #    displayName.suffix = displayName.suffix + " " + credential

                
            if(('name_use' in data) and (data['name_use'] != None) and (data['name_use'] != "")):
                #if(use in ['usual', 'official', 'temp', 'nickname', 'anonymous', 'old', 'maiden']):
                name.use = data['name_use']

            if(('name_display' in data) and (data['name_display'] != None) and (data['name_display'] != "")):
                name.text = data['name_display']

            names.append(name)

   
        if(('alias_last_name' in data) and (data['alias_last_name'] != None) and (data['alias_last_name'] != "")):
            name = hn.HumanName()

            name.family = data['alias_last_name']
            
            if(('alias_middle_name' in data) and (data['alias_middle_name'] != None) and (data['alias_middle_name'] != "")):
                name.given = [data['alias_first_name'], data['alias_middle_name']]
            else:
                name.given = [data['alias_first_name']]

            
            if(('alias_prefix_name' in data) and (data['alias_prefix_name'] != None) and (data['alias_prefix_name'] != "")):
                name.prefix = [data['alias_prefix_name']]

            suffixList = []
            if(('alias_suffix_name' in data) and (data['alias_suffix_name'] != None) and (data['alias_suffix_name'] != "")):
                name.suffix =[data['alias_suffix_name']]
            #if(credential != ''):
            #    suffixList.append(credential)
            #    displayName.suffix = displayName.suffix + " " + credential

                
            if(('alias_name_use' in data) and (data['alias_name_use'] != None) and (data['alias_name_use'] != "")):
                #if(use in ['usual', 'official', 'temp', 'nickname', 'anonymous', 'old', 'maiden']):
                name.use = data['name_use']

            if(('alias_name_display' in data) and (data['alias_name_display'] != None) and (data['alias_name_display'] != "")):
                name.text = data['alias_name_display']

            names.append(name)

    return names
    
def getAddress(data):
    if('first_line' in data):
        address = add.Address()
        addressLines = []
        if('first_line' in data):
            addressLines.append(data['first_line'])
        if(('second_line' in data) and (data['second_line'] != None) and (data['second_line'] != "")):
            addressLines.append(data['second_line'])
        if(len(addressLines) > 0):
            address.line = addressLines
        if('city' in data):
            text = HumanName(data['city'])
            text.capitalize()
            address.city = str(text)
        if('state' in data):
            address.state = data['state']
        if('postal' in data):
            address.postalCode = data['postal'].replace(".0", "")
            if(len(address.postalCode) == 9):
                address.postalCode = address.postalCode[:5] + '-' + address.postalCode[5:]
        if('country' in data):
            address.country = data['country']
        
        if('use' in data):
            address.use = data['use']
        if('type' in data):
            address.use = data['type']

        if('location_type' in data):
            if(data['location_type'] == 1):
                address.use = 'billing'
            elif(data['location_type'] == 2):
                address.use = 'work'
            elif(data['location_type'] == 3):
                address.use = 'work'

        return address

def getGeoLocation(data):

    if(('latitude' in data) and (data['latitude'] != None) and ('longitude' in data) and (data['longitude'] != None) ):
        geoLocation = ext.Extension()
        geoLocation.url = URL_GEOLOCATION
        latitude = ext.Extension()
        latitude.url = 'latitude'
        latitude.valueDecimal = float(data['latitude'])
        longitude = ext.Extension()
        longitude.url = 'longitude'
        longitude.valueDecimal = float(data['longitude'])
        geoLocation.extension = [latitude, longitude]
        
        return geoLocation

def getTelecoms(data):
    
    if(('phone' in data) or ('fax' in data) or ('mail_phone' in data) or ('mail_fax' in data) or ('office_phone' in data) or ('office_fax' in data)):
        telecoms = []
        if(('phone' in data) and (len(data['phone']) > 0)):
            telecom = cp.ContactPoint()
            telecom.value = data['phone'].replace(".0", "")
            telecom.system = 'phone'
            telecoms.append(telecom)

        if(('fax' in data) and (len(data['fax']) > 0)):
            telecom = cp.ContactPoint()
            telecom.value = data['fax'].replace(".0", "")
            telecom.system = 'fax'
            telecoms.append(telecom)
        
        if(('office_phone' in data) and (len(data['office_phone']) > 0)):
            telecom = cp.ContactPoint()
            telecom.value = data['office_phone'].replace(".0", "")
            telecom.system = 'phone'
            telecoms.append(telecom)

        if(('office_fax' in data) and (len(data['office_fax']) > 0)):
            telecom = cp.ContactPoint()
            telecom.value = data['office_fax'].replace(".0", "")
            telecom.system = 'fax'
            telecoms.append(telecom)

        if(('mail_phone' in data) and (len(data['mail_phone']) > 0)):
            telecom = cp.ContactPoint()
            telecom.value = data['mail_phone'].replace(".0", "")
            telecom.system = 'phone'
            telecoms.append(telecom)

        if(('phmail_faxone' in data) and (len(data['mail_fax']) > 0)):
            telecom = cp.ContactPoint()
            telecom.value = data['mail_fax'].replace(".0", "")
            telecom.system = 'fax'
            telecoms.append(telecom)

        return telecoms

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def path_arg(string):
    if os.path.isdir(string):
        return string
    else:
        raise argparse.ArgumentError(f'D{string}, is not a found directory')


main()

    
    