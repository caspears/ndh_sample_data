'''
Coordinate information downloaded from https://simplemaps.com/data/us-cities
Usage: python3 R4CapStatement_Maker.py [xlsx file]
Dependecies: 
    fhirclient
    pandas
    xlrd
    openpyxl
    stringcase
    jinja2
    commonmark
    lxml

To install all dependencies: pip3 install -r requirements.txt
to run on windows: python -m pip ...

NOTE: this requires the r4models to be installed in the fhirclient pip site-package, to be installed in [installdir]/lib/python/site-packages/fhirclient
Email Eric Haas for these models

Modified from: https://github.com/Healthedata1/MyNotebooks/blob/master/CapStatement/R4CapStatement_Maker.ipynb
'''
import sys
import os
import os.path
from os import path
#import validators
from nameparser import HumanName
import phonenumbers
#import fhirclient.r4models.capabilitystatement as CS
import fhirclient.r4models.organization as Organization
import fhirclient.r4models.practitioner as Practitioner
import fhirclient.r4models.endpoint as Endpoint
import fhirclient.r4models.humanname as hn
import fhirclient.r4models.address as add
import fhirclient.r4models.contactpoint as cp
import fhirclient.r4models.identifier as id
import fhirclient.r4models.extension as ext
import fhirclient.r4models.codeableconcept as CC
import fhirclient.r4models.coding as coding
import fhirclient.r4models.fhirdate as D
#import fhirclient.r4models.extension as X
#import fhirclient.r4models.contactdetail as CD
#import fhirclient.r4models.narrative as N
#import fhirclient.r4models.bundle as B
import re 

import tarfile
# import fhirclient.r4models.narrative as N
from json import dumps, loads
#from requests import post
#from pathlib import Path
#from collections import namedtuple
from pandas import *
from datetime import datetime, date
#from stringcase import snakecase, titlecase




import sqlite3
import uuid


#from commonmark import commonmark
#from lxml import etree

# TODO
#   Have mapped titles instead of hard coded
#   Change name casing? https://stackoverflow.com/questions/12928416/anyone-know-an-elegant-function-to-fix-name-cases
#   Organization other name (using same base file name)

# GLOBALS
URL_NDH_IDENTIFIER_STATUS = 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-identifier-status'
URL_GEOLOCATION = 'http://hl7.org/fhir/StructureDefinition/geolocation'
ENDPOINT_TYPE_DICT = {"DIRECT": "direct-project",
                      "FHIR": "hl7-fhir-rest",
                      "CONNECT": "connect",
                      "REST": "rest-non-fhir",
                      "SOAP": "ihe-xds",
                      "WEB": "web",
                      "OTHERS": "other"}
ENDPOINT_TYPE_DISPLAY_DICT = {"DIRECT": "Direct Project",
                      "FHIR": "HL7 FHIR",
                      "CONNECT": "connect",
                      "REST": "REST (not FHIR)",
                      "SOAP": "IHE Cross-Enterprise Document Sharing (XDS) - http://wiki.ihe.net/index.php/Cross-Enterprise_Document_Sharing",
                      "WEB": "Web",
                      "OTHERS": "other"}
#fhir_base_url = 'http://hl7.org/fhir/'
#f_jurisdiction = CC.CodeableConcept({
#    "coding": [
#        {
#            "system": "urn:iso:std:iso:3166",
#            "code": "US"
#        }
#    ]
#})
#conf_url = 'http://hl7.org/fhir/StructureDefinition/capabilitystatement-expectation'
#combo_url = 'http://hl7.org/fhir/StructureDefinition/capabilitystatement-search-parameter-combination'
# dict to for SP to get right canonicals, may use spreadsheet or package file in future.
#sp_specials = {
#    'us-core-includeprovenance': 'http://hl7.org/fhir/us/core/SearchParameter/us-core-includeprovenance'}
#this should be a map to the common
#sp_common_list = ["address", "address-city", "address-country", "address-postalcode", "address-state", "address-use", "birthdate", "code", "context", "context-quantity", "context-type", "context-type-quantity", "context-type-value", "date", "description", "email", "encounter", "family", "gender", "given", "identifier", "jurisdiction", "medication", "name", "patient", "phone", "phonetic", "publisher", "status", "status", "telecom", "title", "type", "url", "version"]
#none_list = ['', ' ', 'none', 'n/a', 'N/A', 'N', 'False']
#sep_list = (',', ';', ' ', ', ', '; ')
f_now = D.FHIRDate(str(date.today()))

print('....Loading City Geographical Coordinates.....')
df_cities = read_csv("uscities.csv")
#def markdown(text, *args, **kwargs):
#    return commonmark(text, *args, **kwargs)

def main():
    if (len(sys.argv) < 2):
        print(
            "Error: missing csv file - correct usage is:\n\tpython3 csv_to_fhir.py [csv file]")
        return

    csv = sys.argv[1]
    # Check file name for type of data. 
    # If NPI then load other names file
    
    
    #df_cities.lookup()
    

    print('....processing.....')

    # npidata_pfile notes
    # Entity Type Code
    #   1 = Practitioner
    #   2 = Organization
    #Paging variables
    page_size = 100
    current_page = 0
    currentItem = 0

    csv = sys.argv[1]
    df = read_csv(csv, chunksize=page_size
                 #header=None, 
                 #skiprows=[0, 10], #current_page * page_size,
                 #skiprows=range(1, current_page * page_size),
                 #nrows=page_size
                )

    csv = csv.replace("./", "")
    csv = csv.replace(".\\", "")





    #Load FHIR Endpoint - temp code
    if(False):
        commit_size = 10000
        currentItem = 0
        totalItems = 0
        conn = sqlite3.connect('nppes.db')
        
        cur = conn.cursor()


        cur.execute("DROP TABLE IF EXISTS FHIR_Endpoint")
        # Creating Location table
        table = """ CREATE TABLE FHIR_Endpoint (
                    id CHAR(36) NOT NULL PRIMARY KEY,
                    url CHAR(2048) NOT NULL,
                    source VARCHAR(512),
                    developer VARCHAR(512),
                    docs VARCHAR(2048)
                ); """
        cur.execute(table)
        index = """CREATE INDEX idx_fhir_endpoints_url
                    ON FHIR_Endpoint (url);"""
        cur.execute(index)
        index = """CREATE INDEX idx_fhir_endpoints_source
                    ON FHIR_Endpoint (source);"""
        cur.execute(index)



        for data in df:
            # loop through each row in the page
            for index, row in data.iterrows():
                currentItem = currentItem + 1
                fhir_endpoint = {}

                fhir_endpoint['id'] = str(uuid.uuid4())
                fhir_endpoint['url'] = str(getValue(row, 'url'))
                fhir_endpoint['source'] = str(getValue(row, 'api_information_source_name'))
                fhir_endpoint['developer'] = str(getValue(row, 'certified_api_developer_name'))
                fhir_endpoint['docs'] = str(getValue(row, 'list_source'))
                if(sqlInsert(cur, "FHIR_Endpoint", fhir_endpoint)):
                    totalItems = totalItems + 1

                    if((totalItems != 0) and (totalItems % commit_size == 0)):
                        print("Processed", totalItems, "FHIR_Endpoint records.", datetime.now().strftime("%H:%M:%S"))
                        conn.commit()

        conn.commit()









    if(csv.startswith('npidata_')):
        practitioners = []
        organizations = []
        #print(df.info())
        #for index, row in df.iterrows():
            #if(notnull(row['Entity Type Code'])):
        #    print(row[0], row['Entity Type Code'], row['Provider Organization Name (Legal Business Name)'], row['Provider Last Name (Legal Name)'], row['Provider First Line Business Mailing Address'])
        # Loop through each page, Starting with Organizations
        for data in df:
            # loop through each row in the page
            for index, row in data.iterrows():
                currentItem = currentItem + 1
                #if(notnull(row['Entity Type Code'])):
                if(row['Entity Type Code'] == 2):
                    if(notnull(row['Provider Organization Name (Legal Business Name)'])):
                        #print(row[0], row['Entity Type Code'], row['Provider Organization Name (Legal Business Name)'], row['Provider Last Name (Legal Name)'], row['Provider First Line Business Mailing Address'])
                        #print(loadOrganization(row).as_json())
                        organizations.append(loadOrganization(row))
                    else:
                        print(row)
        # Loop through each page, Practitioners
        for data in df:
            # loop through each row in the page
            for index, row in data.iterrows():
                currentItem = currentItem + 1
                #if(notnull(row['Entity Type Code'])):
                if(row['Entity Type Code'] == 2):
                    if(notnull(row['Provider Organization Name (Legal Business Name)'])):
                        #print(row[0], row['Entity Type Code'], row['Provider Organization Name (Legal Business Name)'], row['Provider Last Name (Legal Name)'], row['Provider First Line Business Mailing Address'])
                        #print(loadOrganization(row).as_json())
                        organizations.append(loadOrganization(row))
                    else:
                        print(row)
                elif(row['Entity Type Code'] == 1):
                    #print(row[0], row['Entity Type Code'], row['Provider Organization Name (Legal Business Name)'], row['Provider Last Name (Legal Name)'], row['Provider First Line Business Mailing Address'])
                    #print(loadPractitioner(row).as_json())
                    practitioners.append(loadPractitioner(row))

                #Write out resources
                if(currentItem % 1000 == 0):
                    print("Loaded", currentItem, "NPI records.")

    elif(csv.startswith('endpoint_')):
        endpoints = []
        print("test")
        # Loop through each page
        for data in df:
            # loop through each row in the page
            for index, row in data.iterrows():
                currentItem = currentItem + 1
                print(loadEndpoint(row).as_json())
                #organizations.append(loadEndpoint(row))
                

                #Write out resources
                if(currentItem % 1000 == 0):
                    print("Loaded", currentItem, "Endpoint records.")

def loadOrganization(data):
    organization = Organization.Organization()
    
    npi = id.Identifier()
    npi.system = "http://hl7.org/fhir/sid/us-npi"
    npi.value = str(data['NPI'])

    npiStatus = ext.Extension()
    npiStatus.url = URL_NDH_IDENTIFIER_STATUS
    npiStatus.valueCode = 'active'
    npi.extension = [npiStatus]

    organization.identifier = [npi]

    organization.active = True

    orgType = CC.CodeableConcept()
    orgTypeCode = coding.Coding()
    orgTypeCode.system = 'http://hl7.org/fhir/us/ndh/CodeSystem/OrgTypeCS'
    orgTypeCode.code = 'prvgrp'
    orgTypeCode.display = 'Provider Group'

    orgType.coding = [orgTypeCode]
    organization.type = [orgType]

    name = HumanName(data['Provider Organization Name (Legal Business Name)'])
    name.capitalize()
    
    organization.name = str(name).replace("Llc", "LLC").replace("llc", "LLC")
    
    
    ### Organization address
    mailingAddress = loadAddress(getValue(data, 'Provider First Line Business Mailing Address'),
                                 getValue(data, 'Provider Second Line Business Mailing Address'),
                                 getValue(data, 'Provider Business Mailing Address City Name'),
                                 getValue(data, 'Provider Business Mailing Address State Name'),
                                 str(getValue(data, 'Provider Business Mailing Address Postal Code')),
                                 getValue(data, 'Provider Business Mailing Address Country Code (If outside U.S.)'),
                                 'work',
                                 'postal')
    practiceAddress = loadAddress(getValue(data, 'Provider First Line Business Practice Location Address'),
                                  getValue(data, 'Provider Second Line Business Practice Location Address'),
                                  getValue(data, 'Provider Business Practice Location Address City Name'),
                                  getValue(data, 'Provider Business Practice Location Address State Name'),
                                  str(getValue(data, 'Provider Business Practice Location Address Postal Code')),
                                  getValue(data, 'Provider Business Practice Location Address Country Code (If outside U.S.)'),
                                  'work',
                                  'physical')
    organization.address = []
    if(mailingAddress != None):
        organization.address = [mailingAddress]
    if(practiceAddress != None):
        organization.address.append(practiceAddress)

    #print(df_cities.at[df_cities.loc[(df_cities['city_ascii'] == 'Phoenix') & (df_cities['state_id'] == 'AZ')].index[0], 'lat'])
    
        ### Practitioner telecom
    #getValue(data, 'Provider Last Name (Legal Name)'
    phoneNumber = str(getValue(data, 'Provider Business Mailing Address Telephone Number'))
    if(phoneNumber != ''):
        phone = cp.ContactPoint()
        phone.system = 'phone'
        phone.value = phoneNumber
        phone.use = 'work'
        organization.telecom = [phone]

    phoneNumber = str(getValue(data, 'Provider Business Mailing Address Fax Number'))
    if(phoneNumber != ''):
        phone = cp.ContactPoint()
        phone.system = 'fax'
        phone.value = phoneNumber
        phone.use = 'work'
        if(organization.telecom == None):
            organization.telecom = []
        organization.telecom.append(phone)

    phoneNumber = str(getValue(data, 'Provider Business Practice Location Address Telephone Number'))
    if(phoneNumber != ''):
        phone = cp.ContactPoint()
        phone.system = 'phone'
        phone.value = phoneNumber
        phone.use = 'work'
        if(organization.telecom == None):
            organization.telecom = []
        organization.telecom.append(phone)

    phoneNumber = str(getValue(data, 'Provider Business Practice Location Address Fax Number'))
    if(phoneNumber != ''):
        phone = cp.ContactPoint()
        phone.system = 'fax'
        phone.value = phoneNumber
        phone.use = 'work'
        if(organization.telecom == None):
            organization.telecom = []
        organization.telecom = [phone]

    return organization

def loadPractitioner(data):
    practitioner = Practitioner.Practitioner()
    # TODO - https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-ndh-Practitioner.html
    # Endpoint Reference
    # verification status?
    # Where to place taxonomy? How to address PractitionerRole?
    # NPI Deactivation Date identifier.period.end (NPI Deactivation Date) for those without a reactivation date?
    # 1-15 of Healthcare Provider Taxonomy Code_1	Provider License Number_1	Provider License Number State Code_1	Healthcare Provider Primary Taxonomy Switch_1
    # 1-50 of Other Provider Identifier_1	Other Provider Identifier Type Code_1	Other Provider Identifier State_1	Other Provider Identifier Issuer_1

    
    ### Practitioner Identifiers
    npi = id.Identifier()
    npi.system = "http://hl7.org/fhir/sid/us-npi"
    npi.value = str(data['NPI'])

    npiStatus = ext.Extension()
    npiStatus.url = URL_NDH_IDENTIFIER_STATUS
    npiStatus.valueCode = 'active'
    npi.extension = [npiStatus]

    practitioner.identifier = [npi]

    practitioner.active = True
    genderCode = getValue(data, 'Provider Gender Code')
    if(genderCode == 'F'):
        practitioner.gender = "female"
    elif(genderCode == 'M'):
        practitioner.gender = "male"

    ### Practitioner Names
    otherNameType = ''
    if(getValue(data, 'Provider Other Last Name Type Code') == 1):
        otherNameType = 'old'
    legalName = loadHumanName(getValue(data, 'Provider Last Name (Legal Name)'),
                                getValue(data, 'Provider First Name'),
                                getValue(data, 'Provider Middle Name'),
                                getValue(data, 'Provider Name Prefix Text'),
                                getValue(data, 'Provider Name Suffix Text'), 
                                getValue(data, 'Provider Credential Text'),
                                'official')
    otherName = loadHumanName(getValue(data, 'Provider Other Last Name'),
                                getValue(data, 'Provider Other First Name'),
                                getValue(data, 'Provider Other Middle Name'),
                                getValue(data, 'Provider Other Name Prefix Text'),
                                getValue(data, 'Provider Other Name Suffix Text'), 
                                getValue(data, 'Provider Other Credential Text'),
                                otherNameType)
    
    if(otherName != None):
        practitioner.name = [legalName,otherName]
    else:
        practitioner.name = [legalName]

    #if(getValue(data, 'Provider Other Last Name') != ""):
    #    sys.exit()
    
    ### Practitioner address
    mailingAddress = loadAddress(getValue(data, 'Provider First Line Business Mailing Address'),
                                 getValue(data, 'Provider Second Line Business Mailing Address'),
                                 getValue(data, 'Provider Business Mailing Address City Name'),
                                 getValue(data, 'Provider Business Mailing Address State Name'),
                                 str(getValue(data, 'Provider Business Mailing Address Postal Code')),
                                 getValue(data, 'Provider Business Mailing Address Country Code (If outside U.S.)'),
                                 'work',
                                 'postal')
    practiceAddress = loadAddress(getValue(data, 'Provider First Line Business Practice Location Address'),
                                  getValue(data, 'Provider Second Line Business Practice Location Address'),
                                  getValue(data, 'Provider Business Practice Location Address City Name'),
                                  getValue(data, 'Provider Business Practice Location Address State Name'),
                                  str(getValue(data, 'Provider Business Practice Location Address Postal Code')),
                                  getValue(data, 'Provider Business Practice Location Address Country Code (If outside U.S.)'),
                                  'work',
                                  'physical')
    practitioner.address = []
    if(mailingAddress != None):
        practitioner.address = [mailingAddress]
    if(practiceAddress != None):
        practitioner.address.append(practiceAddress)

    #print(df_cities.at[df_cities.loc[(df_cities['city_ascii'] == 'Phoenix') & (df_cities['state_id'] == 'AZ')].index[0], 'lat'])
    
        ### Practitioner telecom
    #getValue(data, 'Provider Last Name (Legal Name)'
    phoneNumber = str(getValue(data, 'Provider Business Mailing Address Telephone Number'))
    if(phoneNumber != ''):
        phone = cp.ContactPoint()
        phone.system = 'phone'
        phone.value = phoneNumber
        phone.use = 'work'
        practitioner.telecom = [phone]

    phoneNumber = str(getValue(data, 'Provider Business Mailing Address Fax Number'))
    if(phoneNumber != ''):
        phone = cp.ContactPoint()
        phone.system = 'fax'
        phone.value = phoneNumber
        phone.use = 'work'
        if(practitioner.telecom == None):
            practitioner.telecom = []
        practitioner.telecom.append(phone)

    phoneNumber = str(getValue(data, 'Provider Business Practice Location Address Telephone Number'))
    if(phoneNumber != ''):
        phone = cp.ContactPoint()
        phone.system = 'phone'
        phone.value = phoneNumber
        phone.use = 'work'
        if(practitioner.telecom == None):
            practitioner.telecom = []
        practitioner.telecom.append(phone)

    phoneNumber = str(getValue(data, 'Provider Business Practice Location Address Fax Number'))
    if(phoneNumber != ''):
        phone = cp.ContactPoint()
        phone.system = 'fax'
        phone.value = phoneNumber
        phone.use = 'work'
        if(practitioner.telecom == None):
            practitioner.telecom = []
        practitioner.telecom = [phone]

    return practitioner

def loadEndpoint(data):
    endpoint = Endpoint.Endpoint()
    # Link through NPI

    ### Practitioner Identifiers
    npi = id.Identifier()
    npi.system = "http://hl7.org/fhir/sid/us-npi"
    npi.value = str(data['NPI'])

    npiStatus = ext.Extension()
    npiStatus.url = URL_NDH_IDENTIFIER_STATUS
    npiStatus.valueCode = 'active'
    npi.extension = [npiStatus]

    endpoint.identifier = [npi]

    endpoint.status = "active"
   
    endPointType = ENDPOINT_TYPE_DICT[str(getValue(data, 'Endpoint Type'))]
    print(endPointType)
    if(endPointType != ''):

        connectionTypeCoding = coding.Coding()
        connectionTypeCoding.system = 'http://hl7.org/fhir/us/ndh/CodeSystem/EndpointConnectionTypeCS'
        connectionTypeCoding.code = endPointType
        connectionTypeCoding.display = ENDPOINT_TYPE_DISPLAY_DICT[str(getValue(data, 'Endpoint Type'))]
        endpoint.connectionType = connectionTypeCoding

    payload = ENDPOINT_TYPE_DICT[str(getValue(data, 'Endpoint Type'))]
    if(payload != ''):
        payloadType = CC.CodeableConcept()
        payloadTypeCoding = coding.Coding()
        payloadTypeCoding.system = 'http://hl7.org/fhir/us/ndh/CodeSystem/EndpointPayloadTypeCS'
        payloadTypeCoding.code = 'NA'
        payloadTypeCoding.display = "'Not Applicable"
        payloadType.coding = [payloadTypeCoding]
        endpoint.payloadType = [payloadType]
    #endpoint.payloadType

    endpoint.address = str(getValue(data, 'Endpoint'))
    #'hl7-fhir-rest'
    #'direct-project'
    #'http://terminology.hl7.org/CodeSystem/endpoint-connection-type'
    return endpoint

def loadHumanName(last, first, middle, prefix, suffix, credential, use=''):
    if(last != ''):
        name = hn.HumanName()
        displayName = HumanName()
        
        
        lastName = HumanName(last)
        lastName.capitalize()
        name.family = str(lastName)
        displayName.last = str(lastName)

        firstName = HumanName(first)
        firstName.capitalize()
        displayName.first = str(firstName)
        
        if(middle != ''):
            middleName = HumanName(middle)
            middleName.capitalize()
            displayName.middle = str(middleName)
            name.given = [str(firstName), str(middleName)]
        else:
            name.given = [str(firstName)]

        
        if(prefix != ''):
            prefixName = HumanName(prefix)
            prefixName.capitalize()
            displayName.title = str(prefixName)
            name.prefix = [str(prefixName)]

        suffixList = []
        if(suffix != ''):
            suffixName = HumanName(suffix)
            suffixName.capitalize()
            displayName.suffix = str(suffixName)
            suffixList.append(str(suffixName))
        if(credential != ''):
            suffixList.append(credential)
            displayName.suffix = displayName.suffix + " " + credential
        if(len(suffixList) > 0):
            name.suffix = suffixList
            
        
        if(use in ['usual', 'official', 'temp', 'nickname', 'anonymous', 'old', 'maiden']):
            name.use = use

        name.text = str(displayName)

        return name

def loadAddress(firstLine, secondLine, city, state, postal, country, use, type):
    # TODO
    # Format address into text
    # geolocation extension
    
    if(firstLine != ''):
        address = add.Address()
        addressLines = []
        if(firstLine != ''):
            addressLines.append(firstLine)
        if(secondLine != ''):
            addressLines.append(secondLine)
        if(len(addressLines) > 0):
            address.line = addressLines
        if(city != ''):
            text = HumanName(city)
            text.capitalize()
            address.city = str(text)
        if(state != ''):
            address.state = state
        if(postal != ''):
            address.postalCode = postal
        if(country != ''):
            address.country = country
        
        if(use != ''):
            address.use = use
        if(type != ''):
            address.type = type

        # THis is not perfect seraching as the names are not always a perfect match, particularly because of formatting, but it can add a lot of geolocation extensions
        #print("Searching...", address.city,address.state)
        coordinates = df_cities.loc[(df_cities['city_ascii'] == address.city) & (df_cities['state_id'] == address.state)]
        
        if(coordinates.empty == False):
            geoLocation = ext.Extension()
            geoLocation.url = URL_GEOLOCATION
            latitude = ext.Extension()
            latitude.url = 'latitude'
            latitude.valueDecimal = df_cities.at[coordinates.index[0], 'lat']
            longitude = ext.Extension()
            longitude.url = 'longitude'
            longitude.valueDecimal = df_cities.at[coordinates.index[0], 'lng']
            geoLocation.extension = [latitude, longitude]
            address.extension = [geoLocation]

        return address

def sqlInsert(write_cur, table, dict):
    ret_val = False
    query = "INSERT INTO {table_name} ({columns}) VALUES ({value_placeholders})".format(
                table_name=table,
                columns=", ".join(dict.keys()),
                value_placeholders=", ".join(["?"] * len(dict)),
            )
    #print(list(dict.values()))
    
    try:
        write_cur.execute(query, list(dict.values()))
        ret_val = True
    except Exception  as err:
        ret_val = False
    
    return ret_val

def getValue(data, title):
    if(notnull(data[title])):
        return data[title]
    else:
        return ""



main()

    
    