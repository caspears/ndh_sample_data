
import sys
import os.path
import glob
import time
from datetime import datetime

import argparse
import sqlite3
from pandas import *
import uuid
from nameparser import HumanName


# TODO PR address set use to work
# DONE? ENDPOINTS HAVE AFFILIATED ADDRESSES WHICH WILL NEED TO BE USED TO CONNECT THE ENDPOINTS TO THE RIGHT PRATITIONERROLES. Perhaps add location_id to Endpoints table and use that to link the two
# findLocation and all CALLERS ONLY ASSUME ONE LOCATION IS POSSIBLE, however, that has changed and the code needs to be updated (Need to change to findLocations)

# ADD Phone numbers to PR
# Loading locations will need to look for duplicates with each write. Perhaps there is a way to handle this with a uniqueneess and to return some ID of a match?
# do not worry about organization hierarchies
# Load practice locations and post-process to match for Practitioner Role (Or perhaps loading PR)
# Consider execute many to reduce the number of writes.
# Use a different cursor for writing (e.g. the location)
# only endpoints that are linked to used NPIs (unfiltered)
# Duplicate endpoints. Why?
# Proper datatype handling and format (e.g. zip code and phone numbers ending in ".0")



### Globals
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

ORGANIZATION_ENTITY_TYPE = 2
PRACTITIONER_ENTITY_TYPE = 1
PRACTITIONER_ROLE_ENTITY_TYPE = 3
ENDPOINT_ENTITY_TYPE = 4

LOCATION_TYPE = {'mail' : 1, 'physical' : 2, 'secondary' : 3}

processOrganizations = True
processLocations = True
processTEMPLocations = True
processPractitioners = True
processPractitionerRoles = True
processEndpoints = True

locationStart = 0
organizationStart = 0
practitionerStart = 0
endpointStart = 0
#430000




print('....Loading City Geographical Coordinates.....')
df_cities = read_csv("uscities.csv")
#df['Courses'] = df['Courses'].apply(str.upper)
df_cities['city_ascii'] = df_cities['city_ascii'].apply(str.upper)


#TODO HAPI Load order Enpoint, Location, Organization, Practioner, PractionerRole

# TODO - Add multi-threading?

def main():
    #if (len(sys.argv) < 2):
    #    print(
    #        "Error: missing csv file - correct usage is:\n\tpython3 csv_to_fhir.py [csv file]")
    #    return

    #csv = sys.argv[1]

    # Check file name for type of data. 
    # If NPI then load other names file
    
    
    #df_cities.lookup()
    
    parser = argparse.ArgumentParser(description="""NPPES Preprocessing for National Directory Import""")
    parser.add_argument('dir_path', type=path_arg, help="Directory containing files to import (maximum, one of each)")
    parser.add_argument("-s", "--states", nargs='+', help="States to include (practicing address only)", required=False)
    
    
    args = parser.parse_args()

    #dir_path = sys.argv[1]
    dir_path = args.dir_path
    state_filter = args.states
    print(dir_path)
    print()
    

    print(f'....processing files in {dir_path} .....')
    if(state_filter):
        print(f'Including only the following states {state_filter}')
    
    
    # npidata_pfile notes
    # Entity Type Code
    #   1 = Practitioner
    #   2 = Organization
    #Paging variables
    conn = sqlite3.connect('nppes.db')
    #cur = conn.cursor()
    table_cur = conn.cursor()
    read_cur = conn.cursor()
    read_loc_cur = conn.cursor()
    write_cur = conn.cursor()
    conn.row_factory = dict_factory

    initializeDB(table_cur)
    conn.commit()
    #print(uuid.uuid4())

    #mydict = {'user': 'Bot', 'version': 0.15, 'items': 43, 'methods': 'standard', 'time': 1536304833437, 'logs': 'no', 'status': 'completed'}

    #columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in mydict.keys())
    #values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in mydict.values())
    #sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('mytable', columns, values)
    
    #print (sql)
    
    
    page_size = 1000
    commit_size = 10000
    currentLocItem = 0
    currentOrgItem = 0
    currentPractItem = 0
    currentEPItem = 0

    totalOrgItem = 0
    totalPractItem = 0
    totalPractRoleItem = 0
    totalPractOrgRoleItem = 0
    totalEPItem = 0
    totalLocItem = 0

    secondaryLocationNPIs = []


    #for path in os.listdir(dir_path):
        # check if current path is a file
    #    if os.path.isfile(os.path.join(dir_path, path)):
    #        res.append(path)

    
    ### Load NPI
    start = time.time()

    '''
    files = glob.glob(os.path.join(dir_path, "pl_*.csv"))
    if(len(files) > 1):
        print("Only loading the first NPI file", files[0])
    if(len(files) > 0):
        csv = files[0]
    
        
        print("Loading Location Data from", csv)
        if(processOrganizations):
            pldf = read_csv(csv #, chunksize=page_size
                        #header=None, 
                        #skiprows=[0, 10], #current_page * page_size,
                        #skiprows=range(1, current_page * page_size),
                        #nrows=page_size
                        )
            
    input('Let us wait for user input.\n')
    #1811355597
    print(pldf['Provider Secondary Practice Location Address - State Name'].where(pldf['NPI'] == "1811355597"))


    sys.exit()
    '''


    
    
        
    # Preload temporary secondary locations
    if(processTEMPLocations):
        files = glob.glob(os.path.join(dir_path, "pl_*.csv"))
        if(len(files) > 1):
            print("Only loading the first PL file", files[0])
        if(len(files) > 0):
            csv = files[0]
            print("Loading Secondary Practice Location Data from", csv)
            df = read_csv(csv, chunksize=page_size
                        #header=None, 
                        #skiprows=[0, 10], #current_page * page_size,
                        #skiprows=range(1, current_page * page_size),
                        #nrows=page_size
                        )
            
            #csv = csv.replace("./", "")
            #csv = csv.replace(".\\", "")
            #if(csv.startswith('npidata_')):
            #practitioners = []
            #organizations = []
            # Load Organizations in to DB
            for data in df:
                # loop through each row in the page
                for index, row in data.iterrows():
                    
                    if(notnull(row['NPI'])):
                        currentLocItem = currentLocItem + 1

                        if(currentLocItem > locationStart):
                            # Always process if not filtering on states, or if filtering on states, check if there is a physical location and if so, whether it is to be included.
                            if((state_filter == None) or (str(getValue(row, 'Provider Secondary Practice Location Address - State Name')) in state_filter)):
                            #if(True):
                                location = loadSecondaryLocation(row)

                                

                                # TODO, See if location already exists
                                # TODO Search for provider and create PR record
                                # TODO See if there are any orgs that match NPI and log.
                                if((len(location) > 0) and ("id" in location) and (len(location['id']) > 0) and ("npi" in location) and (len(location['npi']) > 0)):
                                    if(sqlInsert(write_cur, "TEMP_Location", location)):
                                        totalLocItem = totalLocItem + 1
                                    if(location['npi'] not in secondaryLocationNPIs):
                                        # Store secondary location NPIs in memory to be more efficient, look in memory to see if it exists, before going to the DB.
                                        secondaryLocationNPIs.append(location['npi'])
                                    

                        #else:
                        #    print(row)

                        #Write to database
                        if((totalLocItem != 0) and (totalLocItem % commit_size == 0)):
                            print("Processed", totalLocItem, "Temporary Secondary Location records.", datetime.now().strftime("%H:%M:%S"))
                            conn.commit()
            conn.commit()
            print("Loaded a total of", totalLocItem, "Temporary Secondary Location records.")
            totalLocItem = 0
            


            end = time.time()
            print("Execution time (in minutes)", (end - start)/60)



    files = glob.glob(os.path.join(dir_path, "npidata_*.csv"))
    if(len(files) > 1):
        print("Only loading the first NPI file", files[0])
    if(len(files) > 0):
        csv = files[0]
    
        
        print("Loading NPI Data from", csv)
        if(processOrganizations):
            df = read_csv(csv, chunksize=page_size
                        #header=None, 
                        #skiprows=[0, 10], #current_page * page_size,
                        #skiprows=range(1, current_page * page_size),
                        #nrows=page_size
                        )
            
            #csv = csv.replace("./", "")
            #csv = csv.replace(".\\", "")
            #if(csv.startswith('npidata_')):
            #practitioners = []
            #organizations = []
            # Load Organizations in to DB
            for data in df:
                # loop through each row in the page
                for index, row in data.iterrows():
                    if(row['Entity Type Code'] == 2):
                        
                            #sys.exit()
                        if(notnull(row['Provider Organization Name (Legal Business Name)'])):
                            currentOrgItem = currentOrgItem + 1

                            if(currentOrgItem > organizationStart):
                                secondary_locations = None
                                # Get Secondary practice locations for the organization to see if one is in the state filter list
                                if(row['NPI'] in secondaryLocationNPIs):
                                    if(state_filter == None):
                                        read_cur.execute('SELECT * FROM TEMP_Location WHERE npi = ?', (row['NPI'],))
                                    else:    
                                        #sql="select * from TEMP_Location where npi = ? AND state in ({seq})".format(seq=','.join(['?']*len(state_filter)))
                                        #cur.execute(sql, [[str(row['NPI'])] + state_filter])
                                        sql="select * from TEMP_Location where npi = ? AND state in ({seq})".format(seq=','.join(['?']*len(state_filter)))
                                        test = [[row['NPI']] + state_filter]
                                        read_cur.execute(sql, [[row['NPI']] + state_filter][0])
                                    secondary_locations = read_cur.fetchall()
                                
                                # Always process if not filtering on states, or if filtering on states, check if there is a physical location and if so, whether it is to be included.
                                if((state_filter == None) or (str(getValue(row, 'Provider Business Practice Location Address State Name')) in state_filter) or (str(getValue(row, 'Provider Business Mailing Address State Name')) in state_filter) or (secondary_locations!= None)):
                                    locations = loadLocations(row, secondary_locations)
                                    
                                    #if((state_filter == None) or (('physical' in locations) and (locations['physical']['state'] in state_filter))):
                                    if(True):
                                        organization = loadOrganization(row)
                                        if(len(organization) > 0):
                                            if(sqlInsert(write_cur, "Organization", organization)):
                                                totalOrgItem = totalOrgItem + 1

                                            # multiple organizations may have the same address. In that case, they will be considered different location records. If de-dupe is necessary, that will have to be done in a post process.
                                            for key, value in locations.items():
                                                location = value
                                                # Each Organization needs to have a unique Location (which could have different hours, which will be mocked up)
                                                #existing_location_id = findLocation(read_cur, location)
                                                entity_location = {}
                                                entity_location['id'] = str(uuid.uuid4())
                                                
                                                
                                                #if((existing_location_id) and (len(existing_location_id) > 10)):
                                                #    entity_location['location_id'] = existing_location_id
                                                #else:
                                                if(sqlInsert(write_cur, "Location", location)):
                                                    totalLocItem = totalLocItem + 1
                                                    entity_location['location_id'] = location['id']
                                                    
                                                    entity_location['entity_id'] = organization['id']
                                                    entity_location['entity_type'] = ORGANIZATION_ENTITY_TYPE

                                                    entity_location['location_type'] = LOCATION_TYPE[key.split("_")[0]]
                                                    if(sqlInsert(write_cur, "Entity_Location", entity_location)):
                                                        totalLocItem = totalLocItem + 1
                                                # set the key for the location for the organization record
                                                #row['mail_location'] = locations['mail']['id']

                                            '''
                                            if('physical' in locations):
                                                #if(('mail' in locations) and (locations['mail'] == locations['physical'])):
                                                if(('mail' in locations) and (equal_dicts(locations['mail'], locations['physical'], 'id') == False)):
                                                    # set the key for the location for the organiztion record
                                                    #row['physical_location'] = locations['mail']['id']

                                                    entity_location = {}
                                                    entity_location['id'] = str(uuid.uuid4())
                                                    entity_location['entity_id'] = organization['id']
                                                    entity_location['entity_type'] = ORGANIZATION_ENTITY_TYPE
                                                    entity_location['location_id'] = locations['mail']['id']
                                                    entity_location['location_type'] = PRACTICE_LOCATION_TYPE

                                                    sqlInsert(cur, "Entity_Location", entity_location)

                                                else:
                                                    sql = sqlInsert(cur, "Location", locations['physical'])   
                                                    totalLocItem = totalLocItem + 1
                                                    # set the key for the location for the organiztion record
                                                    row['physical_location'] = locations['physical']['id']

                                                    entity_location = {}
                                                    entity_location['id'] = str(uuid.uuid4())
                                                    entity_location['entity_id'] = organization['id']
                                                    entity_location['entity_type'] = ORGANIZATION_ENTITY_TYPE
                                                    entity_location['location_id'] = locations['physical']['id']
                                                    entity_location['location_type'] = PRACTICE_LOCATION_TYPE

                                                    sqlInsert(cur, "Entity_Location", entity_location)
                                            
                                            
                                            

                                            

                                            
                                            entity_location = {}
                                            entity_location['id'] = str(uuid.uuid4())
                                            entity_location['entity_id'] = organization['id']
                                            entity_location['entity_type'] = ORGANIZATION_ENTITY_TYPE
                                            entity_location['location_id'] = locations['mail']['id']
                                            entity_location['location_type'] = PRACTICE_LOCATION_TYPE

                                            sqlInsert(cur, "Entity_Location", entity_location)
                                            '''

                        #else:
                        #    print(row)

                        #Write to database
                        if(currentOrgItem % commit_size == 0):
                            print("Processed", currentOrgItem, "Organization records.")
                            conn.commit()
            conn.commit()
            print("Loaded a total of", totalOrgItem, "Organization records.")
            


            end = time.time()
            print("Execution time (in minutes)", (end - start)/60)

        if(processPractitioners):
            df = read_csv(csv, chunksize=page_size)

            # TODO
            #    Can Practitioners exist in the file more than once (more than one with the same NPI)? Is this  a way to link a provider to more than one location/organization?
            #    Load different PractitionerRole for mailing address?
            bExit = False
            # Load Practitioners in to DB
            
            for data in df:
                # loop through each row in the page
                for index, row in data.iterrows():
                    if(row['Entity Type Code'] == 1):
                        if(notnull(row['Provider Last Name (Legal Name)'])):
                            currentPractItem = currentPractItem + 1

                            if(currentPractItem > practitionerStart):
                                secondary_locations = None
                                # Get Secondary practice locations for the organization to see if one is in the state filter list
                                if(row['NPI'] in secondaryLocationNPIs):
                                    if(state_filter == None):
                                        read_cur.execute('SELECT * FROM TEMP_Location WHERE npi = ?', (row['NPI'],))
                                    else:    
                                        #sql="select * from TEMP_Location where npi = ? AND state in ({seq})".format(seq=','.join(['?']*len(state_filter)))
                                        #cur.execute(sql, [[str(row['NPI'])] + state_filter])
                                        sql="select * from TEMP_Location where npi = ? AND state in ({seq})".format(seq=','.join(['?']*len(state_filter)))
                                        test = [[row['NPI']] + state_filter]
                                        read_cur.execute(sql, [[row['NPI']] + state_filter][0])
                                    secondary_locations = read_cur.fetchall()
                                # Always process if not filtering on states, or if filtering on states, check if there is a physical location and if so, whether it is to be included.
                                if((state_filter == None) or (str(getValue(row, 'Provider Business Practice Location Address State Name')) in state_filter) or (str(getValue(row, 'Provider Business Mailing Address State Name')) in state_filter)):
                                    locations = loadLocations(row, secondary_locations)
                                    
                                    #if((state_filter == None) or (('physical' in locations) and(locations['physical']['state'] in state_filter))):
                                    if(True):
                                        practitioner = loadPractitioner(row)



                                        if(len(practitioner) > 0):
                                            if(sqlInsert(write_cur, "Practitioner", practitioner)):
                                                totalPractItem = totalPractItem + 1
                                            # capture the Practitioner ID to store in PractionerRole
                                            row['practitioner_id'] = practitioner['id']

                                            # multiple organizations may have the same address. In that case, they will be considered different location records. If de-dupe is necessary, that will have to be done in a post process.
                                            for key, value in locations.items():
                                                if('organization_id' in row):
                                                    del row['organization_id']   
                                                location = value
                                                if(key.split("_")[0] == 'secondary'):
                                                        test = location
                                                existing_location_id = findLocation(read_cur, location)
                                                entity_location = {}
                                                entity_location['id'] = str(uuid.uuid4())
                                                # If there is an existing matching location, use that record, otherwise create a new one
                                                if((existing_location_id) and (len(existing_location_id) > 10)):
                                                    entity_location['location_id'] = existing_location_id
                                                else:
                                                    if(sqlInsert(write_cur, "Location", location)):
                                                        totalLocItem = totalLocItem + 1
                                                    entity_location['location_id'] = location['id']
                                                    
                                                entity_location['entity_id'] = practitioner['id']
                                                entity_location['entity_type'] = PRACTITIONER_ENTITY_TYPE

                                                entity_location['location_type'] = LOCATION_TYPE[key.split("_")[0]]
                                                sqlInsert(write_cur, "Entity_Location", entity_location)
                                                row['pr_location_id'] = entity_location['location_id']
                                                # set the key for the location for the organization record
                                                #row['mail_location'] = locations['mail']['id']

                                                practitionerRole = loadPractitionerRole(row)
                                                if(len(practitionerRole) > 0):
                                                    if(sqlInsert(write_cur, "PractitionerRole", practitionerRole)):
                                                        totalPractRoleItem = totalPractRoleItem + 1

                                                # Create PractitionerRole record if practice address matches an organizations practice address
                                                if((key == 'physical') or (key.split("_")[0] == 'secondary')):
                                                    if(key.split("_")[0] == 'secondary'):
                                                        test = location
                                                    # Find entity (organization) locations that have the same location_id
                                                    practicing_locations = findPracticingLocations(read_cur, entity_location['location_id'])
                                                    if(len(practicing_locations) > 10):
                                                        # If it gets here it seems problematic. Possibly matching on the wrong thing as it is unlikely for a practitioner to be in more that 10 practicing locations
                                                        test = practicing_locations
                                                    for practicing_location in practicing_locations:
                                                        
                                                        #Location found
                                                        # TODO Match with all found locations!!!!!!!!!
                                                        #row['location'] = practicing_location['id']
                                                        #if('organization_id' in practicing_location):
                                                        if('organization_id' in row):
                                                            del row['organization_id']   
                                                        if(str(practicing_location['entity_type']) == str(ORGANIZATION_ENTITY_TYPE)):
                                                            #create practitioner Role
                                                            #print("Found organization with ID:", locationInfo['org_id'])
                                                            row['organization_id'] = practicing_location['entity_id']
                                                        row['pr_location_id'] = location['id']
                                                        practitionerRole = loadPractitionerRole(row)
                                                        if(len(practitionerRole) > 0):
                                                            if(sqlInsert(write_cur, "PractitionerRole", practitionerRole)):
                                                                totalPractOrgRoleItem = totalPractOrgRoleItem + 1

                                                            entity_location = {}
                                                            entity_location['id'] = str(uuid.uuid4())
                                                            entity_location['location_id'] = location['id']
                                                                
                                                            entity_location['entity_id'] = practitionerRole['id']
                                                            entity_location['entity_type'] = PRACTITIONER_ROLE_ENTITY_TYPE

                                                            entity_location['location_type'] = LOCATION_TYPE[key.split("_")[0]]
                                                            sqlInsert(write_cur, "Entity_Location", entity_location)
                                                    if('organization_id' in row):
                                                        del row['organization_id']     


                                            '''
                                            if('physical' in locations):
                                                #if(('mail' in locations) and (locations['mail'] == locations['physical'])):
                                                if(('mail' in locations) and (equal_dicts(locations['mail'], locations['physical'], 'id') == False)):
                                                    # set the key for the location for the organization record
                                                    #row['physical_location'] = locations['mail']['id']

                                                    entity_location = {}
                                                    entity_location['id'] = str(uuid.uuid4())
                                                    entity_location['entity_id'] = organization['id']
                                                    entity_location['entity_type'] = ORGANIZATION_ENTITY_TYPE
                                                    entity_location['location_id'] = locations['mail']['id']
                                                    entity_location['location_type'] = PRACTICE_LOCATION_TYPE

                                                    sqlInsert(cur, "Entity_Location", entity_location)

                                                else:
                                                    sql = sqlInsert(cur, "Location", locations['physical'])   
                                                    totalLocItem = totalLocItem + 1
                                                    # set the key for the location for the organiztion record
                                                    row['physical_location'] = locations['physical']['id']

                                                    entity_location = {}
                                                    entity_location['id'] = str(uuid.uuid4())
                                                    entity_location['entity_id'] = organization['id']
                                                    entity_location['entity_type'] = ORGANIZATION_ENTITY_TYPE
                                                    entity_location['location_id'] = locations['physical']['id']
                                                    entity_location['location_type'] = PRACTICE_LOCATION_TYPE

                                                    sqlInsert(cur, "Entity_Location", entity_location)
                                            
                                            
                                            

                                            

                                            
                                            entity_location = {}
                                            entity_location['id'] = str(uuid.uuid4())
                                            entity_location['entity_id'] = organization['id']
                                            entity_location['entity_type'] = ORGANIZATION_ENTITY_TYPE
                                            entity_location['location_id'] = locations['mail']['id']
                                            entity_location['location_type'] = PRACTICE_LOCATION_TYPE

                                            sqlInsert(cur, "Entity_Location", entity_location)
                                            '''
                                    # Always process if not filtering on states, or if filtering on states, check if there is a physical location and if so, whether it is to be included.
                                    #if((state_filter == None) or (('physical' in locations) and (locations['physical']['state'] in state_filter))):
                                    '''
                                    if(True):
                                        # TODO, see if location exists, if so, use the id of found location and also create a link to the Organization in which the location exists for.
                                        if('mail' in locations):

                                            # multiple organizations may have the same address. In that case, they will be considered different location records. If de-dupe is necessary, that will have to be done in a post process.
                                            sql = sqlInsert(cur, "Location", locations['mail'])
                                            totalLocItem = totalLocItem + 1
                                            
                                            # set the key for the location for the organiztion record
                                            row['mail_location'] = locations['mail']['id']

                                        # Currently only assigning PractitionerRole by physical location
                                        if('physical' in locations):
                                            
                                            locationInfo = OLD_findLocation(cur, locations['physical'])
                                            if((locationInfo != None) and (len(locationInfo) > 0)):
                                                #Location found
                                                # TODO Match with all found locations!!!!!!!!!
                                                row['physical_location'] = locationInfo['id']
                                                if('org_id' in locationInfo):
                                                    #create practitioner Role
                                                    #print("Found organization with ID:", locationInfo['org_id'])
                                                    row['organization_id'] = locationInfo['org_id']
                                                    bExit = True
                                            else:

                                                #if(('mail' in locations) and (locations['mail'] == locations['physical'])):
                                                # If mail and physical location are the same
                                                if(('mail' in locations) and (equal_dicts(locations['mail'], locations['physical'], 'id') == False)):
                                                    # set the key for the location for the organization record
                                                    row['physical_location'] = locations['mail']['id']
                                                else:
                                                    sql = sqlInsert(cur, "Location", locations['physical'])
                                                    totalLocItem = totalLocItem + 1
                                                    # set the key for the location for the organization record
                                                    row['physical_location'] = locations['physical']['id']
                                        
                                        practitioner = loadPractitioner(row)
                                        #print(str(practitioner))
                                    
                                    
                                        if(len(practitioner) > 0):
                                            sql = sqlInsert(cur, "Practitioner", practitioner)
                                            totalPractItem = totalPractItem + 1
                                        # capture the Practitioner ID to store in PractionerRole
                                        row['practitioner_id'] = practitioner['id']

                                        practitionerRole = loadPractitionerRole(row)
                                        if(len(practitionerRole) > 0):
                                            sql = sqlInsert(cur, "PractitionerRole", practitionerRole)
                                            totalPractRoleItem = totalPractRoleItem + 1
                                        #if(bExit):
                                        #    print(practitionerRole)
                                        #    conn.commit()
                                        #    conn.close()
                                        #    sys.exit()
                                    '''

                        else:
                            print(row)

                        #Write to database
                        if(currentPractItem % commit_size == 0):
                            print("Processed", currentPractItem, "Practitioner records.")
                            conn.commit()

            end = time.time()
            print("Loaded a total of", totalPractItem, "Practitioner records.")
            print("Loaded a total of", totalPractRoleItem, "PractitionerRole records.")
            print("Loaded a total of", totalPractOrgRoleItem, "PractitionerRole with Organization records.")
            totalPractOrgRoleItem
            print("Execution time (in minutes)", (end - start)/60)
    ### TODO Load Other names
    ### TODO Load Other locations

    '''
    if(processLocations):
        files = glob.glob(os.path.join(dir_path, "pl_*.csv"))
        if(len(files) > 1):
            print("Only loading the first PL file", files[0])
        if(len(files) > 0):
            csv = files[0]
            print("Loading Location Data from", csv)
            df = read_csv(csv, chunksize=page_size
                        #header=None, 
                        #skiprows=[0, 10], #current_page * page_size,
                        #skiprows=range(1, current_page * page_size),
                        #nrows=page_size
                        )
            
            #csv = csv.replace("./", "")
            #csv = csv.replace(".\\", "")
            #if(csv.startswith('npidata_')):
            #practitioners = []
            #organizations = []
            # Load Organizations in to DB
            for data in df:
                # loop through each row in the page
                for index, row in data.iterrows():
                    
                    if(notnull(row['NPI'])):
                        currentLocItem = currentLocItem + 1

                        if(currentLocItem > locationStart):
                            # Always process if not filtering on states, or if filtering on states, check if there is a physical location and if so, whether it is to be included.
                            if((state_filter == None) or (str(getValue(row, 'Provider Secondary Practice Location Address - State Name')) in state_filter)):
                                location = loadSecondaryLocation(row)

                                

                                # TODO, See if location already exists
                                # TODO Search for provider and create PR record
                                # TODO See if there are any orgs that match NPI and log.
                                
                                sql = sqlInsert(cur, "Location", location)
                                totalLocItem = totalLocItem + 1
                                    
                                    # set the key for the location for the organiztion record
                                    row['mail_location'] = locations['mail']['id']
                                if('physical' in locations):
                                    #if(('mail' in locations) and (locations['mail'] == locations['physical'])):
                                    if(('mail' in locations) and (equal_dicts(locations['mail'], locations['physical'], 'id') == False)):
                                        # set the key for the location for the organiztion record
                                        row['physical_location'] = locations['mail']['id']
                                    else:
                                        sql = sqlInsert(cur, "Location", locations['physical'])   
                                        totalLocItem = totalLocItem + 1
                                        # set the key for the location for the organiztion record
                                        row['physical_location'] = locations['physical']['id']
                                
                                organization = loadOrganization(row)
                                if(len(organization) > 0):
                                    sql = sqlInsert(cur, "Organization", organization)
                                    totalOrgItem = totalOrgItem + 1

                        #else:
                        #    print(row)

                        #Write to database
                        if(currentOrgItem % commit_size == 0):
                            print("Processed", currentOrgItem, "Organization records.")
                            conn.commit()
            conn.commit()
            print("Loaded a total of", totalOrgItem, "Organization records.")
            


            end = time.time()
            print("Execution time (in minutes)", (end - start)/60)
    '''



    ### Load Endpoint
    if(processEndpoints):
        files = glob.glob(os.path.join(dir_path, "endpoint_*.csv"))
        if(len(files) > 1):
            print("Only loading the first Endpoint file", files[0])
        if(len(files) > 0):
            csv = files[0]
            
            print("Loading Endpoint Data from", csv)
            df = read_csv(csv, chunksize=page_size)
            
            
            # Load Organizations in to DB
            for data in df:
                # loop through each row in the page
                for index, row in data.iterrows():

                    if(notnull(row['NPI'])):
                        currentEPItem = currentEPItem + 1

                        # TODO only include those where an NPI is in the DB!!!!!!!!!
                        
                        if(currentEPItem > endpointStart):
                            endpoint = loadEndpoint(row)

                            # TODO Load Practitioner or Organization ID from NPI when doing the extract and conversion to FHIR
                            # multiple organizations may have the same address. In that case, they will be considered different location records. If de-dupe is necessary, that will have to be done in a post process.
                            if(len(endpoint) > 0):
                                current_npi = None
                                #Check to see if there is a state filter, and if there is check to make sure the NPI is in the database in order to add
                                npi_found = False
                                if(state_filter == None):
                                    npi_found = True
                                else:
                                    read_cur.execute('SELECT npi FROM Organization WHERE npi = ?', (endpoint['npi'],))
                                    npi_row = read_cur.fetchone()
                                    if(npi_row != None):
                                        current_npi = npi_row[0]
                                        npi_found = True
                                    else:
                                        read_cur.execute('SELECT npi FROM Practitioner WHERE npi = ?', (endpoint['npi'],))
                                        npi_row = read_cur.fetchone()
                                        if(npi_row != None):
                                            npi_found = True
                                            current_npi = npi_row[0]


                                if(npi_found):

                                    locations = loadLocations(row, secondary_locations)
                                    if('physical' in locations):
                                        existing_locations = findLocations(read_cur, locations['physical'], current_npi)
                                        for existing_location in existing_locations:

                                            # Add Endpoint location affiliation
                                            entity_location = {}
                                            entity_location['id'] = str(uuid.uuid4())
                                            entity_location['location_id'] = existing_location
                                                
                                            entity_location['entity_id'] = endpoint['id']
                                            entity_location['entity_type'] = ENDPOINT_ENTITY_TYPE

                                            entity_location['location_type'] = LOCATION_TYPE['physical']
                                            sqlInsert(write_cur, "Entity_Location", entity_location)

                                    # Add endpoint
                                    sqlInsert(write_cur, "Endpoint", endpoint)
                                    totalEPItem = totalEPItem + 1

                    else:
                        print(row)

                    #Write to database
                    if(currentEPItem % commit_size == 0):
                        print("Processed", currentEPItem, "Endpoint records.")
                        conn.commit()
            conn.commit()
            print("Loaded a total of", totalEPItem, "Endpoint records.")
            
    end = time.time()
    print("Execution time (in minutes)", (end - start)/60)
    
    
    conn.commit()
    print("Loaded a total of", totalOrgItem, "Organization records.")
    print("Loaded a total of", totalLocItem, "Location records.")
    print("Loaded a total of", totalPractItem, "Practitioner records.")
    print("Loaded a total of", totalPractRoleItem, "PractitionerRole records.")
    print("Loaded a total of", totalPractOrgRoleItem, "PractitionerRole with Organization records.")
    print("Loaded a total of", totalEPItem, "Endpoint records.")
    end = time.time()
    print("Execution time (in minutes)", (end - start)/60)

    conn.close()

# TODO For Practitioner, the location/address is the mail address and mail number. The practice address and number is written to PractitionerRole
def initializeDB(table_cur):

    initOrg = processOrganizations
    initLoc = processLocations
    initPract = processPractitioners
    initPr = processPractitionerRoles
    initEp = processEndpoints

    initTempLog = processTEMPLocations
    #initTempLog = True

    #initOrg = True
    #initLoc = True
    #initPract = True
    #initPr = True
    #initEp = True
  

    # Create temporary locations table in order to support filtering by state 
    #   (pl secondary file may contain a practitioner location in a state to be included where the npi practitioner file may contain states not included. In that case, the practitioner still needs to be included. 
    #   without having the pl secondary locations available, the practitioner won't end up in the DB, even though they have a practicing location that is to be included in the filter)
    if(initTempLog):
        table_cur.execute("DROP TABLE IF EXISTS TEMP_Location")
        # Creating Location table
        table = """ CREATE TABLE TEMP_Location (
                    id CHAR(36) NOT NULL PRIMARY KEY,
                    npi CHAR(20) NOT NULL,
                    first_line VARCHAR(255),
                    second_line VARCHAR(255),
                    city VARCHAR(100),
                    state VARCHAR(100),
                    postal VARCHAR(20),
                    country VARCHAR(10),
                    longitude VARCHAR(20),
                    latitude VARCHAR(20),
                    phone CHAR(25),
                    fax CHAR(25)
                ); """
        table_cur.execute(table)
        index = """CREATE INDEX idx_temp_location_address
                    ON TEMP_Location (first_line, second_line, city, state, postal, country);"""
        table_cur.execute(index)
        index = """CREATE INDEX idx_temp_location_npi
                    ON TEMP_Location (npi);"""
        table_cur.execute(index)
        
        
    
    if(initLoc):
        table_cur.execute("DROP TABLE IF EXISTS Location")
        # Creating Location table
        table = """ CREATE TABLE Location (
                    id CHAR(36) NOT NULL PRIMARY KEY,
                    first_line VARCHAR(255),
                    second_line VARCHAR(255),
                    city VARCHAR(100),
                    state VARCHAR(100),
                    postal VARCHAR(20),
                    country VARCHAR(10),
                    longitude VARCHAR(20),
                    latitude VARCHAR(20),
                    phone CHAR(25),
                    fax CHAR(25)
                ); """
        table_cur.execute(table)
        index = """CREATE INDEX idx_location_address
                    ON Location (first_line, second_line, city, state, postal, country);"""
        table_cur.execute(index)
        index = """CREATE INDEX idx_location_city
                    ON Location (city);"""
        table_cur.execute(index)
        index = """CREATE INDEX idx_location_postal
                    ON Location (postal);"""
        table_cur.execute(index)
        index = """CREATE INDEX idx_location_coordinate
                    ON Location (longitude, latitude);"""
        table_cur.execute(index)

        table_cur.execute("DROP TABLE IF EXISTS Entity_Location")
        # Creating Location entity (Provider, Organization, & Endpoint) Linking table table
        table = """ CREATE TABLE Entity_Location (
                    id CHAR(36) NOT NULL PRIMARY KEY,
                    entity_id CHAR(36) NOT NULL,
                    entity_type CHAR(1) NOT NULL,
                    location_id CHAR(36) NOT NULL,
                    location_type CHAR(1) NOT NULL
                ); """
        table_cur.execute(table)
        index = """CREATE INDEX idx_entity_type_location
                    ON Entity_Location (entity_type, entity_id, location_id, location_type);"""
        table_cur.execute(index)
        index = """CREATE INDEX idx_entity_location_entity
                    ON Entity_Location (location_id, entity_type, entity_id);"""
        table_cur.execute(index)

    # Creating Organization table
    # TODO Organization heirarchies (sub parts(OrganizationAffiliation), parent, etc.)
    # Authorized official name/info, taxonomies (incl. group), identifiers, 
    if(initOrg):
        table_cur.execute("DROP TABLE IF EXISTS Organization")
        table = """ CREATE TABLE Organization (
                    id CHAR(36) NOT NULL PRIMARY KEY,
                    npi CHAR(20) NOT NULL,
                    ein VARCHAR(20),
                    name VARCHAR(255),
                    alias VARCHAR(10),
                    alias_type VARCHAR(10),
                    mail_phone CHAR(25),
                    mail_fax CHAR(25),
                    office_phone CHAR(25),
                    office_fax CHAR(25)
                ); """
        table_cur.execute(table)
        index = """CREATE INDEX idx_organization_npi
                    ON Organization (npi);"""
        table_cur.execute(index)
        index = """CREATE INDEX idx_organization_name
                    ON Organization (name);"""
        table_cur.execute(index)

        # DELETE any entity-location records
        table_cur.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='Entity_Location' ''')
        if(table_cur.fetchone()[0]==1):
            delete = """DELETE FROM Entity_Location WHERE entity_type = '2'""" # Delete location entity cross reference for all organizations
            table_cur.execute(delete)

    # Creating Practitioner table
    # TODO Provider Taxonomies, license number, other identifiers etc. (in separate tables?)
    if(initPract):
        table_cur.execute("DROP TABLE IF EXISTS Practitioner")
        table = """ CREATE TABLE Practitioner (
                    id CHAR(36) NOT NULL PRIMARY KEY,
                    npi CHAR(20) NOT NULL,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    middle_name VARCHAR(50),
                    prefix_name VARCHAR(50),
                    suffix_name VARCHAR(50),
                    name_display VARCHAR(255),
                    name_use VARCHAR(10),
                    alias_first_name VARCHAR(50),
                    alias_last_name VARCHAR(50),
                    alias_middle_name VARCHAR(50),
                    alias_prefix_name VARCHAR(50),
                    alias_suffix_name VARCHAR(50),
                    alias_name_display VARCHAR(255),
                    alias_name_use VARCHAR(10),
                    gender VARCHAR(10),
                    phone CHAR(25),
                    fax CHAR(25),
                    sole_proprietor INT,
                    license_1_number VARCHAR(20),
                    license_1_state VARCHAR(10),
                    license_2_number VARCHAR(20),
                    license_2_state VARCHAR(10),
                    license_3_number VARCHAR(20),
                    license_3_state VARCHAR(10),
                    license_4_number VARCHAR(20),
                    license_4_state VARCHAR(10),
                    license_5_number VARCHAR(20),
                    license_5_state VARCHAR(10)
                ); """
        table_cur.execute(table)
        index = """CREATE INDEX idx_practitioner_npi
                    ON Practitioner (npi);"""
        table_cur.execute(index)
        index = """CREATE INDEX idx_practitioner_name
                    ON Practitioner (last_name, first_name, middle_name);"""
        table_cur.execute(index)
        # DELETE any entity-location records
        table_cur.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='Entity_Location' ''')
        if(table_cur.fetchone()[0]==1):
            delete = """DELETE FROM Entity_Location WHERE entity_type = '1'""" # Delete location entity cross reference for all organizations
            table_cur.execute(delete)

    if(initPr):
        table_cur.execute("DROP TABLE IF EXISTS PractitionerRole")
        table = """ CREATE TABLE PractitionerRole (
                    id CHAR(36) NOT NULL PRIMARY KEY,
                    practitioner_id CHAR(36) NOT NULL,
                    organization_id VARCHAR(36),
                    location_id VARCHAR(36),
                    specialty_1 VARCHAR(20),
                    specialty_2 VARCHAR(20),
                    specialty_3 VARCHAR(20),
                    specialty_4 VARCHAR(20),
                    specialty_5 VARCHAR(20),
                    phone CHAR(25),
                    fax CHAR(25),
                    UNIQUE(practitioner_id, organization_id, location_id)
                ); """
        table_cur.execute(table)
        index = """CREATE INDEX idx_practitioner_role_practitioner_id
                    ON PractitionerRole (practitioner_id);"""
        table_cur.execute(index)
        index = """CREATE INDEX idx_practitioner_role_organization_id
                    ON PractitionerRole (organization_id);"""
        table_cur.execute(index)
        index = """CREATE INDEX idx_practitioner_role_unique_id
                    ON PractitionerRole (practitioner_id, organization_id, location_id);"""
        table_cur.execute(index)
        # DELETE any entity-location records
        table_cur.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='Entity_Location' ''')
        if(table_cur.fetchone()[0]==1):
            delete = """DELETE FROM Entity_Location WHERE entity_type = '3'""" # Delete location entity cross reference for all organizations
            table_cur.execute(delete)

    
    if(initEp):
        table_cur.execute("DROP TABLE IF EXISTS Endpoint")
        table = """ CREATE TABLE Endpoint (
                    id CHAR(36) NOT NULL PRIMARY KEY,
                    npi CHAR(20) NOT NULL,
                    name VARCHAR(255),
                    connection_type VARCHAR(20),
                    connection_type_description VARCHAR(255),
                    url VARCHAR(1024)
                ); """
        table_cur.execute(table)
        index = """CREATE INDEX idx_endpoint_npi
                    ON Endpoint (npi);"""
        table_cur.execute(index)

def loadLocations(entity_data, secondary_location_records = None):
    locations = {}
    mail_location = {}
    physical_location = {}
    if(getValue(entity_data, 'Provider First Line Business Mailing Address') != ''):
        mail_location['id'] = str(uuid.uuid4())
        mail_location['first_line'] = getValue(entity_data, 'Provider First Line Business Mailing Address').upper()
        mail_location['second_line'] = getValue(entity_data, 'Provider Second Line Business Mailing Address').upper()
        #text = HumanName(getValue(data, 'Provider Business Mailing Address City Name'))
        #text.capitalize()
        #mail_location['city'] = str(text)
        mail_location['city'] = getValue(entity_data, 'Provider Business Mailing Address City Name').upper()
        mail_location['state'] = getValue(entity_data, 'Provider Business Mailing Address State Name')
        mail_location['postal'] = getValue(entity_data, 'Provider Business Mailing Address Postal Code')
        mail_location['country'] = getValue(entity_data, 'Provider Business Mailing Address Country Code (If outside U.S.)')

        # THis is not perfect seraching as the names are not always a perfect match, particularly because of formatting, but it can add a lot of geolocation extensions
        #print("Searching...", address.city,address.state)
        coordinates = df_cities.loc[(df_cities['city_ascii'] == mail_location['city']) & (df_cities['state_id'] == mail_location['state'])]
        
        if(coordinates.empty == False):
            mail_location['latitude'] = df_cities.at[coordinates.index[0], 'lat']
            mail_location['longitude'] = df_cities.at[coordinates.index[0], 'lng']

        mail_location['phone'] = getValue(entity_data, 'Provider Business Practice Location Address Telephone Number')
        mail_location['fax'] = getValue(entity_data, 'Provider Business Practice Location Address Fax Number')
        locations['mail'] = mail_location

    if(getValue(entity_data, 'Provider First Line Business Practice Location Address') != ''):
        physical_location['id'] = str(uuid.uuid4())
        physical_location['first_line'] = getValue(entity_data, 'Provider First Line Business Practice Location Address').upper()
        physical_location['second_line'] = getValue(entity_data, 'Provider Second Line Business Practice Location Address').upper()
        #text = HumanName(getValue(data, 'Provider Business Practice Location Address City Name'))
        #text.capitalize()
        #physical_location['city'] = str(text)
        physical_location['city'] = getValue(entity_data, 'Provider Business Practice Location Address City Name').upper()
        physical_location['state'] = getValue(entity_data, 'Provider Business Practice Location Address State Name')
        physical_location['postal'] = getValue(entity_data, 'Provider Business Practice Location Address Postal Code')
        physical_location['country'] = getValue(entity_data, 'Provider Business Practice Location Address Country Code (If outside U.S.)')

        # THis is not perfect seraching as the names are not always a perfect match, particularly because of formatting, but it can add a lot of geolocation extensions
        #print("Searching...", address.city,address.state)
        coordinates = df_cities.loc[(df_cities['city_ascii'] == physical_location['city']) & (df_cities['state_id'] == physical_location['state'])]
        
        if(coordinates.empty == False):
            physical_location['latitude'] = df_cities.at[coordinates.index[0], 'lat']
            physical_location['longitude'] = df_cities.at[coordinates.index[0], 'lng']

        physical_location['phone'] = getValue(entity_data, 'Provider Business Mailing Address Telephone Number')
        physical_location['fax'] = getValue(entity_data, 'Provider Business Mailing Address Fax Number')

        locations['physical'] = physical_location

    if(getValue(entity_data, 'Affiliation Address Line One') != ''):
        physical_location['id'] = str(uuid.uuid4())
        physical_location['first_line'] = getValue(entity_data, 'Affiliation Address Line One').upper()
        physical_location['second_line'] = getValue(entity_data, 'Affiliation Address Line Two').upper()
        #text = HumanName(getValue(data, 'Provider Business Practice Location Address City Name'))
        #text.capitalize()
        #physical_location['city'] = str(text)
        physical_location['city'] = getValue(entity_data, 'Affiliation Address City').upper()
        physical_location['state'] = getValue(entity_data, 'Affiliation Address State')
        physical_location['postal'] = getValue(entity_data, 'Affiliation Address Postal Code')
        physical_location['country'] = getValue(entity_data, 'Affiliation Address Country')

        # THis is not perfect seraching as the names are not always a perfect match, particularly because of formatting, but it can add a lot of geolocation extensions
        #print("Searching...", address.city,address.state)
        coordinates = df_cities.loc[(df_cities['city_ascii'] == physical_location['city']) & (df_cities['state_id'] == physical_location['state'])]
        
        if(coordinates.empty == False):
            physical_location['latitude'] = df_cities.at[coordinates.index[0], 'lat']
            physical_location['longitude'] = df_cities.at[coordinates.index[0], 'lng']

        physical_location['phone'] = getValue(entity_data, 'Provider Business Mailing Address Telephone Number')
        physical_location['fax'] = getValue(entity_data, 'Provider Business Mailing Address Fax Number')

        locations['physical'] = physical_location


    if((secondary_location_records != None) and (len(secondary_location_records) > 0)):
        secondary_location_index = 1
        for secondary_location_record in secondary_location_records:
            secondary_location = loadSecondaryLocationFromTempDBRecord(secondary_location_record)
            locations['secondary_' + str(secondary_location_index)] = secondary_location
            secondary_location_index = secondary_location_index + 1
            

    return locations

def loadSecondaryLocation(data):
    location = {}
    if((getValue(data, 'Provider Secondary Practice Location Address- Address Line 1') != '') and (getValue(data, 'NPI') != '')):
        location['id'] = str(uuid.uuid4())
        location['npi'] = getValue(data, 'NPI').upper()
        location['first_line'] = getValue(data, 'Provider Secondary Practice Location Address- Address Line 1').upper()
        location['second_line'] = getValue(data, 'Provider Secondary Practice Location Address-  Address Line 2').upper()
        location['city'] = getValue(data, 'Provider Secondary Practice Location Address - City Name').upper()
        location['state'] = getValue(data, 'Provider Secondary Practice Location Address - State Name').upper()
        location['postal'] = getValue(data, 'Provider Secondary Practice Location Address - Postal Code').upper()
        location['country'] = getValue(data, 'Provider Secondary Practice Location Address - Country Code (If outside U.S.)').upper()

        # THis is not perfect seraching as the names are not always a perfect match, particularly because of formatting, but it can add a lot of geolocation extensions
        #print("Searching...", address.city,address.state)
        coordinates = df_cities.loc[(df_cities['city_ascii'] == location['city']) & (df_cities['state_id'] == location['state'])]
        
        if(coordinates.empty == False):
            location['latitude'] = df_cities.at[coordinates.index[0], 'lat']
            location['longitude'] = df_cities.at[coordinates.index[0], 'lng']

    location['phone'] = getValue(data, 'Provider Secondary Practice Location Address - Telephone Number')
    extension = getValue(data, 'Provider Secondary Practice Location Address - Telephone Extension')
    if(len(extension) > 0):
        location['phone'] = location['phone'] + "x" + extension
    location['fax'] = getValue(data, 'Provider Practice Location Address - Fax Number')
    
    return location

def loadSecondaryLocationFromTempDBRecord(data):
    location = {}
    if((data) and (len(data) == 12)):
        location['id'] = data[0]
        location['first_line'] = data[2]
        location['second_line'] = data[3]
        location['city'] = data[4]
        location['state'] = data[5]
        location['postal'] = data[6]
        location['country'] = data[7]
        location['latitude'] = data[8]
        location['longitude'] = data[9]
        location['phone'] = data[10]
        location['fax'] = data[11]
    
    return location

def loadOrganization(data):
    organization = {}
    if(str(data['NPI']) != ''):
        organization['id'] = str(uuid.uuid4())
        organization['npi'] = str(data['NPI'])

        name = HumanName("")
        name.first = str(getValue(data, 'Provider Organization Name (Legal Business Name)'))
        name.capitalize()
        
        organization['name'] = str(name).replace("Llc", "LLC").replace("llc", "LLC")
        # Location linking moved to a different table
        #organization['mail_location'] = str(getValue(data, 'mail_location'))
        #organization['physical_location'] = str(getValue(data, 'physical_location'))
        
        ### Organization telecom
        #getValue(data, 'Provider Last Name (Legal Name)'
        organization['mail_phone'] = str(getValue(data, 'Provider Business Mailing Address Telephone Number'))
        organization['mail_fax'] = str(getValue(data, 'Provider Business Mailing Address Fax Number'))
        
        organization['office_phone'] = str(getValue(data, 'Provider Business Practice Location Address Telephone Number'))
        organization['office_fax'] = str(getValue(data, 'Provider Business Practice Location Address Fax Number'))
        
    return organization

def loadPractitioner(data):
    practitioner = {}
    if(str(data['NPI']) != ''):
        practitioner['id'] = str(uuid.uuid4())
        practitioner['npi'] = str(data['NPI'])

        name = HumanName(data['Provider Last Name (Legal Name)'])
        name.capitalize()
        
        ### Practitioner Names
        otherNameType = ''
        if(getValue(data, 'Provider Other Last Name Type Code') == 1):
            otherNameType = 'old'
        legalName = getHumanName(getValue(data, 'Provider Last Name (Legal Name)'),
                                    getValue(data, 'Provider First Name'),
                                    getValue(data, 'Provider Middle Name'),
                                    getValue(data, 'Provider Name Prefix Text'),
                                    getValue(data, 'Provider Name Suffix Text'), 
                                    getValue(data, 'Provider Credential Text'),
                                    'official')
        otherName = getHumanName(getValue(data, 'Provider Other Last Name'),
                                    getValue(data, 'Provider Other First Name'),
                                    getValue(data, 'Provider Other Middle Name'),
                                    getValue(data, 'Provider Other Name Prefix Text'),
                                    getValue(data, 'Provider Other Name Suffix Text'), 
                                    getValue(data, 'Provider Other Credential Text'),
                                    otherNameType)
        

        #practitioner.update(legalName)
        practitioner = practitioner | legalName
        if(otherName != None):
            otherName = {'alias_' + str(key): val for key, val in otherName.items()}
            practitioner.update(otherName)

        sole = str(getValue(data, 'Is Sole Proprietor'))

        if(sole != 'Y'):
            practitioner['sole_proprietor'] = 1
        elif(sole != 'N'):
            practitioner['sole_proprietor'] = 0

        
        #practitioner['location'] = str(getValue(data, 'mail_location'))
        #practitioner['physical_location'] = str(getValue(data, 'physical_location'))
        
        ### Practitioner telecom

        practitioner['phone'] = str(getValue(data, 'Provider Business Mailing Address Telephone Number'))
        practitioner['fax'] = str(getValue(data, 'Provider Business Mailing Address Fax Number'))
        
        #practitioner['office_phone'] = str(getValue(data, 'Provider Business Practice Location Address Telephone Number'))
        #practitioner['office_fax'] = str(getValue(data, 'Provider Business Practice Location Address Fax Number'))
        
        practitioner['license_1_number'] = str(getValue(data, 'Provider License Number_1'))
        practitioner['license_1_state'] = str(getValue(data, 'Provider License Number State Code_1'))
        practitioner['license_2_number'] = str(getValue(data, 'Provider License Number_2'))
        practitioner['license_2_state'] = str(getValue(data, 'Provider License Number State Code_2'))
        practitioner['license_3_number'] = str(getValue(data, 'Provider License Number_3'))
        practitioner['license_3_state'] = str(getValue(data, 'Provider License Number State Code_3'))
        practitioner['license_4_number'] = str(getValue(data, 'Provider License Number_4'))
        practitioner['license_4_state'] = str(getValue(data, 'Provider License Number State Code_4'))
        practitioner['license_5_number'] = str(getValue(data, 'Provider License Number_5'))
        practitioner['license_5_state'] = str(getValue(data, 'Provider License Number State Code_5'))

        return practitioner

def loadPractitionerRole(data):
    practitionerRole = {}
    if(str(data['NPI']) != ''):
        practitionerRole['id'] = str(uuid.uuid4())
        practitionerRole['practitioner_id'] = str(data['practitioner_id'])
        if('organization_id' in data):
            practitionerRole['organization_id'] = str(data['organization_id'])
        

           

        #practitioner['name'] = str(name).replace("Llc", "LLC").replace("llc", "LLC")
        #practitioner['mail_location'] = str(getValue(data, 'mail_location'))
        #practitioner['physical_location'] = str(getValue(data, 'physical_location'))
        
        ### Organization telecom
        #getValue(data, 'Provider Last Name (Legal Name)'
        #practitioner['mail_phone'] = str(getValue(data, 'Provider Business Mailing Address Telephone Number'))
        #practitioner['mail_fax'] = str(getValue(data, 'Provider Business Mailing Address Fax Number'))
        practitionerRole['location_id'] = str(getValue(data, 'pr_location_id'))
        practitionerRole['phone'] = str(getValue(data, 'Provider Business Practice Location Address Telephone Number'))
        practitionerRole['fax'] = str(getValue(data, 'Provider Business Practice Location Address Fax Number'))

        # TODO Get rid of duplicate specialties that show up in the source
        practitionerRole['specialty_1'] = str(getValue(data, 'Healthcare Provider Taxonomy Code_1'))
        practitionerRole['specialty_2'] = str(getValue(data, 'Healthcare Provider Taxonomy Code_2'))
        practitionerRole['specialty_3'] = str(getValue(data, 'Healthcare Provider Taxonomy Code_3'))
        practitionerRole['specialty_4'] = str(getValue(data, 'Healthcare Provider Taxonomy Code_4'))
        practitionerRole['specialty_5'] = str(getValue(data, 'Healthcare Provider Taxonomy Code_5'))

            
        return practitionerRole


def loadEndpoint(data):
    endpoint = {}
    if(str(data['NPI']) != ''):
        endpoint['id'] = str(uuid.uuid4())
        endpoint['npi'] = getValue(data, 'NPI')
        endpoint['name'] = getValue(data, 'Endpoint Description')
        endpoint_type = ENDPOINT_TYPE_DICT[getValue(data, 'Endpoint Type')]
        if(endpoint_type != ''):
            endpoint['connection_type'] = endpoint_type
            endpoint['connection_type_description'] = ENDPOINT_TYPE_DISPLAY_DICT[getValue(data, 'Endpoint Type')]
        endpoint['url'] = str(data['Endpoint'])

        return endpoint

def findPracticingLocations(read_cur, location_id):
    found_locations = []
    found_location = {}
    
    #sql = """SELECT l.id, o.id, o.name FROM Location l INNER JOIN Entity_Location el ON l.id = el.location_id WHERE l.first_line = ? AND l.second_line = ? AND l.city = ? AND l.state = ? AND l.postal = ? AND l.country = ? """
    ####sql = """SELECT entity_type, entity_id FROM Entity_Location WHERE location_id = ? AND location_type = '2' AND entity_type = '2'"""
    
    # Find location with matching location ID and an entity type of organization
    sql = """SELECT DISTINCT entity_type, entity_id FROM Entity_Location WHERE location_id = ? AND location_type = '2' AND entity_type = '2'"""
    #sql = """SELECT id FROM Location WHERE city = "%s" """ % location['city']
    #print(sql)
    read_cur.execute(sql, (location_id,))
    rows = read_cur.fetchall()
    if(len(rows) > 1):
        test = 0
    for row in rows:
        found_location['entity_type'] = row[0]
        found_location['entity_id'] = row[1]
        found_locations.append(found_location)
    #if(row != None):
    #    foundLocation['id'] = row[0]
    #    foundLocation['organization_id'] = row[1]
        #print(foundLocation)

    return found_locations

def findLocation(read_cur, location):
    location_id = None
    sql = """SELECT id FROM Location WHERE first_line = ? AND second_line = ? AND city = ? AND state = ? AND postal = ? AND country = ? """
    #sql = """SELECT id FROM Location WHERE city = "%s" """ % location['city']
    #print(sql)
    read_cur.execute(sql, (location['first_line'], location['second_line'], location['city'],location['state'], location['postal'], location['country'],))
    row = read_cur.fetchone()
    if(row != None):
        location_id = row[0]

    return location_id

def findLocations(read_cur, location, npi = None):
    location_ids = []
    

    sql = """SELECT id FROM Location WHERE first_line = ? AND second_line = ? AND city = ? AND state = ? AND postal = ? AND country = ? """

    #sql = """SELECT id FROM Location WHERE city = "%s" """ % location['city']
    #print(sql)
    read_cur.execute(sql, (location['first_line'], location['second_line'], location['city'],location['state'], location['postal'], location['country'],))
    rows = read_cur.fetchall()
    for row in rows:
        if(npi != None):
            # Filter to only find locations associated with the NPI.
            #"SELECT * FROM Entity_Location el INNER JOIN Practitioner pract ON pract.id = el.entity_id WHERE el.location_id = ? AND pract.npi = ?"
            read_cur.execute('SELECT * FROM Entity_Location el INNER JOIN Practitioner pract ON pract.id = el.entity_id WHERE el.location_id = ? AND pract.npi = ?', (row[0], npi,))
            test = read_cur.fetchone()
            if(test != None):
                location_ids.append(row[0])
            
        else:
            location_ids.append(row[0])

    return location_ids
    #, second_line, city, state, postal, country"""


def getHumanName(last, first, middle, prefix, suffix, credential, use=''):
    name = {}
    if(last != ''):
        displayName = HumanName()
        
        
        lastName = HumanName(last)
        lastName.capitalize()
        name['last_name'] = str(lastName)
        displayName.last = str(lastName)

        firstName = HumanName(first)
        firstName.capitalize()
        name['first_name'] = str(firstName)
        displayName.first = str(firstName)
        
        if(middle != ''):
            middleName = HumanName(middle)
            middleName.capitalize()
            name['middle_name'] = str(firstName)
            displayName.middle = str(middleName)

        
        if(prefix != ''):
            prefixName = HumanName(prefix)
            prefixName.capitalize()
            displayName.title = str(prefixName)
            name['prefix_name'] = str(prefixName)

        suffixName = ""
        if(str(suffix) != ''):
            suffixName = HumanName(str(suffix))
            suffixName.capitalize()
            displayName.suffix = str(suffixName)
            if(displayName.suffix != ''):
                displayName.suffix = displayName.suffix + ", "
            displayName.suffix = displayName.suffix + str(suffixName)
        if(str(credential) != ''):
            if(displayName.suffix != ''):
                displayName.suffix = displayName.suffix + ", "
            displayName.suffix = displayName.suffix + str(credential)

        if(displayName.suffix != ''):
            name['suffix_name'] = str(displayName.suffix)
            
        
        if(use in ['usual', 'official', 'temp', 'nickname', 'anonymous', 'old', 'maiden']):
            name['name_use'] = use

        name['name_display'] = str(displayName)

    return name


def createSQLInsert(table, dict):
    columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in dict.keys())
    values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in dict.values())
    return """INSERT INTO %s ( %s ) VALUES ( %s );""" % (table, columns, values)

def createSQLInsert2(table, dict):
    columns = ', '.join(str(x).replace('/', '_') for x in dict.keys())
    values = ', '.join("%s" for x in dict.values())
    #return """INSERT INTO %s ( %s ) VALUES ( %s )""" % (table, columns, values)
    return 'INSERT INTO Location ( id, first_line, second_line, city, state, postal, country, latitude, longitude ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s )'


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
    #if((title in data) and (data[title])):
    if((str(title) in data) and (str(data[title]).upper() != 'NAN')):
        return str(data[title])
    else:
        return ""
    
def equal_dicts(d1, d2, ignore_keys):
    ignored = set(ignore_keys)
    for k1, v1 in d1.items():
        if k1 not in ignored and (k1 not in d2 or d2[k1] != v1):
            return False
    for k2, v2 in d2.items():
        if k2 not in ignored and k2 not in d1:
            return False
    return True


def path_arg(string):
    if os.path.isdir(string):
        return string
    else:
        raise argparse.ArgumentError(f'D{string}, is not a found directory')

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

main()

'''
Duplicate address query check
SELECT first_line, second_line, city, state, postal, country, COUNT(*)
FROM Location
GROUP BY first_line, second_line, city, state, postal, country
ORDER BY COUNT(*)
'''