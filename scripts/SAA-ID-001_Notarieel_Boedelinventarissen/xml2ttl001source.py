# -*- coding: utf-8 -*-
"""
    This is the script to convert huge xml file to rdf tll format
    I reads record by record of the xml file into a json format and then processes it into a ttl format
    For each <<out_size>> records, the processed outcome is saved in a file called <<output_file_name>>N.ttl where N is a counter

    In order to run it, use the command line bellow replacing <<file_name>>
        cat <<file_name>> | python -m xmltodict 2 | python -u xml2ttl002source.py

    The variable <<skip>> can be settled to allow you to see a counter while the file is running
    It means the counter will be updated for each <<skip>> records

cat "Notarieel_Export_20180321.xml" | python -m xmltodict 2 | python -u xml2ttl001source.py

"""


###########################################################################
# MODULES TO IMPORT
###########################################################################
import sys, marshal, dicttoxml, codecs
from kitchen.text.converters import to_bytes, to_unicode

###########################################################################
# VARIABLE TO SET UP
###########################################################################
output_file_name = 'SAA-ID-001-Notarieel-Boedelinventarissen'
out_size = 20000
skip = 10#00
limit_files = 30

###########################################################################
# INITIATE VARIABLES
###########################################################################
count_registers = 0
count_person = 1
outcome_start = """
###########################################################################
@prefix saaRec: <http://goldenagents.org/uva/SAA/record/@@/> .
@prefix saaPerson: <http://goldenagents.org/uva/SAA/person/@@/> .
@prefix saaLocation: <http://goldenagents.org/uva/SAA/location/@@/> .
@prefix saaOnt: <http://goldenagents.org/uva/SAA/ontology/> .
###########################################################################
"""
outcome = outcome_start
list_no_date = []
list_no_akteType =[]


###########################################################################
# MAIN CODE
# Performing a loop reading each record until is find the end of the file (EOF)
# In this case an exception is fired (see "except Exception as error")
# when the last file is saved with the last parsed records
###########################################################################
while True:
    try:
        ###########################################################################
        # Reading the source file
        ###########################################################################
        path, item = marshal.load(sys.stdin)

        # Use this to skip the first X records
        # if count_registers <= 80000:
        #    count_registers += 1
        #    continue

        # restart the counter for person
        # count_person = 1

        # Extracts the type of index and its id from the variable <<path>>
        # this has been changed into a fixed value -- variable is no longer necessary i guess

        index_type_prefix = to_bytes('Notarieel-Boedelinventarissen')


        if 'akteType' in item:
            index_type = (item['akteType'].replace(' ', '_').replace('(','').replace(')',''))
        else:
            index_type = ('Notarieel-Boedelinventarissen')

        #index_type = (item['akteType'])
        #index_type = ('Notarieel-Boedelinventarissen')

        index_id = to_bytes(str(item['uuid']).strip())
        outcome = outcome.replace('@@', index_type_prefix)


        ###########################################################################
        # Defining some auxiliary functions to support parsing the original data
        ###########################################################################
        def getFamily(s):

            if type(s) in [str, unicode]:
                if "," in s:
                    return s[:s.index(",")] if s.index(",") >= 0 else s
            elif type(s) == list:
                return map(getFamily, s)
            return ''

        getStructure = lambda text, idRec, pNum : {'a': 'Person',
                                             'id': idRec+pNum,
                                             'full_name': text,
                                             'family_name':getFamily(text)}


        getStructureNameParts = lambda full_name, idRec, pNum :{'a': 'Person',
                                             'id': idRec+pNum,
                                             'full_name': full_name}

        getStructureNamePartsPos = lambda full_name, id :{'a': 'Person',
                                             'id': id,
                                             'full_name': full_name}

        structure = {}
        list_names = []
        if 'persoonsnamen' in item:
            if 'persoonsnaam' in item['persoonsnamen']:

                if type(item['persoonsnamen']['persoonsnaam']) == list:
                    # if 'voornaam' in item['persoonsnamen']['persoonsnaam'][0] or 'achternaam' in item['persoonsnamen']['persoonsnaam'][0]:

                    list_name_parts_and_positions = item['persoonsnamen']['persoonsnaam']
                    structure['hasPerson'] = []
                    for name_parts_and_pos_dict in list_name_parts_and_positions:
                        if 'voornaam' in name_parts_and_pos_dict and name_parts_and_pos_dict['voornaam'] and len(name_parts_and_pos_dict['voornaam']) > 0:
                            voornaam = to_bytes(name_parts_and_pos_dict['voornaam']).replace("\\","").replace("\"","\\\"")
                        else:
                            voornaam = '...'
                        if 'achternaam' in name_parts_and_pos_dict and name_parts_and_pos_dict['achternaam'] and len(name_parts_and_pos_dict['achternaam']) > 0:
                            achternaam = to_bytes(name_parts_and_pos_dict['achternaam']).replace("\\","").replace("\"","\\\"")
                        else:
                            achternaam = '...'
                        if 'tussenvoegsel' in name_parts_and_pos_dict:
                            tussenvoegsel = to_bytes(name_parts_and_pos_dict['tussenvoegsel']).replace("\\","").replace("\"","\\\"")
                        else:
                            tussenvoegsel = ''


                        full_name = to_bytes(achternaam + ', ' + voornaam + ' [' + tussenvoegsel + ']') if tussenvoegsel != '' else to_bytes(achternaam + ', ' + voornaam)

                        # if 'scanNaam' in name_parts_and_pos_dict:
                        #     scanNaam = name_parts_and_pos_dict['scanNaam'].replace('.jpg','-')
                        # else:
                        #     scanNaam = index_id+name_parts_and_pos_dict['@id']
                        #
                        # if 'scanPositie' in name_parts_and_pos_dict:
                        #     scanPositie = name_parts_and_pos_dict['scanPositie'].replace(', ','-')
                        # else:
                        #     scanPositie = ''
                        #
                        # id = scanNaam + scanPositie

                        if 'uuidNaam' in name_parts_and_pos_dict:
                            # print 'uuid', name_parts_and_pos_dict['uuidNaam']
                            uuidNaam = name_parts_and_pos_dict['uuidNaam']
                        else:
                            # print 'no uuid', index_id+name_parts_and_pos_dict['@id']
                            # print name_parts_and_pos_dict
                            uuidNaam = index_id+name_parts_and_pos_dict['@id']

                        id = uuidNaam

                        temp = getStructureNamePartsPos(full_name, id)

                        if voornaam != '...':
                            temp['first_name'] = voornaam
                        if achternaam != '...':
                            temp['family_name'] = achternaam
                        if tussenvoegsel != '':
                            temp['infix'] = tussenvoegsel

                        ## To be improved: position in scan is not a property of a Person
                        if 'scanNaam' in name_parts_and_pos_dict:
                            temp['scanName'] = name_parts_and_pos_dict['scanNaam']
                        if 'scanPositie' in name_parts_and_pos_dict:
                            temp['scanPosition'] = name_parts_and_pos_dict['scanPositie']

                        temp['isInRecord'] = 'saaRec:' + index_id
                        structure['hasPerson'] += [temp]


                    # else:
                    #     list_names = item['persoonsnamen']['persoonsnaam']
                    #     structure['hasPerson'] = []
                    #     for person in list_names:
                    #         temp = getStructure(person['#text'], index_id, person['@id'])
                    #         temp['isInRecord'] = 'saaRec:' + index_id
                    #         structure['hasPerson'] += [temp]

                else:
                    # if 'voornaam' in item['persoonsnamen']['persoonsnaam']:
                    person = item['persoonsnamen']['persoonsnaam']
                    if 'voornaam' in person and person['voornaam'] and len(person['voornaam']) > 0 :
                        voornaam = to_bytes(person['voornaam']).replace("\\","").replace("\"","\\\"")
                    else:
                        voornaam = '...'
                    if 'achternaam' in person and person['achternaam'] and len(person['achternaam']) > 0:
                        achternaam = to_bytes(person['achternaam']).replace("\\","").replace("\"","\\\"")
                    else:
                        achternaam = '...'
                    if 'tussenvoegsel' in person:
                        tussenvoegsel = to_bytes(person['tussenvoegsel']).replace("\\","").replace("\"","\\\"")
                    else:
                        tussenvoegsel = ''

                    full_name = to_bytes(achternaam + ', ' + voornaam + ' [' + tussenvoegsel + ']') if tussenvoegsel != '' else to_bytes(achternaam + ', ' + voornaam)

                    # if 'scanNaam' in person:
                    #     scanNaam = person['scanNaam'].replace('.jpg', '-')
                    # else:
                    #     scanNaam = index_id + person['@id']
                    #
                    # if 'scanPositie' in person:
                    #     scanPositie = person['scanPositie'].replace(', ', '-')
                    # else:
                    #     scanPositie = ''
                    #
                    # id = scanNaam + scanPositie

                    if 'uuidNaam' in person:
                        uuidNaam = person['uuidNaam']
                    else:
                        uuidNaam = index_id + person['@id']

                    id = uuidNaam

                    temp = getStructureNamePartsPos(full_name, id)
                    # temp = getStructureNameParts(full_name, index_id, person['@id'])

                    if voornaam != '...':
                        temp['first_name'] = voornaam
                    if achternaam != '...':
                        temp['family_name'] = achternaam
                    if tussenvoegsel != '':
                        temp['infix'] = tussenvoegsel
                    ## To be improved: position in scan is not a property of a Person
                    if 'scanNaam' in person:
                        temp['scanName'] = person['scanNaam']
                    if 'scanPositie' in person:
                        temp['scanPosition'] = person['scanPositie']

                    structure['hasPerson'] = temp
                    structure['hasPerson']['isInRecord'] = 'saaRec:' + index_id

                    # else:
                    #     structure['hasPerson'] = getStructure(person['#text'], index_id, person['@id'])
                    #     structure['hasPerson']['isInRecord'] = 'saaRec:' + index_id


        getStructureLoc = lambda text, id, n: {'a': 'Location',
                                             'id': id + 'l' + str(n),
                                             'location': text}

        structureLoc = {}
        if 'locaties' in item:
            if 'locatie' in item['locaties']:
                if type(item['locaties']['locatie']) == list:
                    for location in item['locaties']['locatie']:
                        locationName = location['#text'].replace("\\","").replace("\"","\\\"")
                        structureLoc['location'] = getStructureLoc(locationName, index_id, location['@id'])
                        structureLoc['location']['isInRecord'] = 'saaRec:' + index_id
                else:
                    locationName = item['locaties']['locatie']['#text'].replace("\\","").replace("\"","\\\"")
                    structureLoc['location'] = getStructureLoc(locationName, index_id, item['locaties']['locatie']['@id'])
                    structureLoc['location']['isInRecord'] = 'saaRec:' + index_id
                    # count_location += 1

        #
        # ###########################################################################
        # # Formating a ttl output string to be write in the file
        # # The <<outcome>> string will be composed of two parts
        # # 1. A resource record itself, which will refer to other resouces deacribing Persons
        # #   This part is writen directly in the variable <<outcome>>
        # # 2. Several reources about Persons complied into the varialbe <<inner>>
        # ###########################################################################

        outcome += " saaRec:{}\t\t a \t\t\t saaOnt:{} ;\n".format(index_id, index_type)
        if 'datering' in item:
            outcome += " \t\tsaaOnt:registration_date \t \"{}\"^^xsd:date;\n".format(item[u'datering'])
        else:
            list_no_date += [item]
        if 'notaris' in item:
            outcome += " \t\tsaaOnt:notary \t \"\"\"{}\"\"\";\n".format(to_bytes(item[u'notaris']))
        if 'inventarisNr' in item:
            outcome += " \t\tsaaOnt:inventaryNumber \t \"{}\";\n".format(to_bytes(item[u'inventarisNr']))
        if 'akteNr' in item:
            outcome += " \t\tsaaOnt:actNumber \t \"{}\";\n".format(to_bytes(item[u'akteNr']))
        if 'akteType' in item:
            outcome += " \t\tsaaOnt:actType \t \"\"\"{}\"\"\";\n".format(to_bytes(item[u'akteType']))
        else:
            list_no_akteType += [item]

        if 'taal' in item:
            outcome += " \t\tsaaOnt:language \t \"{}\";\n".format(to_bytes(item[u'taal']))
        if 'beschrijving' in item:
            outcome += " \t\tsaaOnt:description \t \"\"\"{}.\"\"\";\n".format(to_bytes(item[u'beschrijving']))

        if 'urlScans' in item:
            if 'urlScan' in item['urlScans']:
                if type(item['urlScans']['urlScan']) == list:
                    for url in item['urlScans']['urlScan']:
                        outcome += " \t\tsaaOnt:urlScan \t <{}>;\n".format(url['#text'])
                else:
                    outcome += " \t\tsaaOnt:urlScan \t <{}>;\n".format(item['urlScans']['urlScan']['#text'])


        inner = ''

        for master_key in structure:
            listS = []
            if type(structure[master_key]) == dict:
                listS += [structure[master_key]]
            elif type(structure[master_key]) == list:
                listS = structure[master_key]
            for elem in listS:


                outcome +=  "\t\tsaaOnt:{} \t\t saaPerson:{} ;\n".format(master_key, elem['id'])

                inner += " saaPerson:{} \t a \t saaOnt:{} ;\n".format(elem['id'], elem['a'])
                for key in elem:
                    if key not in ['a','id','isInRecord'] and elem[key]:
                        text = to_bytes(elem[key])
                        inner += "\t\tsaaOnt:{}  \t\t \"\"\"{}\"\"\" ;\n".format(key, text)
                    elif key == 'isInRecord':
                        inner += "\t\tsaaOnt:{}  \t\t {} ;\n".format(key, elem[key])
                inner = inner[:-2] + '.\n\n'


        for master_key in structureLoc:
            outcome += "\t\tsaaOnt:{} \t saaLocation:{} ;\n".format(master_key, structureLoc[master_key]['id'])

            inner += " saaLocation:{} \t a \t saaOnt:{} ;\n".format(structureLoc[master_key]['id'], 'Location')
            for key in structureLoc[master_key]:
                if key not in ['a', 'id', 'isInRecord'] and structureLoc[master_key][key]:
                    if type(structureLoc[master_key][key]) == list:
                        for text in structureLoc[master_key][key]:
                                inner += "\t\tsaaOnt:{}  \t\t \"\"\"{}\"\"\" ;\n".format(key, to_bytes(text))
                    else:
                        text = to_bytes(structureLoc[master_key][key])
                        inner += "\t\tsaaOnt:{}  \t\t \"\"\"{}\"\"\" ;\n".format(key, to_bytes(text))
                elif key == 'isInRecord':
                    inner += "\t\tsaaOnt:{}  \t\t {} ;\n".format(key, structureLoc[master_key][key])

            inner = inner[:-2] + '.\n\n'


        outcome = outcome[:-2] + '.\n\n' + inner


        count_registers += 1
        if count_registers % out_size == 0:
            # print path
            n_file = count_registers/out_size
            if n_file == limit_files:
                # print "Limit of files is reached"
                # exit(0)
                raise Exception('Limit of files is reached', 'Limit of files is reached')
            filename = output_file_name + 'N' + str(count_registers/out_size) + '.ttl'
            with codecs.open(filename, 'wb', encoding='utf-8') as outfile:
                outfile.write(to_unicode(outcome))
                outfile.close()
                print "file ", filename
            outcome = outcome_start

    except Exception as error:
        print '\n\n'


        print 'Final List no date', list_no_date
        print 'Final List no akteType', list_no_akteType


        filename = output_file_name + 'N' + str((count_registers-1)//out_size+1) + '.ttl'
        with codecs.open(filename, 'wb', encoding='utf-8') as outfile:
            outfile.write(to_unicode(outcome))
            outfile.close()
            print "file ", filename
        outcome = outcome_start

        raise

    if count_registers % skip == 0:
        print >> sys.stderr, count_registers, '\r',
