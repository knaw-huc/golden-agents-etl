# -*- coding: utf-8 -*-
"""
    This is the script to convert huge xml file to rdf tll format
    I reads record by record of the xml file into a json format and then processes it into a ttl format
    For each <<out_size>> records, the processed outcome is saved in a file called <<output_file_name>>N.ttl where N is a counter

    In order to run it, use the command line bellow replacing <<file_name>>
        cat <<file_name>> | python -m xmltodict 2 | python -u xml2ttl003source.py

    The variable <<skip>> can be settled to allow you to see a counter while the file is running
    It means the counter will be updated for each <<skip>> records

    cat "SAA-ID-004-SAA_Index_op_kwijtscheldingen.xml" | python -m xmltodict 2 | python -u xml2ttl004source.py
"""

###########################################################################
# MODULES TO IMPORT
###########################################################################
import sys, marshal, dicttoxml, codecs
from findertools import location

from kitchen.text.converters import to_bytes, to_unicode

###########################################################################
# VARIABLE TO SET UP
###########################################################################
output_file_name = 'SAA-ID-004-SAA_Index_op_kwijtscheldingen'
out_size = 20000
skip = 1000
limit_files = 10

###########################################################################
# INITIATE VARIABLES
###########################################################################
count_registers = 0
# count_person = 1
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

        # if count_registers <= x000:
        #     count_registers += 1
        #     continue

        # restart the counter for person and location
        count_person = 1
        count_location = 1

        index_type = str(path[0][1][u'name']).replace("SAA","").title().replace(" ","").strip()
        index_id = str(path[1][1][u'id']).strip()
        outcome = outcome.replace('@@', index_type)

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

        getStructure1 = lambda text, id, n : {'a': 'saaOnt:Person',
                                             'id': id+'p'+str(n),
                                             'full_name': text,
                                             'family_name':getFamily(text)}

        getStructure2 = lambda text, id, n: {'a': 'saaOnt:Location',
                                             'id': id + 'l' + str(n),
                                             'street_name': text,
                                             'street_name_source': item['Straatnaam_in_bron']}


        #####################################################################################
        # Assembling an auxiliary dictionary (structure) for the persons described in the record
        #####################################################################################
        structure1 = {}

        if 'Verkoper' in item:
            if type(item['Verkoper']) == list:
                structure1['Seller'] = []
                for text in item['Verkoper']:
                    temp = getStructure1(text, index_id, count_person)
                    temp['isInRecord'] = 'saaRec:'+index_id
                    structure1['Seller'] += [temp]
                    count_person += 1
            else:
                structure1['Seller'] = getStructure1(item['Verkoper'], index_id, count_person)
                structure1['Seller']['isInRecord'] = 'saaRec:'+index_id
                count_person += 1

        if 'Koper' in item:
            if type(item['Koper']) == list:
                structure1['Buyer'] = []
                for text in item['Koper']:
                    temp = getStructure1(text, index_id, count_person)
                    temp['isInRecord'] = 'saaRec:'+index_id
                    structure1['Buyer'] += [temp]
                    count_person += 1
            else:
                structure1['Buyer'] = getStructure1(item['Koper'], index_id, count_person)
                structure1['Buyer']['isInRecord'] = 'saaRec:'+index_id
                count_person += 1

        structure2 = {}
        if 'Straatnaam' in item:
            structure2['Street'] = getStructure2(item['Straatnaam'], index_id, count_location)
            structure2['Street']['isInRecord'] = 'saaRec:' + index_id
            #count_location += 1


        ###########################################################################
        # Formating a ttl output string to be write in the file
        # The <<outcome>> string will be composed of two parts
        # 1. A resource record itself, which will refer to other resouces deacribing Persons
        #   This part is writen directly in the variable <<outcome>>
        # 2. Several reources about Persons complied into the varialbe <<inner>>
        ###########################################################################

        outcome += " saaRec:{}\t\t a \t\t\t saaOnt:{} ;\n".format(index_id, index_type)
        if 'Datum_overdracht' in item:
            outcome += " \t\tsaaOnt:date_transaction \t \"{}\"^^xsd:date;\n".format(item[u'Datum_overdracht'])
        else:
            list_no_date += [item]

        if 'Omschrijving' in item:
                outcome += " \t\tsaaOnt:description \t \"{}\";\n".format(to_bytes(item['Omschrijving']))

        if 'urlScan' in item:
            if type(item['urlScan']) == list:
                for url in item['urlScan']:
                    outcome += " \t\tsaaOnt:urlScan \t <{}>;\n".format(url)
            else:
                outcome += " \t\tsaaOnt:urlScan \t <{}>;\n".format(item['urlScan'])

        inner = ''
        for master_key in structure1:
            listS = []
            if type(structure1[master_key]) == dict:
                listS += [structure1[master_key]]
            else:
                listS = structure1[master_key]
            for elem in listS:
                outcome += "\t\tsaaOnt:{} \t\t saaPerson:{} ;\n".format(master_key, elem['id'])

                inner += " saaPerson:{} \t a \t saaOnt:{} ;\n".format(elem['id'], 'Person')
                for key in elem:
                    if key not in ['a', 'id', 'isInRecord'] and elem[key]:
                        text = to_bytes(elem[key])
                        inner += "\t\tsaaOnt:{}  \t\t \"{}\" ;\n".format(key, text)
                    elif key == 'isInRecord':
                        inner += "\t\tsaaOnt:{}  \t\t {} ;\n".format(key, elem[key])
                inner = inner[:-2] + '.\n\n'

        for master_key in structure2:
            outcome += "\t\tsaaOnt:{} \t saaLocation:{} ;\n".format(master_key, structure2[master_key]['id'])

            inner += " saaLocation:{} \t a \t saaOnt:{} ;\n".format(structure2[master_key]['id'], 'Location')
            for key in structure2[master_key]:
                if key not in ['a', 'id', 'isInRecord'] and structure2[master_key][key]:
                    if type(structure2[master_key][key]) == list:
                        for text in structure2[master_key][key]:
                                inner += "\t\tsaaOnt:{}  \t\t \"{}\" ;\n".format(key, to_bytes(text))
                    else:
                            text = to_bytes(structure2[master_key][key])
                            inner += "\t\tsaaOnt:{}  \t\t \"{}\" ;\n".format(key, to_bytes(text))
                elif key == 'isInRecord':
                    inner += "\t\tsaaOnt:{}  \t\t {} ;\n".format(key, structure2[master_key][key])

            inner = inner[:-2] + '.\n\n'

        outcome = outcome[:-2] + '.\n\n' + inner

        count_registers += 1

        ###########################################################################
        # Writing the output file when <<out_size>> records is reached
        ###########################################################################
        if count_registers % out_size == 0:
            n_file = count_registers/out_size
            filename = output_file_name + 'N' + str(n_file) + '.ttl'
            with codecs.open(filename, 'wb', encoding='utf-8') as outfile:
                outfile.write(to_unicode(outcome))
                outfile.close()
                print "file ", filename

            outcome = outcome_start
            if n_file == limit_files:
                print "Limit of files is reached"
                exit(0)

    ###########################################################################
    # This piece of code only runs when an error occurs

    except Exception as error:

        filename = output_file_name + 'N' + str((count_registers-1)//out_size+1) + '.ttl'
        with codecs.open(filename, 'wb', encoding='utf-8') as outfile:
            outfile.write(to_unicode(outcome))
            outfile.close()
        print "file ", filename
        print 'Final List no date', list_no_date
        print '\n================ DONE =================='
        raise

    if count_registers % skip == 0:
        print >> sys.stderr, count_registers, '\r',
