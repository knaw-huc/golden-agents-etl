# -*- coding: utf-8 -*-
"""
    This is the script to convert huge xml file to rdf tll format
    I reads record by record of the xml file into a json format and then processes it into a ttl format
    For each <<out_size>> records, the processed outcome is saved in a file called <<output_file_name>>N.ttl where N is a counter

    In order to run it, use the command line bellow replacing <<file_name>>
        cat <<file_name>> | python -m xmltodict 2 | python -u <<file_name.py>>

    The variable <<skip>> can be settled to allow you to see a counter while the file is running
    It means the counter will be updated for each <<skip>> records

    cat "SAA-ID-002-SAA-Index-op-doopregister.xml" | python -m xmltodict 2 | python -u xml2rdf.py

"""

###########################################################################
# MODULES TO IMPORT
###########################################################################
import sys, marshal, dicttoxml, codecs
from kitchen.text.converters import to_bytes, to_unicode

###########################################################################
# VARIABLE TO SET UP
###########################################################################
output_file_name = 'SAA-ID-002-SAA_Index_op_doopregister'
out_size = 20000
skip = 1000
limit_files = 1000

###########################################################################
# INITIATE VARIABLES
###########################################################################
count_registers = 0
# count_person = 1
outcome_start = """
###########################################################################
@prefix saaRec: <http://goldenagents.org/uva/SAA/record/@@/> .
@prefix saaPerson: <http://goldenagents.org/uva/SAA/person/@@/> .
@prefix saaOnt: <http://goldenagents.org/uva/SAA/ontology/> .
###########################################################################

<http://goldenagents.org/datasets/Baptism002> {
"""
outcome = outcome_start
list_no_date = []

def processStructure2RDF(elem, main_key):
    outerpart = "\t\tsaaOnt:{} \t {}:{} ;\n".format(main_key, elem['prefix'], elem['id'])

    innerpart = " {}:{} \t a \t saaOnt:{} ;\n".format(elem['prefix'], elem['id'], elem['a'])
    subInnerPart = ""
    try:
        for key in elem:
            if key not in ['a', 'id', 'prefix', 'isInRecord'] and elem[key]:
                if type(elem[key]) != list:
                    elem[key] = [elem[key]]
                for innerElem in elem[key]:
                    if type(innerElem) is not list:
                        innerElem = [innerElem]
                    if innerElem != [] and type(innerElem[0]) is dict:
                        for ie in innerElem:
                            (subOut, subIn) = processStructure2RDF(ie, key)
                            innerpart += subOut
                            subInnerPart += subIn
                    else:
                        for ie in innerElem:
                            innerpart += "\t\tsaaOnt:{}  \t\t \"{}\" ;\n".format(key, to_bytes(ie))
            elif key == 'isInRecord':
                innerpart += "\t\tsaaOnt:{}  \t\t {} ;\n".format(key, elem[key])
    except:
        print elem

    innerpart = innerpart[:-2] + '.\n\n'
    innerpart += subInnerPart
    return (outerpart, innerpart)

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
        # Reads one record
        path, item = marshal.load(sys.stdin)

        # if count_registers <= x0000:
        #     count_registers += 1
        #     continue

        # restart the counter for person
        count_person = 1

        index_type = str(path[0][1][u'name']).replace("SAA","").title().replace(" ","").strip()

        index_id = str(path[1][1][u'id']).strip()
        outcome = outcome.replace('@@', index_type)


        ###########################################################################
        # Defining some auxiliary functions to support parsing the original data
        ###########################################################################

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

        def getFirst(s):
            if type(s) in [str, unicode]:
                if "," in s:
                    return s[s.index(",")+2:s.index("[")].strip() if "[" in s else s[s.index(",")+2:].strip()
            elif type(s) == list:
                return map(getFirst, s)
            return ''

        def getInfix(s):
            if type(s) in [str, unicode]:
                if "[" in s:
                    return s[s.index("[")+1:s.index("]")] if s.index("[") >= 0 else ''
            elif type(s) == list:
                return map(getInfix, s)
            return ''

        getStructurePerson = lambda text, id, n : {'a': 'Person',
                                             'prefix': 'saaPerson',
                                             'id': id+'p'+str(n),
                                             'full_name': text,
                                             'first_name': getFirst(text),
                                             'infix_name': getInfix(text),
                                             'family_name': getFamily(text)}

        #####################################################################################
        # Assembling an auxiliary dictionary (structure) for the persons described in the record
        #####################################################################################
        structure = {}
        if 'Moeder' in item:
            structure['hasMother'] = getStructurePerson(item['Moeder'], index_id, count_person)
            structure['hasMother']['isInRecord'] = 'saaRec:'+index_id
            count_person += 1

        if 'Vader' in item:
            structure['hasFather'] = getStructurePerson(item['Vader'], index_id, count_person)
            structure['hasFather']['isInRecord'] = 'saaRec:'+index_id
            count_person += 1

        if 'Kind' in item:
            child_name = item['Kind']
            if '...,' in child_name:
                surname = ''
                parent_name = ''
                if 'hasFather' in structure:
                    surname = structure['hasFather']['family_name']
                    parent_name = structure['hasFather']['full_name']
                elif 'hasMother' in structure:
                    surname = structure['hasMother']['family_name']
                    parent_name = structure['hasMother']['full_name']
                if surname and parent_name:
                    surname = surname[0] if type(surname) == list else surname
                    parent_name = parent_name[0] if type(parent_name) == list else parent_name
                    prefix = parent_name[parent_name.index("["):] if "[" in parent_name else ''
                    full_name = surname + child_name[child_name.index(","):].strip() + ' ' + prefix
                else:
                    full_name = child_name[child_name.index(",")+1:].strip()
                structure['hasChild'] = getStructurePerson(full_name, index_id, count_person)
            else:
                structure['hasChild'] = getStructurePerson(child_name, index_id, count_person)
            structure['hasChild']['isInRecord'] = 'saaRec:'+index_id
            count_person += 1

        if 'Getuige' in item:
            if type(item['Getuige']) == list:
                structure['hasWitness'] = []
                for text in item['Getuige']:
                    temp = getStructurePerson(text, index_id, count_person)
                    temp['isInRecord'] = 'saaRec:'+index_id
                    structure['hasWitness'] += [temp]
                    count_person += 1
            else:
                structure['hasWitness'] = getStructurePerson(item['Getuige'], index_id, count_person)
                structure['hasWitness']['isInRecord'] = 'saaRec:'+index_id
                count_person += 1


        ###########################################################################
        # Formating a ttl output string to be write in the file
        # The <<outcome>> string will be composed of two parts
        # 1. A resource record itself, which will refer to other resouces deacribing Persons
        #   This part is writen directly in the variable <<outcome>>
        # 2. Several reources about Persons complied into the varialbe <<inner>>
        ###########################################################################


        outcome += " saaRec:{}\t\t a \t\t\t saaOnt:{} ;\n".format(index_id, index_type)
        if 'Doopdatum' in item:
            outcome += " \t\tsaaOnt:registration_date \t \"{}\"^^xsd:date;\n".format(item[u'Doopdatum'])
        else:
            list_no_date += [item]
        if 'Geboortedatum' in item:
            outcome += " \t\tsaaOnt:birth_date \t \"{}\"^^xsd:date;\n".format(item[u'Geboortedatum'])

        if 'Kerk' in item:
            outcome += " \t\tsaaOnt:church \t \"{}\";\n".format(to_bytes(item['Kerk']))

        if 'Godsdienst' in item:
            outcome += " \t\tsaaOnt:religion \t \"{}\";\n".format(to_bytes(item['Godsdienst']))

        if 'Bronverwijzing' in item:
            outcome += " \t\tsaaOnt:source_reference \t \"{}\";\n".format(to_bytes(item['Bronverwijzing']))

        if 'urlScan' in item:
            if type(item['urlScan']) == list:
                for url in item['urlScan']:
                    outcome += " \t\tsaaOnt:urlScan \t <{}>;\n".format(url)
            else:
                outcome += " \t\tsaaOnt:urlScan \t <{}>;\n".format(item['urlScan'])

        inner = ''
        for master_key in structure:
            elem = structure[master_key]
            if type(elem) is dict:
                elem = [elem]
            if type(elem) is list and elem != [] and type(elem[0]) is dict:
                for e in elem:
                    (outerpart, innerpart) = processStructure2RDF(e, master_key)
                    outcome += outerpart
                    inner += innerpart

        # for master_key in structure:
        #     listS = []
        #     if type(structure[master_key]) == dict:
        #         listS += [structure[master_key]]
        #     else:
        #         listS = structure[master_key]
        #     for elem in listS:
        #         outcome +=  "\t\tsaaOnt:{} \t\t saaPerson:{} ;\n".format(master_key, elem['id'])
        #
        #         inner += " saaPerson:{} \t a \t saaOnt:{} ;\n".format(elem['id'], 'Person')
        #         for key in elem:
        #             if key not in ['a','id','isInRecord'] and elem[key]:
        #                 text = to_bytes(elem[key])
        #                 inner += "\t\tsaaOnt:{}  \t\t \"{}\" ;\n".format(key, text)
        #             elif key == 'isInRecord':
        #                 inner += "\t\tsaaOnt:{}  \t\t {} ;\n".format(key, elem[key])
        #         inner = inner[:-2] + '.\n\n'

        outcome = outcome[:-2] + '.\n\n' + inner


        count_registers += 1
        if count_registers % out_size == 0:

            n_file = count_registers/out_size
            if n_file == limit_files:

                raise Exception('Limit of files is reached', 'Limit of files is reached')
            outcome += "\n } "  ## closes the named graph
            filename = output_file_name + 'N' + str(count_registers/out_size) + '.trig'
            with codecs.open(filename, 'wb', encoding='utf-8') as outfile:
                outfile.write(to_unicode(outcome))
                outfile.close()
                print "file ", filename
            outcome = outcome_start

    except Exception as error:

        print 'Final List no date', list_no_date
        outcome += "\n } "  ## closes the named graph
        filename = output_file_name + 'N' + str((count_registers-1)//out_size+1) + '.trig'
        with codecs.open(filename, 'wb', encoding='utf-8') as outfile:
            outfile.write(to_unicode(outcome))
            outfile.close()
            print "file ", filename
        outcome = outcome_start
        raise

    if count_registers % skip == 0:
        print >> sys.stderr, count_registers, '\r',
