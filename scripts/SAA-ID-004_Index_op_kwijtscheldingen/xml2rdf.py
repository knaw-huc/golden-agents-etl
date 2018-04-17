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
import re
# from findertools import location

from kitchen.text.converters import to_bytes, to_unicode

###########################################################################
# VARIABLE TO SET UP
###########################################################################
output_file_name = 'SAA-ID-004-SAA_Index_op_kwijtscheldingen'
out_size = 20000
skip = 100 #1000
limit_files = 20

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
@prefix saaProperty: <http://goldenagents.org/uva/SAA/property/@@/> .
@prefix saaPropertyNameRef: <http://goldenagents.org/uva/SAA/propertyNameRef/@@/> .
@prefix saaOnt: <http://goldenagents.org/uva/SAA/ontology/> .
###########################################################################

<http://goldenagents.org/datasets/TransportActs004> {
"""
outcome = outcome_start
list_no_date = []


housePattern = "((pak|woon|achter|hoek|koet|koets|winkel|werk|dwar|wagen|heren|vrouwen|broodbakkers|koekbakkers|koopmans|boven|kook|hooi|dwars)*hui(sjes|sen|s|zinge|zing|zen|js)|woningen)"
shipPattern = "(buis|smal|kaag|karveel|beitel|water|pinas|fluit|boeier|pot|vlot|den|dam|smak|gaffel|tjalk|zeilsteiger|steiger|galjoot|damkraak|hekboot|pink|kraak|kat|zolder|turf|snik|veer|snauw|brigantijn|brik)*(schip|scheepje|schuit|kof|eiker|fregat|wijd)"
industryPattern = "((suikerbakk|zeemtouw|siroopkok|zep|looi|steenkop|pers|beschuitbakk|pottenbakk|hoedenmak|brand|verv|brouw|blek|wasblek|kopergiet)*erij)"
gardenPattern = "(((hoek|schuitenmakersw|timmerw|scheepstimmerw|achter|w)*er(f|ven))|((warmoes|hout|speel)*tuin))"
molenPattern = "((run|koren)*molen)"

# housePattern = houseRegPattern+"[\s,;]*'"
notHouseNamePattern = '^('+housePattern+'[\s,;]*)' #String that starts with some of the house descriptions
notHouseNamePattern += '|(^'+shipPattern+'[\s,;]*)' #String that starts with some of the ship descriptions
notHouseNamePattern += '|(^'+industryPattern+'[\s,;]*)' #String that starts with some of the industry descriptions
notHouseNamePattern += '|(^'+gardenPattern+'[\s,;]*)' #String that starts with some of the garden descriptions
notHouseNamePattern += '|(^'+molenPattern+'[\s,;]*)' #String that starts with some of the garden descriptions
# notHouseNamePattern += '|(^(hoek|schuitenmakersw|timmerw|scheepstimmerw|achter|w)*er(f|ven)[\s,;]*)'
# terms2 += ['erf', 'erven', 'hoekerf', 'schuitenmakerswerf',  'timmerwerf']
# notHouseNamePattern += '|(^(warmoes|hout|speel)*tuin[\s,;]*)'
# terms2 = ['tuin', 'warmoestuin', 'houttuin']
# notHouseNamePattern += '|(^(run|koren)*molen[\s,;]*)'
# terms2 += ['runmolen', 'molen', 'korenmolen']
notHouseNamePattern += '|(^(rente|losrente|lijfrente|kustingtermijn|schuld|oudeigen)*brief[\s,;]*)'
# terms2 += ['rentebrief', 'losrentebrief', 'lijfrentebrief', 'kustingtermijnbrief', 'schuldbrief', 'oudeigenbrief']

# keukentjes

terms2 = ['spijker', 'dwarsspijker']
terms2 += ['groenhoven', 'bruikweer', 'lijnbaan', 'stuk', 'lichter', 'scheepstimmermanslichter', 'korenlichter',  'lakenraam',  'stal', 'opstal', 'paardenstal', 'slagersstal', 'slepersstalling', 'woning',  'woninkje', 'achterwoning', 'zoutkeet', 'loods', 'getimmerte', 'boot', 'boeier']
terms2 += ['hofstede', 'grafstede', 'graf', 'perceel', 'leeg', 'met', 'stenen', 'dubbele', 'dubbel', 'ledig', 'land', 'warmoesland', 'damloper', 'gedeelte', 'plaats', 'gehoogde', 'deel', 'tussen', 'uitgang', 'gang', 'bleekveld', 'helft' ]
terms2 += ['obligatie', 'houtwal', 'actie', 'resterende', 'opstaande', 'veldje', 'schepenkennis', 'beitelaak',  'kits', 'turfpot', 'turfpont', 'sloep', 'galjoot', 'dijn', 'keulse', 'vleet', 'zoutkeet', 'zandpont', 'winkel', 'loodfs', 'kamer', 'schuur', 'schuldbekentenis' ]

# Kusting-losrentes van een erf met huis daarop
# Nieuw onvolmaakt huis en erf, in het park C
# Bleekvelderf, achter de vijfde molen buiten de Regulierspoort
# Bleekveld, buiten de Sint Antoniespoort binnendijks
# Onvolmaakt schip
# Een nedergevallen huis en erf
# Dijn op pontschip
# Enkel graf
# Vrije achteruitgang
# Vrije overgang
# Vierhuizen
# Bleekveld
# Loevestein
# Keulse aak
# Grote prijsobligatie
# Aandeel in een sociëteit
# Legitieme portie
# Recepis
# Effecten
# Ramen
# Twee huizen en erven
# Uitgang, tot aan de burgwal
# Rente op 3 woningen

# Raam, buiten de Raampoort in een slopje
# Valkoog, werf met getimmerte
# Hogendorp, pakhuis en erf
# Het vierde huis van het Singel

# Jaagschuit
# Snebschuit
# Ballastschip
# Samoreus-schip
# Klerenschip
# Beurtschip

#### To review::
# Aak of beitelschip
# Vrije overgang
# Enkel graf oude kerk nz derde laag nr. 40
# Beoosten de Nieuwe Vaart naast de stadshoutwallen in park B
# Kusting-losrentes van een erf met huis daarop
# Oostfries kaagschip
# De Juffrouw Maria
# Walvisvangersgereedschap
# Portie in een contract van lijfrente
# Portie in een contract
# Een portie in een contract
# Portie in een contract van actie
# Portie in een contract van lijfrente
# Contract van overleving
# Contract van overleving
# Contract van overleving
# Portie in een contract van overleving
# Portie in een contract van overleving
# Recht over het weeshuis tussen Roskamsteeg en Spui uitkomende op de Spuistraat (Achterburgwal)
# Bij de Willemsstraat (Goudsbloemgracht) NZ
# Naast het hoekhuis van het Rokin tussen Kalverstraat en Rokin
# Houten loods en erf
# Het veertiende huis achter het hoekhuis van de Leidsestraat in park B tussen Leidsekruisstraat en Leidsestraat
# Het tweede huis bezuiden het hoekhuis van de Sloterdijkstraat benoorden de Haarlemmerpoort
# Het achtste huis van de Staalstraat  (Kleine Zwanenburgerstraat)
# Naast het hoekhuis van de Prinsengracht woonhuis en zijdeververij
# Oosthoek Keizershofsgang tot aan den Krimp tussen Lijnbaansgracht en laatste dwarsstraat
# Grote prijsobligatie
# Op de zuidoosthoek van de Batavierdwarsstraat op Uilenburg
# Aandeel in een sociëteit
# Legitieme portie
# Portie in een contract van overleving
# Buiten de Raampoort aan het einde van de Vinkenbuurt aan de Wetering
# Recepis
# Effecten
# Het vierde huis van het Singel
# Ramen
# Rente op 3 woningen
# Ramen


for t in terms2:
    notHouseNamePattern += "|^({})".format(t)
notHouseNamePattern += '|^\d'

# start = 20500
# stop = 21500
# start = 1
# stop = 500


notHouseName = set()


houseFeaturesList = ['uithangend in het onderhuis', 'op de hoek uithangend', 'naast de deur uithangend', 'in de balans uithangend', 'uithangend en boven de deur', 'naast uithangend', 'uithangend of in de gevel', 'voor uithangend', 'wederzijds uithangend', 'uithangende', 'uithangend', 'uitangend', 'uithangt', 'utihangend', 'uihangend', 'uithangen', 'uihangt', 'op een stok uitstekend', 'voor uitstekend', 'uitstekend', 'uistekend', 'in de gevel of boven de deur', 'in de gevel van het achterhuis', 'in de gevel geschilderd', 'op de gevel geschilderd', 'ertegenover in de gevel', 'naast de gang in de gevel', 'voor in de gevel', 'achter in de gevel', 'in de gevel van het voorste huis', 'onder in de gevel', 'boven in de gevel', 'in de gevel uitgehouwen', 'uitgehouwen in de gevel', 'in de gevel gehouwen', 'op de hoek van de gang in de gevel', 'in de gevel van het achterhuis', 'in de zijgevel', 'boven de trap in de gevel', 'ten dele in de gevel', 'in de gevel staat', 'boven de deur in de gevel', 'bovenin de gevel', 'in de gevel', 'in dfe gevel', 'bovenop de gevel', 'boven op de gevel', 'op de gevel', 'boven gevel', 'boven de gevel', 'voor de gevel hangend', 'in het bord voor de gevel', 'voor de gevel gespijkerd', 'voor de gevel', 'voor aan de gevel', 'in de voorgevel', 'in de achtergevel', 'boven aan de gevel ', 'aan de gevel geschilderd', 'aan de gevel', 'voor tegen de gevel', 'tegen de gevel aangespijkerd', 'tegen de gevel', 'boven de deur geschilderd', 'boven de deur gespijkerd', 'boven de deur uitgehouwen', 'boven de deur van de poort', 'boven de deur hangend', 'in het kalf boven de deur', 'boven de onderdeur', 'boven in de deur', 'in de deur', 'boven de deur van de gang', 'boven de deur geschreven', 'voor of boven de deur', 'boven de deur en poort', 'boven de deur', 'boven deur', 'op de deur geschilderd', 'op de deur geschreven', 'op de deur', 'voor de deur van de gang', 'voor de deur geschilderd', 'voor de deur s', 'voor de deur ', 'aan de deur', 'boven de ingang van de deur', 'boven de ingang', 'bij de ingang', 'op de poort van de gang', 'voor de poort geschilderd', 'voor de poort van de gang geschilderd', 'voor de poort', 'boven de voorpoort', 'bij de deur geschilderd', 'bij de deur van de gang', 'bij de deur', 'ervoor geschilderd', 'ervoor geschreven', 'ervoor', 'met een bord op de luifel', 'voor op de luifel', 'op de luifel', 'in de luifel', 'aan de luifel', 'tegen de luifel', 'boven de pui', 'boven de gangdeur', 'boven de poort uitgehouwen', 'boven de poort of deur', 'boven de poort', 'naast en op de poort geschilderd', 'op de poort', 'voor de luifel', 'boven de luifel', 'uitgehouwen boven de gang', 'boven de gang', 'voor de gang geschilderd', 'voor de gang', 'in de gang', 'op de pakdeur geschreven', 'op de pakdeuren', 'op de pakdeur', 'op de scheiding', 'erbovenop', 'erboven', 'uitgedrukt in een brievendeur', 'in het kalf van de deur gesneden', 'in het kalf', 'tegen de pui gespijkerd', 'tegen de pui', 'boven de staat', 'voor het huis gespijkerd', 'voor het huis', 'op de stok', 'in de puibalk', 'in de pui', 'op de hoek van de gang', 'voor de looierij', 'op het huis', 'erop geschilderd', 'erop', 'boven de post van de deur', 'voor het hek geschilderd', 'tegen de zijgevel', 'op het zoldervenster', 'aan het dakvenster geschilderd', 'voor aan de poort', 'aan de poort', 'voor een deel', 'tegen het pakvenster geschilderd', 'aan elke zijde', 'ertegen', 'aan het poortje', 'geschilderd', 'tegen de schutting', 'op de loods', 'in het zolderraam', 'in het zoldervenster', 'belend', 'uitgestoken', 'boven het huis', 'aan elke zijde']
#in the voorste gevel -> gable stone
patternGevel = "(aan|achter|voor|boven|onder|op|de|ertegenover|in|naast|scheiding|tegen|ten|uitgehouwen|uithangend).*(gevel)( (geschilderd|aangespijkerd|gespijkerd|hangend|gehouwen|uitgehouwen|staat|of boven de deur|.*(achter|voor).*huis))*"
patternNameRefPos = "\W((er)*((aan|achter|voor|boven|onder|op|de|ertegenover|in|naast|scheiding|tegen|ten|uitgehouwen|uithangend|met|bij|wederzijds|uitgedrukt|van)[^A-Z]*(luifel|gevel|deur|poort|zijde|dakvenster|ingang|gang|pui|staat|huis|balkon|kalf|zolderraam|zoldervenster|bord|uithangend|uitstekend|hoek|hek|schutting|tuin|deel|dele|looierij|puibalk|loods|pakdeuren|scheiding|stok|pakvenster)(je)*(en)*( (geschilderd|aangespijkerd|gespijkerd|hangend|gehouwen|uitgehouwen|staat|of boven de deur|geschreven|gesneden|.*(achter|voor).*huis)|.*gang|.*ertegenover)*)|(belend|bokkinghang|wederzijds|utihangend|uitstekend|uithangt|uithangende|uithangend|uithangen|uitgestoken|uitangend|uistekend|uihangt|uihangend|schutting|scheiding|geschilderd|(pakdeur|puibalken|erboven|erop|ertegen|ervoor)( geschreven|en|op| geschilderd)*))"
patternMainRefPos = "\W(luifel|gevel|deur|poort|zijde|dakvenster|ingang|gang|pui|staat|huis|balkon|kalf|zolderraam|zoldervenster|bord|uithangend|uitstekend|hoek|hek|schutting|tuin|deel|dele|looierij|puibalk|loods|pakdeuren|scheiding|stok|pakvenster|belend|bokkinghang|wederzijds|utihangend|uitstekend|uithangt|uithangende|uithangend|uithangen|uitgestoken|uitangend|uistekend|uihangt|uihangend|schutting|scheiding|geschilderd|(pakdeur|puibalken|erboven|erop|ertegen|ervoor)( geschreven|en|op| geschilderd))"


def cleanSeveralNamesErrors(description):
    description = description.replace('; Gerrit van Velsen', '; Gerrit van Velsen,')
    description = description.replace('De; Victoria Gouden Valk', 'De; Victoria, Gouden Valk')
    description = description.replace('Het; Straatsburg', 'Het; Straatsburg,')
    description = description.replace('; Fortuin Hoop Geloof en Liefde dat de Waarheid', '; Fortuin Hoop Geloof en Liefde dat de Waarheid,')
    description = description.replace('De; Amsterdam Drie Tassen', 'De; Amsterdam, Drie Tassen')
    description = description.replace('De; Botjenter Leeuw in de gevel', 'De; Botjenter, Leeuw in de gevel')
    description = description.replace('De; Vredenburg Oranjeboom', 'De; Vredenburg, Oranjeboom')
    description = description.replace('De; Zweden Witte Leeuw', 'De; Zweden, Witte Leeuw')
    description = description.replace('De; Kolderzicht Lindeboom', 'De; Kolderzicht, Lindeboom')
    description = description.replace('De; Schellingwoude Lammerenberg in de gevel', 'De; Schellingwoude, Lammerenberg in de gevel')
    description = description.replace('Het; Lust en Werk Oude Varken', 'Het; Lust en Werk, Oude Varken')
    description = description.replace('De; Parijs IJzerberg', 'De; Parijs, IJzerberg')
    #####
    description = description.replace('Het Constantia Rode Kalf uithangend', 'Het Constantia, Rode Kalf uithangend')
    description = description.replace('De; Valkoog Zwaan', 'De; Valkoog, Zwaan')
    description = description.replace('La; Libourn; Bergerac Rochelle', 'La; Libourne, Bergerac Rochelle')
    description = description.replace('Het; Samson Utrechtse Veerhuis', 'Het; Samson, Utrechtse Veerhuis')
    description = description.replace('Het; Haas boven de deur', 'Het Haas boven de deur')
    description = description.replace('De; Vestzicht Kleine Bijenkorf', 'De; Vestzicht, Kleine Bijenkorf')
    description = description.replace('De; Lansmans Lijnbaan Grote Zeevaart', 'De; Lansmans Lijnbaan, Grote Zeevaart')
    description = description.replace('De; Mars; Aarde; Venus; Mercurius Zeven Planeten', 'De Mars; Aarde; Venus; Mercurius Zeven Planeten')
    description = description.replace('Het; Munster Witte Garenhuis', 'Het Munster; Witte Garenhuis')
    description = description.replace('De; Sapffenberg Vlies van Dordt', 'De; Sapffenberg, Vlies van Dordt')
    description = description.replace('De; Blauw Jan Hoop', 'De; Blauw, Jan Hoop')
    description = description.replace('Het; Munster Witte Garenhuis uithangend', 'Het Munster, Witte Garenhuis uithangend')
    description = description.replace('De; Prins Frederik Koning van Denemarken Karelshaven uithangend', 'De; Prins Frederik Koning van Denemarken, Karelshaven uithangend')
    description = description.replace('De; Binnen en Buiten Zicht Drie Oude Bloempotten', 'De; Binnen en Buiten Zicht, Drie Oude Bloempotten')
    description = description.replace('Het; Buiten Rust Varken', 'Het; Buiten Rust, Varken')
    description = description.replace('De; Zorg en Rust IJzeren Berg boven de deur', 'De; Zorg en Rust, IJzeren Berg boven de deur')
    description = description.replace('De; Saturnus Zon', 'De; Saturnus, Zon')
    description = description.replace('Het; Samson Vergulde Koffer uithangend', 'Het; Samson, Vergulde Koffer uithangend')
    description = description.replace('Het; Smolensko Turfschip van Breda', 'Het; Smolensko, Turfschip van Breda')
    description = description.replace('Het; Zorg en Vlijt Vosje', 'Het; Zorg, Vlijt Vosje')
    description = description.replace('Het; Schenkenschans Karthuizerklooster', 'Het; Schenkenschans, Karthuizerklooster')
    description = description.replace('De; Haarlem Walvis', 'De; Haarlem, Walvis')
    description = description.replace('De; Harlingen Gulden Kleerbezem uithangend', 'De; Harlingen Gulden, Kleerbezem uithangend')
    description = description.replace('Het; Waagdragers Welvaren Groene Woud', 'Het; Waagdragers Welvaren, Groene Woud')
    description = description.replace('Het; Nooit Gedacht Welvaren van de Levant', 'Het; Nooit Gedacht, Welvaren van de Levant')
    description = description.replace('De; Voorbreed Rode Galei uithangend', 'De; Voorbreed Rode, Galei uithangend')
    description = description.replace('Het; Nieuw Malta Veldhoen in de gevel', 'Het; Nieuw Malta, Veldhoen in de gevel')
    description = description.replace('De; Jan Izak Koning van Perzië', 'De; Jan Izak, Koning van Perzië')
    description = description.replace('Het; Arion Recht Kromhout in de gevel', 'Het; Arion Recht, Kromhout in de gevel')
    description = description.replace('De; Slagers Welvaren Bonte Os', 'De; Slagers, Welvaren Bonte Os')
    description = description.replace('De; Stad Hasselt in de gevel', 'De; Stad, Hasselt in de gevel')
    description = description.replace('Het; Kortenhoef Vergulde Bekken uithangend', 'Het; Kortenhoef, Vergulde Bekken uithangend')
    description = description.replace('De; Sphaera Mundi Wereldkloot', 'De; Sphaera Mundi, Wereldkloot')
    description = description.replace('La; Libourne; Bergerac Rochelle', 'La; Libourne, Bergerac Rochelle')

    # description = description.replace('De; Baarn in de gevel; Zon uitstekend De Witte Zaag uithangend', 'De Baarn in de gevel; Zon uitstekend; De Witte Zaag uithangend')
    # description = description.replace('De; Jan Knollenkerk uithangend; Koning David op de deur,', 'De Jan Knollenkerk uithangend; Koning David op de deur;')
    # description = description.replace('De; Burgers Gerief uithangend; Karel de Twaalfde, Koning van Zweden uithangend', 'De; Burgers Gerief uithangend; Karel de Twaalfde Koning van Zweden uithangend;')


    description = description.replace('De; Haan', 'De Haan')
    description = description.replace('De; Verslijt Tijd', 'De Verslijt Tijd')
    description = description.replace('De; Valk uithangend', 'De Valk uithangend')
    description = description.replace('De; Nabij Buiten Post', 'De Nabij Buiten Post')
    description = description.replace('De; Zomerlust Vrede', 'De Zomerlust Vrede')
    description = description.replace('De; Ashof Boom', 'De Ashof Boom')
    description = description.replace('De; Zomervreugd Otter', 'De Zomervreugd Otter')
    description = description.replace('Het; Nigtevecht Krapvat', 'Het Nigtevecht Krapvat')
    description = description.replace('De; IJzicht Beer', 'De IJzicht Beer')
    description = description.replace('Achtkante windcementmolen;', 'Achtkante windcementmolen')
    description = description.replace('Het; Neurenberg Marsje', 'Het Neurenberg Marsje')
    description = description.replace('De; Moscovië Blauwe Pakhuis', 'De Moscovië Blauwe Pakhuis')
    description = description.replace('De; Paramaribo Vrede', 'De Paramaribo Vrede')
    description = description.replace('De; Malta Boonakker', 'De Malta Boonakker')
    description = description.replace('De; Weesp Nijptang', 'De Weesp Nijptang')
    description = description.replace('De; Veronica\'s Baan Dolfijn', 'De Veronica\'s Baan Dolfijn')
    description = description.replace('Blekerij; scheepstimmerwerf', 'Blekerij scheepstimmerwerf')
    description = description.replace('De; Ararat Pijnappel', 'De Ararat Pijnappel')
    description = description.replace('Het; Vergulde Tralie uithangend', 'Het Vergulde Tralie uithangend')

    return description


def processSeveralNames(listDesc, count):
    # there's at least one ;
    houseNamesPre = []
    for i in range(len(listDesc)): # check for all possible ;
        # check if the ; is only indicating there's another houseName or if it is dividing
        splitAux = listDesc[i].strip().split(';')
        if len(splitAux) > 1:
            # # It is possibly being used to divide the first and second names
            # # OR It is used only to indicate the occurence of other names: 'De;' or 'Het;'
            # # However, this name is not always the next in the split of ',''
            # # either because no separator was used, or because the separator is another ;
            # # suggesting that yet another name is to come
            # # De; Burgers Gerief uithangend; Karel de Twaalfde Koning van Zweden uithangend Drie Friese Timmerlieden uithangend
            j = 0
            supposedHousename1 = listDesc[i].strip() # to make sure it starts with a value
            # print description, count
            while j < len(splitAux)-1: # while j is not the last position in split
                # Het; Schaap op de scheiding in de gevel, Het Kantoorinktvat uithangend, huis en erf
                # print 'Start:', splitAux[j].strip()
                if splitAux[j].strip() not in ['De', 'Het', 'In de', '\'s']:
                # if the first split is not a part of the name
                    # print '1 - name:', splitAux[j].strip()
                    # print '    next:', splitAux[j+1].strip()
                    houseNamesPre += [splitAux[j].strip()]
                    supposedHousename1 = splitAux[j+1].strip()
                else:
                # if the first split is a part of the name => concatenate with the next part
                    if j+1 < len(splitAux)-1: # j+1 is not the end of the split => next iteration
                        # print '2.1 - name:', splitAux[j].strip() + ' ' + splitAux[j+1].strip()
                        houseNamesPre += [splitAux[j].strip() + ' ' + splitAux[j+1].strip()]
                        supposedHousename1 = splitAux[2].strip()
                        j += 1 # will jump two positions
                    else: # j+1 is the end of the split => go check if it has no separator
                        # print '2.2 - supposedname:', splitAux[j].strip() + ' ' + splitAux[j+1].strip()
                        supposedHousename1 = splitAux[j].strip() + ' ' + splitAux[j+1].strip()
                        break
                j += 1

            # check for the occurence of a feature in the first wihtout a comma afterwards
            # supposedHousename1 = listDesc[i].strip()
            found = False
            for f in houseFeaturesList:
                # if (f in supposedHousename1) and (not supposedHousename1.endswith(f) or f in supposedHousename1[:-4]):
                if f in supposedHousename1[:-4]:
                    # Het; Jonas uit de Walvis boven de deur Grauwe Kalfsvel boven de deur
                    if f+';' in supposedHousename1:
                        break
                    # print f, supposedHousename1
                    supposedHousename1 = supposedHousename1[:-4].replace(f, f+',') + supposedHousename1[-4:]
                    # print description
                    # print supposedHousename1
                    splitAux2 = supposedHousename1.split(',')
                    # print f, supposedHousename1, splitAux2
                    houseNamesPre += [splitAux2[0].replace(';','').strip()]
                    houseNamesPre += [splitAux2[1].replace(';','').strip()]
                    # print '2.3 - name:', splitAux2[0].replace(';','').strip()
                    # print '    - name:', splitAux2[1].replace(';','').strip()
                    found = True
                    break
            if found:
                break
            else:  # ';' in listDesc[i]:
                houseNamesPre += [supposedHousename1.replace(';','').strip()]
                # print '2.4 - name:', supposedHousename1.replace(';','').strip()
                if len(houseNamesPre) >= count:
                    break
        else:
            houseNamesPre += [listDesc[i].strip()]
            # print '2.5 - name:', listDesc[i].strip()
            break
    # print ''
    return houseNamesPre


patternGable = "((van|in|op|bovenin|bovenop|boven|aan) (de |dfe )*(zij|voor|achter)*gevel)|(boven de (deur|poort|gang))|(in een brievendeur)|(erboven)"
patternBoard = "((ui(t)*stekend)|(u(t)*i(t)*(h)*ang(t|en))|(voor aan|voor|aan|tegen) (de |dfe )*(zij|voor|achter)*gevel)|(op de stok)|(op het bord)|(uitgestoken)"
patternCanopy = "(luifel)"
def extractNameRefLoc(houseNamePre):
    nameRefPos = ''
    nameRefPosType = 'none'
    # Test for the patterns
    # resultMainRefPos = re.findall(patternMainRefPos, houseNamePre)
    if True: # len(resultMainRefPos) > 0:
        resultNameRefPos = re.findall(patternNameRefPos, houseNamePre)
        # if len(resultNameRefPos) == 0:
        #     print houseNamePre
            # Ieder 1/2 huis en erf
            # Houten loods zijnde een parserij
            # Houten loods
            # Grond en loods
            # Houten loods
            # Afgekeurd huis
            # Afgekeurd huis en erf
            # 1/2 tuin en huizinge
            # Houte getimmerde huis
            # Ieder 1/2 huis en erf
        for tupleElem in resultNameRefPos:
            # print tupleElem
            for nrp in tupleElem:
                if nrp != '':
                    nameRefPos = nrp
                    # print nrp, '\t', houseNamePre
                    break
        dictName = {'name': houseNamePre.replace(nameRefPos,'').strip()}
        if nameRefPos != '':
            dictName['position'] = nameRefPos
            resultFType = re.findall(patternGable, nameRefPos, re.IGNORECASE)
            if len(resultFType) > 0:
                dictName['type'] = 'gable_stone'
            resultFType = re.findall(patternBoard, nameRefPos, re.IGNORECASE)
            if len(resultFType) > 0:
                dictName['type'] = 'sign_board'
            resultFType = re.findall(patternCanopy, nameRefPos, re.IGNORECASE)
            if len(resultFType) > 0:
                dictName['type'] = 'canopy'
    return dictName


patternFraction = "^(\d+[\\/ ]?\d*)"
def extractPropertyLegalFraction(descProp):
    resultFraction = re.findall(patternFraction, descProp.strip(), re.IGNORECASE)
    # print result3
    if len(resultFraction) > 0:
        fraction = resultFraction[0]
    else:
        fraction = '1'
    # print fraction, descProp, '\n'
    # ('De Bul', 'none', '') 1  korenmolen
    return fraction


# patternHouseDescr = "(?<!Het) ((\d+[\/ ]?\d*)*(achterhuis|pakhuis|huis|huizen)((, [\w\d\s]+)*( en [\d+\s]*(achterhuis|huis|huizen|erf|erven|ververij|erf zijnde een ververij|achterwoning)(en)?))*( met [^,|$|\n]*)*)"
patternHouseDescr = "(([\w\s\d\\/]*(\d+[\\/ ]?\d*)*[\w\s]* (?<!Het )(?<!voor het )(?<!op het )"+housePattern+"(([,\w\d\s]+(\?\>\! en ))*( en [\w\d\s]+))*( met [\w\d\s]+(([,\w\d\s]+(\?\>\! en ))*( en [\w\d\s]+))*)*))"
# TODO: Capture this sort of house type, taking the geoRef as limits
# Het Kasteel van Antwerpen uithangend, 2 huizen, 2 ververijen en erven met 2 woningen onder een dak, buiten de Sint Antoniespoort op de nieuw gegraven gracht
# For now it takes only what comes before the comma: 2 huizen
def extractHousePropertyDescription(description):
    # print description
    resultHouseDescr = re.findall(patternHouseDescr, description)
    # print resultHouseDescr
    if len(resultHouseDescr) > 0:
        # print resultHouseDescr[0][0]
        return resultHouseDescr[0][0]
    return ''
    # else:
    #     print description


# patternPropDescr = ", ((\d+[\/ ]?\d*)*(erf|erven|tuin|brouwerij)((, \w+)*( en (huis|huizen|erf|erven|ververij)))*( met [^,|$|\n]*)*)"
patternPropDescr = "(([\w\s\d\\/]*(\d+[\\/ ]?\d*)*[\w\s]* (?<!Het )("+shipPattern+"|"+industryPattern+"|"+gardenPattern+"|"+molenPattern+")(([,\w\d\s]+(\?\>\! en ))*( en [\w\d\s]+))*( met [\w\d\s]+(([,\w\d\s]+(\?\>\! en ))*( en [\w\d\s]+))*)*))"
# print patternPropDescr
def extractPropertyDescription(description):
    resultPropDescr = re.findall(patternPropDescr, description)
    # print resultPropDescr
    if len(resultPropDescr) > 0:
        # print resultPropDescr[0]
        return resultPropDescr[0][0]
    return ''
    # else:
    #     print description


patternGeoRef = '[\s]((daar|achter|voor|bij|in|naar|belend|strekkend|dicht|tegen|over|binnen|buiten|op|naast|bezijden|gang|vleugel|park|hoek|schuin[s]?|aan|tot|uitkomend|midden|van|recht|even|tussen|beide|en|\([\w\s]+\)|(een|de|het)[\w\s]*|\s)+ (([A-Z]\w*|,|en|\s)+|(laatste|eerste|nieuwe) \w+)+(\([\w\s]+\))*)+|((aan de )*(Be|be)?(zuid|oost|noord|west)+(zijde|en|hoek)* (de|het|[A-Z]\w+|van|\s)+)|([A-Z]\w+ (\(.*\) )*(zuid|oost|noord|west)+(zijde|en|hoek)*)|((het|Het|de) (eer|elf|twee|twaalf|der|vier|veer|vijf|zes|zeven|acht|negen)*(ste|de|tiende) \w+)|(met (\d+ )*[\w\s]+((ernaast|daarachter)[\w\s]+)* (in|aan) (de|het|een)([A-Z]\w*|,|en|\s)+)'
def extractGeoReference(description, propDesc):
    index = description.find(propDesc) + len(propDesc)
    if index < len(description):
        # print '\n', description
        # print propDesc
        # print description[index:]
        resultGeoReference = re.findall(patternGeoRef, description[index:])
        if len(resultGeoReference) > 0:
            # print resultGeoReference[0]
            return resultGeoReference[0][0]
    return ''

    # De Vergulde Doofpot uithangend, huis, erf, 2 achterwoningen en plaats, buiten de oude Regulierspoort
# recht over het|de ...
# ... vooraan|op|van|buiten|ann|naast|bij een|het|de(n)* ...
# tussen .* en .*
# zuidoosthoek|noordhoek|westhoek|bezuiden|benoorden|beoosten|achter het|de
# ((daar|achter|voor|bij|in|belend|strekkend|dicht|tegen|over|binnen|buiten|op|naast|bezijden|gang|hoek|schuin[s]?|aan|tot|\s)+ (de|het|[A-Z]\w+)+(en|de|het|[A-Z]\w+|uitkomend|op|een \w+|tot|midden|van|\s)+)
# |((aan de )*(Be|be)?(zuid|oost|noord|west)+(zijde|en|hoek)* (de|het|[A-Z]\w+|van|\s)+)|([A-Z]\w+ (\(.*\) )*(zuid|oost|noord|west)+(zijde|en|hoek)*)|(tussen [\w\s]+ en [\w\s]+)|(tussen beide [\w\s]+)|((het|Het|de) (eer|elf|twee|twaalf|der|vier|veer|vijf|zes|zeven|acht|negen)*(ste|de|tiende) \w+)|(met (\d+ )*[\w\s]+((ernaast|daarachter)[\w\s]+)* (in|aan) (de|het|een)[\w\s]+)
# ((((be)(zuid|oost|noord|west)+(en))|(zuid|oost|noord|west)+|(achter))(het|de))
# Het vierde huis van het Singel
# Het veertiende huis achter het hoekhuis van de Leidsestraat in park B tussen Leidsekruisstraat en Leidsestraat
# Beoosten de Nieuwe Vaart naast de stadshoutwallen in park B
# voorbij de Binnen Oranjestraat (eerste kruisstraat) binnendijks belend achter de Vinkenstraat (Middelstraat)
# Vlissingen, huis en erf, voorbij de eerste dwarsstraat
# Het Leidse Wapen, molen, buiten de nieuwe fortificatie tussen molens De Vrede en De Steur
# Kalfje Spaar Uw Hooi, huis en erf, over de Handboogsdoelen
# De Kruip, korenmolen, binnen de nieuwe fortificatie
# De Valk, molen, over het Oudezijds Huiszittenhuis
# De Drie Baarsjes uithangend, huis en erf, door een gang in te gaan
# Het Wapen van Amsterdam in de gevel, pakhuis, hoek Tuinstraat
# De Twee Vergulde Elanden uithangend, huis en erf, tussen beide Kerkstegen
# De Vijfhoek, huis en erf, buiten [de] Haarlemmerpoort
# De Arke Noachs, huis en erf, gang over de Koestraat
# De Enkhuizer Maagd in de gevel, huis en erf, op de Haarlemmerstraat
# De Beer, molen, op het het eerste pad benoorden de Haarlemmerpoort
# Het Zwarte Paard met Klompen uithangend, huis en erf, tegenover het Sint Jacobstorentje
# Achterhuis, achter de Kruiskerk en het huis Jeruzalem
# De Witte Voet uithangend, huis en erf, schuin over de Westermarkt
# De Rode Leeuw in de gevel, huis en erf, schuins over de Westermarkt
# De Gooier, 1/2 molen met woning, op het bolwerk aan de Stenen Beer
# De Graaf van Buren, huis en erf, tussen Oudezijds Armsteeg (Narmsteeg) en sluis, belend het Wijngaardsstraatje en de gang van De Vette Hen
# De Drie Witte Rozen uithangend, huis en erf, naast de hoek van de Moddermolensluis
# De Fortuin, korenmolen, bolwerk beoosten de Weesperpoort
# De Drie Koningen uithangend, huis en erf, bezijden de Raadhuisstraat (Huiszittensteeg)
# De Haagse Landouwen, huis en erf, het tweede huis voorbij de eerste dwarsstraat
# De Stadstrompetters uithangend, huis en erf, achter het hoekhuis van de Nieuwezijds Voorburgwal
# A√§ron in de gevel, huis en erf, Waterlooplein (Nieuwe Houtgracht) noordzijde
# De Nachtegaal, huis en erf, het zesde huis benoorden de Wolvenstraat
# Het Witte Paard, huis en erf, recht over de Prinsensluis
# Het Witte Lam, huis en erf, even buiten de Leidsestraat
# De Gekroonde Blauwkuip uithangend, huis en erf, vleugel van de sluis tussen Sint Antoniesbreestraat en Raamgracht
# De Blinde Ezel, huis en erf, uitkomende op de Nieuwezijds Voorburgwal (Hopmarkt)
# De Adelaar, huis en erf, met uitgang in de Vinkenstraat
# De Witte Gevel ervoor, huis en erf, noordhoek Kalfsvelsteeg
# De Drie Kronen in de gevel, huis en erf, over de laatste dwarsstraat
# Het Hart in de gevel, huis en erf, in het midden van de Appelmarkt
# Medemblik uithangend, huis en erf, bij de Haarlemmersluis over de Ramskooi (Ranscoi) hoek Brouwerssteeg
# Het Delftse Wapen, huis met brouwerij en mouterij, met uitgangen in de Wijdesteeg, Nieuwezijds Voorburgwal en Spuistraat
# De Drie Kijftgen, lichter, tegenover de Gelderse Kaai
# De Morgenstond, huis en erf zijnde een ververij, in de ververij bij de Nieuwe Anthoniesluis
# Het Dorstige Hert uithangend, 1/2 huis en erf, dicht bij de Warmoesstraat
# De Citer uithangend, huis en erf, op Vlooienburg
# De Vier Vergulde Leeuwen, huis en erf, strekkend tot achter aan de Martelaarsgracht (Haarlemmersluis)
# De Drie Huiken uithangend, huis en erf, achter uitkomend op een steeg
# De Vergulde Pot, huis, erf en achterwoning, belend de Damsluis aan de OZ strekkend tot achter aan de opgang van de Beurs
# Bergen in Noorwegen uithangend, huis en erf, met gang aan de NZ naar het Rokin (Amstel)
# De Geboren Hollander uithangend, huis, erf, plaats, achterwoning en gang, gang aan de OZ
# Het Nieuwe Slot van Gelder, huis en erf met plaats, spijker en huis, in een poortje achter uitkomend op het Groot Hemelrijk (Hemelrijk)
# Het Reigertje, huis en erf, met vrije uitgang in de Duivensteeg (Duifjessteegje)
# De Drie Suikervormen in de gevel, huis, erf, 4 woningen en pottenbakkerij, met 2 woningen ernaast en 2 woningen daarachter in een steegje aan de WZ
# De Rode Wagen in de gevel, huis, erf en tuin, binnen voor de Haarlemmerpoort
# De Blauwe Arend, huis en erf met ververij en ziederij, aan de noordzijde van het Glashuis


# De afbraak van 2 huizen en erven, het vierde en vijfde huis van de Prinsengracht
# De Hoop, pakhuis en erf
# De Fortuin, 1/2 pakhuis en erf

# De Wildeman, tuin
# De Os, brouwerij


def processPropertyNames(description):
    ################################################################
    # DATA DESCRIPTION
        # There can be houses either with names-references or without.
        # There can also be other types of properties (named or not): ship, tuin, land, etc.
        # One or more house names (gable-stone, sign board or canopy) names are always followed by:
            # OPTIONAL: a description of where to physically find the 'name/reference' in the house (e.g. in de gevel)
            # MANDATORY: a description of the type of property being described, possibly preceeded by
                # the legal-part being dealed (1/2 huis, archterhuis en erf)
            # OPTIONAL: a geo-reference (e.g tussen Korte Prinsengracht (Prinsensluis) en Buiten Oranjestraat)
            # Those elements, when present, are split by ',' (not exclusive use)
            # More than one house-name-references are indicated by the use of ';' (exclusive use)
                # some house names have no character spliting them
                # when the description indicating gable-stone, sign board or canopy is present, it can be used for spliting
        # CONTATIN HUISNAME: De; Spijkermand met een bord op de luifel, De Drie Prinsen, 1/2 huis en erf, tussen ophaalbrug en Prins Hendrikkade (Oude Teertuinen)
        # DO NOT CONTATIN HUISNAME: Huis en erf, noordhoek Steenkoperssteeg

    ################################################################
    # DATA PROBLEMS
    # Abraham; Isaac; Jacob, 3 huizen en erven en de gangen
    # Issue1: how to know if the several names are alternative to a house or names for several houses?

    # Issue2: Each "feature" belongs to a house-name, where it's reference can be seen

    # Issue 3: Data problems
    # Onrustige Bokken. De, huis en erf, bij de Oude Sluis
    # Pasteibakker in de gevel. De, huis en erf
    # Het Gulden Hart uithangend, huis en erfd\, bij de Hartenstraat

    ################################################################
    # DATA PROCESS RATIONALE
    # 1st strategy: Eliminate descriptions that have no comma
        # Hypothesis: no house or or property name will be found (to check, specially for ships)
    # 2nd strategy: Eliminate descriptions that start with keywords that cannot be a house name
        # Hypothesis: a regular expressions can capture the descriptions that start with terms that cannot be a house name
        # despite having a comma-separator that include aditional information about the property such as geo-reference
    # 3rd-A strategy: Include descriptions containing a semicolon-separator, extract house name
        # Hypothesis: descriptions containing a semicolon-separator, meaning several names,
        # are not used for other properties than house
        # TODO: Fix it: not only house names
        # De; Groene Molen, De Bul, korenmolen, bolwerk beoosten de Amstel
        # De; Victoria Gouden Valk, 7/16 fluitschip, in de Oude Waal
        # De; Vergulde Kieviet, De Witte Fortuin, lichter, bij de Zoutketen aan de timmerwerf van de Boertjens
        # De; Sint Paulo, De Valk, hoekerscheepje, voor de werf van mr. Jan Rogh
        # Klein Kostverloren; Overdiep, hofstedeke, in de ban van Amstelveen strekkende bezuiden de ringsloot van de Diemermeer tot aan het Nieuwediep in het noordwesten
    # 3rd-B strategy: Include descriptions containing after a comma-separator the term 'huis', its synonims and variances,
        # extract house name
        # Hypothesis: The presence of the term 'huis', its synonims and variances after a comma-separator,
        # mean that the description before the comma is a house names
    # 4th strategy: Extract the reference of the location where the name-sign can be found
        # Hypothesis: House names may indicate with some terms types of location such as gable-stone, sign-board, canopy
    # 5th strategy: Extract the reference of the location where the name-sign can be found
        # Hypothesis: House names may indicate with some terms types of location such as gable-stone, sign-board, canopy

    ################################################################

    # 1st strategy: Eliminate descriptions that have no comma
    # Hypothesis: no house or or property name will be found (to check, specially for ships)
    # TODO: check hypothesis
    if "," not in description:
        # notHouseName.add(description)
        pass
        # HINT: uncomment this to print to produce a list of descriptions that are believed not to contain a house name
        # print description
    else: ## It can be house names
        # 2nd strategy: Eliminate descriptions that start with keywords that cannot be a house name
        # TODO-1: check for false positives or negatives
        # TODO-2: work on extracting other properties names
        description = cleanSeveralNamesErrors(description)
        resultNotHouseName = re.findall(notHouseNamePattern, description, re.IGNORECASE)
        houseNamePre = ''
        houseNamesPre = []
        dictHouseName = None
        if len(resultNotHouseName) > 0: # Then the description does not contain a house name
            # notHouseName.add(description)
            pass
            # HINT: uncomment this to print to produce a list of descriptions that are believed not to contain a house name
            # print description
        else:
            # HINT: uncomment this to print to produce a list of descriptions that could contain a house name from a certain start
            # if count_registers > start:
                # print count_registers, ' : ', description
            descList = description.split(',')
            if ';' in descList[0]:
                # 3rd-A strategy: Include descriptions containing a semicolon-separator
                    # Hypothesis: descriptions containing a semicolon-separator, meaning several names,
                    # are not used for other properties than house
                    # TODO: Fix it: not only house names
                    # De; Groene Molen, De Bul, korenmolen, bolwerk beoosten de Amstel
                # The ammount of house names are given by the ammount of semicolon-separators plus one
                # If the property description mentions more than one house, than each name is a different houses
                # otherwise, it actually contains be several names for the same house
                count = description.count(';') + 1
                houseNamesPre = processSeveralNames(descList, count)
                dictHouseName = {'names': []}
                for houseNamePre in houseNamesPre:
                    dictHouseName['names'] += [extractNameRefLoc(houseNamePre)]
                    # print dictHouseName #, description
            else:
                # 3rd-B strategy: Include descriptions containing after a comma-separator the term 'huis', its synonims and variances
                    # Hypothesis: The presence of the term 'huis', its synonims and variances after a comma-separator,
                    # mean that the description before the comma is a house names
                result2 = re.findall(patternHouseDescr, descList[1], re.IGNORECASE)
                if len(result2) > 0:
                    houseNamesPre = [descList[0]]
                    dictHouseName = {'names': [extractNameRefLoc(houseNamesPre[0])]}
                    # print dictHouseName #, description

            ##### DESCRIPTION OF PROPERTY HAS TO BE EXTRACTED WITH PATTERN CAUSE IT HAS COMMA
            if len(houseNamesPre) > 0:
                dictHouseName['propertyType'] = extractHousePropertyDescription(description)
                # if is not a house-property, check the other types
                if dictHouseName['propertyType'] == '':
                    dictHouseName['propertyType'] = extractPropertyDescription(description)
                # else:
                    # print description
                if dictHouseName['propertyType'] != '':
                    dictHouseName['legalFraction'] = extractPropertyLegalFraction(dictHouseName['propertyType'])
                    geoRef = extractGeoReference(description, dictHouseName['propertyType'])
                    if geoRef != '':
                        dictHouseName['geoReference'] = geoRef
                # else:
                #     print description
                # print dictHouseName
            # else:
            #     print description
                # Het Doolhof, 1/2 erf
                # De Wildeman, tuin
                # De Os, 3 erven met getimmerte zijnde een brouwerij
                # De Os, brouwerij
                # >>>>> Twee woningen met tuin, westeinde, buiten de Regulierspoort
                # De Os, brouwerij
                # De Kieviet, molen, buiten de Regulierspoort
                # Het Leidse Wapen, molen, buiten de nieuwe fortificatie tussen molens De Vrede en De Steur
                # De Os, brouwerij
                # De Kruip, korenmolen, binnen de nieuwe fortificatie
                # De Valk, molen, over het Oudezijds Huiszittenhuis
                # Het Anker, brouwerij
                # De Boonstoppel, lichter
                # De Rozenboom, molen
                # Het Luipaard, molen, buiten de Raampoort
                # De Beer, molen, op het het eerste pad benoorden de Haarlemmerpoort
                # De Vier Winden, lichter
        return dictHouseName


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
        path, item = marshal.load(sys.stdin)

        # if count_registers <= x000:
        #     count_registers += 1
        #     continue

        # restart the counter for person and location
        count_person = 1
        count_location = 1
        count_property = 1

        index_type = str(path[0][1][u'name']).replace("SAA","").title().replace(" ","").strip()
        index_id = str(path[1][1][u'id']).strip()
        outcome = outcome.replace('@@', index_type)

        ##########################################################################
        # Defining some auxiliary functions to support parsing the original data
        ##########################################################################
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
         # Gallois echtgenoot Anna Casijn, Erven Moise
         # Curiel alias Lopes Nunes Ramires, Erven Abraham Raphael van David
         # Bleu echtgenote Adriaan van der Steen, Erven Anna [le]
         # Kouw wed. Pieter Andriesz de Jong, Erven Maria
         # Verhoeven echtgenote Albertus de Heer, Erven Magtelt
         # Mom echtgenoot Maria de Heer, Samuel
         # Mispel echtgenoot Willemina Arents wed. Joris Mené, Erven Dirk [van]
         # Stouthals baron van Bleekhem, Franciscus Josephus
         # Kerkhoven baron van Exaerde echtgenoot Maria Elisabet Baldina van Zinserling, Johannes Franciscus

        getStructureLocation = lambda name, name_source, id, n: {'a': 'Location',
                                             'prefix': 'saaLocation',
                                             'id': id + 'l' + str(n),
                                             'street_name': name,
                                             'street_name_source': name_source }

        getStructureProperty = lambda propStr, id, n: {'a': 'Property',
                                             'prefix': 'saaProperty',
                                             'id': id + 'pr' + str(n),
                                             'propertyType': propStr['propertyType'].strip() if 'propertyType' in parsedDescr else '',
                                             'geoReference': propStr['geoReference'].strip() if 'geoReference' in parsedDescr else ''}

        getStructurePropertyNameDesc = lambda nameStr, id, n, m: {'a': 'PropertyNameReference',
                                             'prefix': 'saaPropertyNameRef',
                                             'id': id + 'pr' + str(n) + 'pnd' + str(m),
                                             'name': nameStr['name'] if 'name' in nameStr else '',
                                             'type': nameStr['type'] if 'type' in nameStr else '',
                                             'positionReference': nameStr['position'] if 'position' in nameStr else ''}

        def convertFraction(literal):
            try:
                if literal == '':
                    return ''
                numbers = literal.split('/')
                if len(numbers) > 1:
                    return int(numbers[0])/int(numbers[1])
                else:
                    return int(numbers[0])
            except:
                return ''
                print 'Error on converting fraction:', literal

        #####################################################################################
        # Assembling an auxiliary dictionary (structure) for the entities described in the record
        #####################################################################################
        # --------------------------
        # Structure for Person names
        # --------------------------
        structure1 = {}

        if 'Verkoper' in item:
            if type(item['Verkoper']) == list:
                structure1['hasSeller'] = []
                for text in item['Verkoper']:
                    temp = getStructurePerson(text, index_id, count_person)
                    temp['isInRecord'] = 'saaRec:'+index_id
                    structure1['hasSeller'] += [temp]
                    count_person += 1
            else:
                structure1['hasSeller'] = getStructurePerson(item['Verkoper'], index_id, count_person)
                structure1['hasSeller']['isInRecord'] = 'saaRec:'+index_id
                count_person += 1

        if 'Koper' in item:
            if type(item['Koper']) == list:
                structure1['hasBuyer'] = []
                for text in item['Koper']:
                    temp = getStructurePerson(text, index_id, count_person)
                    temp['isInRecord'] = 'saaRec:'+index_id
                    structure1['hasBuyer'] += [temp]
                    count_person += 1
            else:
                structure1['hasBuyer'] = getStructurePerson(item['Koper'], index_id, count_person)
                structure1['hasBuyer']['isInRecord'] = 'saaRec:'+index_id
                count_person += 1

        # --------------------------
        # Structure for Street names
        # --------------------------
        structure2 = {}
        if 'Straatnaam' in item:
            structure2['hasStreet'] = getStructureLocation(item['Straatnaam'], item['Straatnaam_in_bron'], index_id, count_location)
            structure2['hasStreet']['isInRecord'] = 'saaRec:' + index_id
            # count_location += 1

        # ----------------------------------------------
        # Structure for House or Property descriptions
        # ----------------------------------------------
        structure3 = {}
        if 'Omschrijving' in item:
            description = to_bytes(item['Omschrijving'])
            parsedDescr = processPropertyNames(description)
            # print parsedDescr
            if parsedDescr:
                count_names = 1
                if 'propertyType' in parsedDescr and parsedDescr['propertyType'] == '':
                    # print description
                    # Klein Kostverloren; Overdiep, hofstedeke, in de ban van Amstelveen strekkende bezuiden de ringsloot van de Diemermeer tot aan het Nieuwediep in het noordwesten
                    # De; Zwaan, De Hector, 5/35 opstal van een lijnbaan, bezuiden de Raampoort
                    # Het; Lust en Werk Oude Varken boven de deur, dubbel lakenraam, buiten de Raampoort
                    # De; Visser, De; Vergenoegen, Het Viskoper, buitenplaats, hoek Kuipersgang
                    # De; Visser, De Viskoper, buitenplaatsje, hoek Kuipersgang
                    # notHouseName.add(description)
                    pass
                elif 'legalFraction' in parsedDescr and parsedDescr['legalFraction'] != '':
                    if convertFraction(parsedDescr['legalFraction']) <= 1.0:
                        # this means only (part of) one property
                        # print 'One property:', parsedDescr
                        structure3['hasProperty'] = getStructureProperty(parsedDescr, index_id, count_property)
                        structure3['hasProperty']['isInRecord'] = 'saaRec:'+index_id
                        structure3['hasProperty']['hasNameDescription'] = []
                        for name in parsedDescr['names']:
                            # print name
                            structure3['hasProperty']['hasNameDescription'] += [getStructurePropertyNameDesc(name, index_id, count_property, count_names)]
                            count_names += 1
                        # if 'propertyType' in parsedDescr:
                        #     # if parsedDescr['propertyType'].find(' huis') is -1:
                        #         # print 'One property:', parsedDescr['propertyType']
                        #     # 2 pakhuizen en nog een huis en erven
                        #     # One property: {'legalFraction': '2/3', 'geoReference': 'het derde en vierde huis van de Prinsengracht', 'names': [{'position': 'in de gevel', 'type': 'gable_stone', 'name': 'De Hoge Boom'}], 'propertyType': ' 2/3 in 2 huizen en erven'}
                        #     # One property: {'legalFraction': '1/3', 'names': [{'name': 'Sint Andries'}], 'propertyType': ' 1/3 in een huis'}
                        #     # One property: {'legalFraction': '1', 'names': [{'name': 'De Rotgans'}, {'name': 'De Rode Leeuw'}], 'propertyType': ' buisschip'}
                        #     # One property: {'legalFraction': '1', 'names': [{'position': 'in de gevel', 'type': 'gable_stone', 'name': 'Danzig tussen beide huizen'}], 'propertyType': 'Danzig tussen beide huizen'}
                        # if 'geoReference' in parsedDescr:
                    else:
                        # this means several properties
                        structure3['hasProperty'] = []
                        # print 'Several properties:'
                        for name in parsedDescr['names']:
                            temp = getStructureProperty(parsedDescr, index_id, count_property)
                            temp['isInRecord'] = 'saaRec:'+index_id
                            # print name
                            temp['hasNameDescription'] = [getStructurePropertyNameDesc(name, index_id, count_property, count_names)]
                            # if 'type' in name:
                            #     # print 'has type', name['type']
                            #     temp['hasNameDescription'][-1]['type'] = name['type']
                            # if 'position' in name:
                            #     temp['hasNameDescription'][-1]['positionReference'] = name['position']
                            count_property += 1
                            # if 'propertyType' in parsedDescr:
                            #     # if parsedDescr['propertyType'].find(' huis') is -1:
                            #     #     print 'Several properties:', parsedDescr['propertyType']
                            #     temp['propertyType'] = parsedDescr['propertyType']
                            # if 'geoReference' in parsedDescr:
                            #     temp['geoReference'] = parsedDescr['geoReference']
                            structure3['hasProperty'] += [temp]


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
           # description = to_bytes(item['Omschrijving'])
            outcome += " \t\tsaaOnt:description \t \"{}\";\n".format(description)

        if 'urlScan' in item:
            if type(item['urlScan']) == list:
                for url in item['urlScan']:
                    outcome += " \t\tsaaOnt:urlScan \t <{}>;\n".format(url)
            else:
                outcome += " \t\tsaaOnt:urlScan \t <{}>;\n".format(item['urlScan'])

        inner = ''

        for master_key in structure1:
            elem = structure1[master_key]
            if type(elem) is dict:
                elem = [elem]
            if type(elem) is list and elem != [] and type(elem[0]) is dict:
                for e in elem:
                    (outerpart, innerpart) = processStructure2RDF(e, master_key)
                    outcome += outerpart
                    inner += innerpart

        for master_key in structure2:
            elem = structure2[master_key]
            if type(elem) is dict:
                elem = [elem]
            if type(elem) is list and elem != [] and type(elem[0]) is dict:
                for e in elem:
                    (outerpart, innerpart) = processStructure2RDF(e, master_key)
                    outcome += outerpart
                    inner += innerpart

        for master_key in structure3:
            elem = structure3[master_key]
            if type(elem) is dict:
                elem = [elem]
            if type(elem) is list and elem != [] and type(elem[0]) is dict:
                for e in elem:
                    (outerpart, innerpart) = processStructure2RDF(e, master_key)
                    outcome += outerpart
                    inner += innerpart
            # else:
            #     print elem

        outcome = outcome[:-2] + '.\n\n' + inner
        # print outcome

        count_registers += 1

        # if count_registers == stop:
        #     # print notHouseName
        #     exit(0)

        ###########################################################################
        # Writing the output file when <<out_size>> records is reached
        ###########################################################################
        if count_registers % out_size == 0:
            n_file = count_registers/out_size
            outcome += "\n } "  ## closes the named graph
            filename = output_file_name + 'N' + str(n_file) + '.trig'
            with codecs.open(filename, 'wb', encoding='utf-8') as outfile:
                outfile.write(to_unicode(outcome))
                outfile.close()
                print "file ", filename

            outcome = outcome_start
            if n_file == limit_files:
                print "Limit of files is reached"
                exit(0)

    ################################################################################
    # This piece of code only runs when an expection occurs => end of the input file

    except Exception as error:
        outcome += "\n } "  ## closes the named graph
        filename = output_file_name + 'N' + str((count_registers-1)//out_size+1) + '.trig'
        with codecs.open(filename, 'wb', encoding='utf-8') as outfile:
            outfile.write(to_unicode(outcome))
            outfile.close()
        print "file ", filename
        print 'Final List no date', list_no_date
        print '\n================ DONE =================='
        raise

    if count_registers % skip == 0:
        print >> sys.stderr, count_registers, '\r',
