from datetime import datetime
from pytz import timezone

import pandas as pd
import logging
import schedule
import time
import requests
import json
from aclib.ops4app import Ops4app
from aclib.func4strings import Func4strings as f4s
import os,sys


my_homwiz_contact_previous_change_ts = dict() # buffer temporel detaille : contient le ts complet suite a requete de l'histo du capteur : contact Uname : "2015-10-23 12:06:29"

def homwiz_fetch() :
    # -- Log d'activite
    ops.insertKPI(measurement='state', value=1.0, tags={'component' : 'python'})

    my_homwiz_tz       = timezone('Europe/Paris')
    # ------ recup du statut actuel aupres de la box
    logging.log(logging.DEBUG-2, "Homwiz - Lancement request URL")
    try :
        tmpResp          = requests.get(ops.config['homwiz.url.allsensors'], timeout=10)  # Example de reponse = Parsing de : {"status": "ok", "version": "3.32", "request": {"route": "/get-sensors" }, "response": {"preset":0,"time":"2015-10-16 17:31","switches":[{"id":0,"name":"PriseLampeSalon","type":"switch","status":"off","favorite":"no"},{"id":1,"name":"Air","type":"switch","status":"off","favorite":"no"}],"uvmeters":[],"windmeters":[],"rainmeters":[],"thermometers":[{"id":0,"name":"LivingRoom","code":"10320649","model":1,"lowBattery":"no","version":2.31,"te":14.2,"hu":57,"te+":14.6,"te+t":"00:00","te-":12.1,"te-t":"10:02","hu+":59,"hu+t":"12:41","hu-":54,"hu-t":"09:36","outside":"yes","favorite":"no"}],"weatherdisplays":[], "energymeters": [], "energylinks": [], "heatlinks": [], "hues": [], "scenes": [], "kakusensors": [{"id":1,"name":"porte1","status":"no","type":"contact","favorite":"no","timestamp":"12:02","cameraid":null},{"id":2,"name":"porte2","status":"no","type":"contact","favorite":"no","timestamp":"14:20","cameraid":null}], "cameras": [{"id":0,"model":4,"name":"Cam1","username":"fosutil","password":"K%2Bdzn6H92G-","ip":"192.168.75.112","port":"88","mode":0,"url":{"path":"cgi-bin/CGIProxy.fcgi","query":"cmd=snapPicture2&usr=fosutil&pwd=K%2Bdzn6H92G-","auth":""},"presets":[]}]}}
        my_homwiz_status = json.loads(tmpResp.content.decode(encoding='utf-8'))
        my_homwiz_tz     = timezone('Europe/Paris')
    except Exception as e :
        logging.error("Exception a la recup Homwiz : %s" % str(e))
        my_homwiz_status  = None

    # -- Parsing de la reponse globale
    logging.log(logging.DEBUG-2, "Homwiz - Parsing answer")
    if my_homwiz_status is not None :
        # -- Creation des variables vides
        logging.log(logging.DEBUG-2, "Loop - Ending")
        my_json4idb= list()           # pour IDB, on va inserer une liste d'objets avec le meme timestamp et les memes flags, mais des variables differentes
        # -- Parsing
        if my_homwiz_status.get('status','') == 'ok' and 'time' in my_homwiz_status.get('response', {}) :
            logging.log(logging.DEBUG-2, "Homwiz - Parsing entete")
            # ----- Parsing Entete
            idbitemEntete = dict()  # On creera les items IDB a partir de cette entete qui contiendra les valeurs communes (dont le timestamp)
            idbitem_tags = dict(dict(lieu='treffort', source='homwiz'))

            try:
                my_homwiz_ts = datetime.strptime(my_homwiz_status['response']['time'], '%Y-%m-%d %H:%M')      # 2015-10-16 17:31
            except:
                logging.warning("Impossible de caster le timestamp depuis la response web, remplace par now()")
                my_homwiz_ts = datetime.now()
            idbitemEntete['time']            = my_homwiz_tz.localize(my_homwiz_ts).isoformat()    # tmpDatetime a ete parse pour le RDB

            # ----- Parsing des champs dans la reponse
            logging.log(logging.DEBUG-2, "Homwiz - Parsing des champs")
            for key in my_homwiz_status['response'] :
                try :
                    # ---- On a un sensor temp + humidite : # {'te-': 14.8, 'hu+': 58, 'te+t': '00:01', 'hu+t': '00:01', 'te+': 15.2, 'te-t': '02:23', 'model': 1, 'version': 2.31, 'code': '10320649', 'outside': 'yes', 'hu': 57, 'id': 0, 'hu-t': '02:54', 'name': 'LivingRoom', 'hu-': 57, 'te': 14.8, 'lowBattery': 'no', 'favorite': 'no'}
                    if key == 'thermometers' and type(my_homwiz_status['response'].get(key)) == list :
                        logging.log(logging.DEBUG-2, "Homwiz - Parsing thermo")
                        for item in my_homwiz_status['response'].get(key) :

                            # ---- TEMPERATURE -----
                            SensorName  = f4s.strCleanSanitize(str(item.get('name', 'Noname')), phtmlunescape=True, pLignesTabsGuillemets=True,pNormalizeASCII=True, pEnleveSignesSpeciaux=True, pLettreDigitPointTiret=True, pBagOfWords=False)

                            #SensorUName = 'Temp.' + str(item.get('id', 0)) + '.' + SensorName.replace(" ","",-1) + '.value'
                            if 'te' in item :
                                SensorValue                   = float(item.get('te', 100.0))
                                idbitem                       = dict(idbitemEntete)             # attention : le dict tag se passe par pointeur, meme si le dict idbitem est instancie a chaque fois
                                idbitem['tags']               = dict(idbitem_tags)
                                idbitem['fields']             = dict()
                                idbitem['measurement']        = 'temperature'
                                idbitem['tags']['nom']        = SensorName
                                idbitem['fields']['value']    = SensorValue
                                my_json4idb.append(idbitem)

                            # ---- HUMIDITE -----
                            SensorName  = f4s.strCleanSanitize(str(item.get('name', 'Noname')), phtmlunescape=True, pLignesTabsGuillemets=True,pNormalizeASCII=True, pEnleveSignesSpeciaux=True, pLettreDigitPointTiret=True, pBagOfWords=False)
                            #SensorUName = 'Hum.' + str(item.get('id', 0)) + '.' + SensorName.replace(" ","",-1) + '.value'
                            if 'hu' in item :
                                SensorValue                   = float(item.get('hu', 100)) # hu est un int dans homwiz, caste en float
                                idbitem                       = dict(idbitemEntete)             # attention : le dict tag se passe par pointeur, meme si le dict idbitem est instancie a chaque fois
                                idbitem['tags']               = dict(idbitem_tags)
                                idbitem['fields']             = dict()
                                idbitem['measurement']        = 'humidity'
                                idbitem['tags']['nom']        = SensorName
                                idbitem['fields']['value']    = SensorValue
                                my_json4idb.append(idbitem)

                    # ---- sensor energy : { "id": 0, "name": "ConsoElec", "key": "0", "code": "2009013743", "po": 510, "dayTotal": 2.62, "po+": 3310, "po+t": "20:23", "po-": 0, "po-t": "0:00", "lowBattery": "no", "favorite": "no" }
                    if key == 'energymeters' and type(my_homwiz_status['response'].get(key)) == list :
                        logging.log(logging.DEBUG-2, "Homwiz - Parsing energy")
                        for item in my_homwiz_status['response'].get(key) :

                            # ---- ELECTRICITY PUISSANCE -----
                            SensorName  = f4s.strCleanSanitize(str(item.get('name', 'Noname')), phtmlunescape=True, pLignesTabsGuillemets=True,pNormalizeASCII=True, pEnleveSignesSpeciaux=True, pLettreDigitPointTiret=True, pBagOfWords=False)
                            #SensorUName = 'NRJ.' + str(item.get('id', 0)) + '.' + SensorName.replace(" ","",-1) + '.value'
                            if 'po' in item :
                                SensorValue                   = float(item.get('po', 0.0))
                                idbitem                       = dict(idbitemEntete)             # attention : le dict tag se passe par pointeur, meme si le dict idbitem est instancie a chaque fois
                                idbitem['tags']               = dict(idbitem_tags)
                                idbitem['fields']             = dict()
                                idbitem['measurement']        = 'elecpower'
                                idbitem['tags']['nom']        = SensorName
                                idbitem['fields']['value']    = SensorValue
                                my_json4idb.append(idbitem)

                            # ---- ELECTRICITY CUMUL (que RDB) -----
                            #SensorUName = 'NRJ.' + str(item.get('id', 0)) + '.' + custof.strSanitizeNoLower(str(item.get('name', 'NomParDefaut'))).replace(" ","",-1) + '.cumuljour'

                    # ---- On a un sensor switch : {"id":0,"name":"PriseLampeSalon","type":"switch","status":"off","favorite":"no"}
                    elif key == 'switches' and type(my_homwiz_status['response'].get(key)) == list :
                        logging.log(logging.DEBUG-2, "Homwiz - Parsing switches")
                        for item in my_homwiz_status['response'].get(key) :

                            # ---- SWITCH on/off prises elec -----
                            SensorName  = f4s.strCleanSanitize(str(item.get('name', 'Noname')), phtmlunescape=True, pLignesTabsGuillemets=True,pNormalizeASCII=True, pEnleveSignesSpeciaux=True, pLettreDigitPointTiret=True, pBagOfWords=False)
                            SensorUName = 'Switch.' + str(item.get('id', 0)) + '.' + SensorName.replace(" ","",-1) + '.value'
                            if 'status' in item :
                                SensorValue = 2
                                if   item.get('status','') == 'on' :    SensorValue = int(1)   # 1 = On
                                elif item.get('status','') == 'off' :   SensorValue = int(0)   # 0 = Off
                                else :                                  logging.error("Status non reconnu pour %s" % SensorUName)
                                if SensorValue < 2 :
                                    idbitem                       = dict(idbitemEntete)             # attention : le dict tag se passe par pointeur, meme si le dict idbitem est instancie a chaque fois
                                    idbitem['tags']               = dict(idbitem_tags)
                                    idbitem['fields']             = dict()
                                    idbitem['measurement']        = 'switch'
                                    idbitem['tags']['nom']        = SensorName
                                    idbitem['fields']['value']    = SensorValue
                                    my_json4idb.append(idbitem)

                    # ---- On a un sensor contact "id":1,"name":"porte1","status":"no","type":"contact","favorite":"no","timestamp":"12:02","cameraid":null}
                    elif key == 'kakusensors' and type(my_homwiz_status['response'].get(key)) == list :
                        logging.log(logging.DEBUG-2, "Homwiz - Parsing detecteurs")
                        for item in my_homwiz_status['response'].get(key) :
                            # -- contact avec status et timestamp
                            logging.log(logging.DEBUG-2, "Homwiz - Parsing contacts")

                            if item.get('type', '') == 'contact' :
                                # ----- CONTACT PORTE : STATUS & TIMESTAMP -----
                                SensorName    = f4s.strCleanSanitize(str(item.get('name', 'Noname')), phtmlunescape=True, pLignesTabsGuillemets=True,pNormalizeASCII=True, pEnleveSignesSpeciaux=True, pLettreDigitPointTiret=True, pBagOfWords=False)
                                SensorUName   = 'Contact.' + str(item.get('id', 0)) + '.' + SensorName.replace(" ","",-1) + '.value'
                                #SensorUNameTS = 'Contact.' + str(item.get('id', 0)) + '.' + SensorName.replace(" ","",-1) + '.ts'
                                if 'status' in item :
                                    SensorValue = int(2)
                                    if   item.get('status','') == 'no' :    SensorValue = int(0)   # 0 = close
                                    elif item.get('status','') == 'yes' :   SensorValue = int(1)   # 1 = open
                                    else :
                                        logging.error("Status non reconnu pour %s" % SensorUName)

                                    if SensorValue < 2 :
                                        # on pousse le statut actuel
                                        idbitem                       = dict(idbitemEntete)             # attention : le dict tag se passe par pointeur, meme si le dict idbitem est instancie a chaque fois
                                        idbitem['tags']               = dict(idbitem_tags)
                                        idbitem['fields']             = dict()
                                        idbitem['measurement']        = 'contact'
                                        idbitem['tags']['nom']        = SensorName
                                        idbitem['fields']['value']    = SensorValue
                                        my_json4idb.append(idbitem)

                                        # --- On regarde le TS du dernier changement d'etat qui est remonte avec le status actuel
                                        if 'timestamp' in item :
                                            SensorLastChangeTime = str(item.get('timestamp', ''))   # De la forme "HH:MM" donc pas vraiment un timestamp, ca fonctionne sur 1 jour, avec des 0 si 1 seul chiffre
                                            # -- Si le dernier "HH:MM" est cette meme minute (pb si plusieurs change dans 1 meme min mieux vaut attendre) ou s'il est le meme que le dernier TS de changement en RAM alors pas besoin de recup l'historique
                                            if my_homwiz_contact_previous_change_ts.get(SensorUName,None) is None :
                                                my_homwiz_contact_previous_change_ts[SensorUName] = datetime(2000,1,1,0,0,0)

                                            # -- S'il y a eu un changement dans le passe (pas dans cette meme minute) et qu'il n'est pas deja dans la RAM (historique deja recup boucle precedente)
                                            if SensorLastChangeTime != idbitemEntete['time'][11:16] and SensorLastChangeTime != my_homwiz_contact_previous_change_ts[SensorUName].isoformat()[11:16] :  # Forme : '2016-05-09T13:11:00'
                                                try :
                                                    tmpURL  = ops.config['homwiz.url.contactsensor.history.prefixe'] + str(item.get('id', '0')) + ops.config['homwiz.url.contactsensor.history.suffixe']
                                                    tmpResp = requests.get(tmpURL, timeout=10)
                                                    contactHistory = json.loads(tmpResp.content.decode(encoding='utf-8'))
                                                except Exception as e :
                                                    logging.error("Exception a la recup historique capteur : %s" % str(e))
                                                else :
                                                    # Parsing de : {"status": "ok", "version": "3.32", "request": {"route": "/kks" }, "response": [{ "t": "2015-10-23 12:06:29", "status": "no"}, {...
                                                    df = pd.DataFrame(contactHistory['response'])
                                                    df.sort_values(by='t', ascending=True)       # on demarre avec la plus ancienne
                                                    for i,row in df.iterrows() :
                                                        tmpTS = datetime.strptime(row['t'], '%Y-%m-%d %H:%M:%S')   # Le TS du changement d'etat dans l'historique
                                                        tmpNbSecondsBack = (my_homwiz_ts-tmpTS).total_seconds()            # Les secondes entre ce TS et le TS de la requete

                                                        # - Si le TS n'est pas trop vieux ET s'il est plus recent que celui en RAM
                                                        if tmpNbSecondsBack < 5*60 and tmpTS > my_homwiz_contact_previous_change_ts[SensorUName] :
                                                            my_homwiz_contact_previous_change_ts[SensorUName] = tmpTS

                                                            # -- On parse et logue le TS
                                                            histoValue = int(2)
                                                            if   row['status'] == 'no' :    histoValue = int(0)   # 0 = close
                                                            elif row['status'] == 'yes' :   histoValue = int(1)   # 1 = open
                                                            else :                          logging.error("Status historique non reconnu pour %s" % SensorUName)

                                                            # -- Insertion d'un etat passe dans idb
                                                            idbitem2                      = dict(idbitemEntete)             # attention : le dict tag se passe par pointeur, meme si le dict idbitem est instancie a chaque fois
                                                            idbitem2['tags']              = dict(idbitem_tags)
                                                            idbitem2['fields']            = dict()
                                                            idbitem2['time']              = my_homwiz_tz.localize(tmpTS).isoformat()     # On ecrase la date de l'entete qui pointe sur le moment present
                                                            idbitem2['measurement']       = 'contact'
                                                            idbitem2['tags']['nom']       = SensorName
                                                            idbitem2['fields']['value']   = histoValue
                                                            my_json4idb.append(idbitem2)

                            # --- LIGHT/CREPUSCULE : le capteur de crepuscule enregistre comme mesure de lumiere
                            if item.get('type', '') == 'light' :
                                logging.log(logging.DEBUG-2, "Homwiz - Parsing light")
                                # ----- LUMIERE : STATUS & TIMESTAMP -----
                                # on pourrait remplacer ce qui suit par celui des contact qui est maintenant plus complet
                                SensorName    = f4s.strCleanSanitize(str(item.get('name', 'Noname')), phtmlunescape=True, pLignesTabsGuillemets=True,pNormalizeASCII=True, pEnleveSignesSpeciaux=True, pLettreDigitPointTiret=True, pBagOfWords=False)
                                SensorUName   = 'Luminosite.' + str(item.get('id', 0)) + '.' + SensorName.replace(" ","",-1) + '.value'
                                #SensorUNameTS = 'Luminosite.' + str(item.get('id', 0)) + '.' + SensorName.replace(" ","",-1) + '.ts'
                                if 'status' in item :
                                    SensorValue = int(2)
                                    if   item.get('status','') == 'yes' :    SensorValue = int(0)   # 'yes' = la nuit est la, on met 0
                                    elif item.get('status','') == 'no' :     SensorValue = int(1)   # 'no' = jour = lumiere, on met  valeur = 1
                                    else :                                  logging.error("Status [%s] non reconnu pour %s" % (item.get('status',''),SensorUName))
                                    if SensorValue < 2 :
                                        # -- On logge la valeur actuelle avec le TS actuel dans rdb & idb
                                        idbitem                       = dict(idbitemEntete)             # attention : le dict tag se passe par pointeur, meme si le dict idbitem est instancie a chaque fois
                                        idbitem['tags']               = dict(idbitem_tags)
                                        idbitem['fields']             = dict()
                                        idbitem['measurement']        = 'lumiere'
                                        idbitem['tags']['nom']        = SensorName
                                        idbitem['fields']['value']    = SensorValue
                                        my_json4idb.append(idbitem)

                                        # --- On regarde le TS du dernier changement d'etat qui est remonte avec le status actuel
                                        if 'timestamp' in item :
                                            SensorLastChangeTime = str(item.get('timestamp', ''))   # De la forme "HH:MM" donc pas vraiment un timestamp, ca fonctionne sur 1 jour, avec des 0 si 1 seul chiffre

                                            if SensorUName not in my_homwiz_contact_previous_change_ts.keys() :
                                                # -- nouveau capteur ou le script vient d'être lance : on ne rajoute pas d'historique
                                                my_homwiz_contact_previous_change_ts[SensorUName] = SensorLastChangeTime  # On stocke un chg d'etat maintenant

                                            elif my_homwiz_contact_previous_change_ts.get(SensorUName, '') == SensorLastChangeTime :
                                                pass  # ---- Le contact n'a pas change d'etat depuis la la derniere recup de status : RAS, on a deja mis l'etat actuel avec son TS

                                            else :
                                                # ---- Le contact a change d'etat depuis la derniere fois : on calcule le TS du changement et on va l'inserer dans le IDB, normalement ca doit tomber entre le dernier log et celui ci
                                                my_homwiz_contact_previous_change_ts[SensorUName] = SensorLastChangeTime  # On stocke un chg d'etat maintenant
                                                if len(SensorLastChangeTime) == 5 :
                                                    tmpH = int(SensorLastChangeTime[:2])
                                                    tmpM = int(SensorLastChangeTime[-2:])
                                                    datetChangement = None
                                                    if (tmpH*60+tmpM) > (my_homwiz_ts.hour*60+my_homwiz_ts.minute) :
                                                        # -- HH:MM indique une heure superieure a now donc remonte tres pb au jour precedent : on fixe la date du changement ce jour a 00:00
                                                        datetChangement = datetime(year=my_homwiz_ts.year, month=my_homwiz_ts.month, day=my_homwiz_ts.day, hour=0, minute=0, second=0)
                                                    elif tmpH < my_homwiz_ts.hour or tmpM < my_homwiz_ts.minute :
                                                        # -- HH:MM indique une heure anterieure a maintenant ET PAS EGALE
                                                        datetChangement = datetime(year=my_homwiz_ts.year, month=my_homwiz_ts.month, day=my_homwiz_ts.day, hour=tmpH, minute=tmpM, second=0)
                                                    elif tmpH == my_homwiz_ts.hour and tmpM == my_homwiz_ts.minute :
                                                        # -- HH:MM indique l'heure exacte de la collecte : on diminue d'une min pour ne pas manquer l'evenement
                                                        tmpM -= 1
                                                        if tmpM<0 :
                                                            tmpM=0
                                                            tmpH -=tmpH
                                                            if tmpH<0 :
                                                                tmpH=0
                                                        datetChangement = datetime(year=my_homwiz_ts.year, month=my_homwiz_ts.month, day=my_homwiz_ts.day, hour=tmpH, minute=tmpM, second=0)

                                                    if datetChangement is not None :
                                                        prevSensorValue = int(0)    # pour que le type soit bien int dans IDB
                                                        if SensorValue == 0 :
                                                            prevSensorValue = int(1)

                                                        # -- Insertion d'un etat passe dans idb
                                                        idbitem2                      = dict(idbitemEntete)             # attention : le dict tag se passe par pointeur, meme si le dict idbitem est instancie a chaque fois
                                                        idbitem2['tags']              = dict(idbitem_tags)
                                                        idbitem2['fields']            = dict()
                                                        idbitem2['time']              = my_homwiz_tz.localize(datetChangement).isoformat()     # On ecrase la date de l'entete qui pointe sur le moment present
                                                        idbitem2['measurement']       = 'lumiere'
                                                        idbitem2['tags']['nom']       = SensorName
                                                        idbitem2['fields']['value']   = prevSensorValue
                                                        my_json4idb.append(idbitem2)
                except:
                    logging.error("Exception dans le parsing de homwiz, key=%s" % key)
        logging.log(logging.DEBUG-2, "IDB insert lancement")
        try :
            # --- Injection dans IDB
            if len(my_json4idb) > 0 :
                ops.insertInIDB(db_name=ops.config['idb.database.sensors'], liste_objets=my_json4idb)
                logging.info("Insertion sensors homwiz dans IDB : %d measures" % len(my_json4idb))
        except:
            logging.error("Exception durant l'injection dans IDB")
        logging.log(logging.DEBUG-2, "IDB insert closing")

    # -- Log d'activite
    time.sleep(1)  # On attend 1s pour que cela donne une duree >1 pour l'affichage grafana
    ops.insertKPI(measurement='state', value=0.0, tags={'component' : 'python'})
    #logging.info("RDB rowcounts inserted in IDB at %s" % idbitem_ts.ctime())

if __name__ == '__main__':
    logging.addLevelName(logging.DEBUG-2, 'DEBUG_DETAILS') # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.basicConfig(level=logging.INFO, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(levelname)s|%(asctime)s | %(message)s")  # %(thread)d %(funcName)s L%(lineno)d
    logging.getLogger("requests").setLevel(logging.WARNING) # On desactive les logs pour la librairie requests
    logging.getLogger("schedule").setLevel(logging.WARNING) # On desactive les logs pour la librairie schedule

    # -- PATH
    from pathlib import Path
    if 'fetch' in Path.cwd().parts[-1] :
        os.chdir("..")
    elif 'github' in Path.cwd().parts[-1] :
        os.chdir("./hdh")
    sys.path.append('./')
    logging.info("Starting from %s" % str(os.getcwd()))

    # -- Lancement
    ops = Ops4app(appli_uname="fetcher.homwiz")
    homwiz_fetch()  # 1 lancement immediat
    schedule.every(ops.config['period.in.minutes']).minutes.do(homwiz_fetch)
    while True:
        schedule.run_pending()
        time.sleep(5)
