from datetime import datetime
from pytz import timezone
import rethinkdb as r
import logging
import schedule
import time
from aclib.ops4app import Ops4app
import os,sys

def rdb_rowcount() :
    # -- Log d'activite
    ops.insertKPI(measurement='state', value=1.0, tags={'component' : 'python'})

    # --- Creation de l'objet pour idb
    idbitems = list()
    idbitem_ts = timezone('Europe/Paris').localize(datetime.now())
    idbitem_entete = dict()
    idbitem_entete['time'] = idbitem_ts.isoformat()
    idbitem_tags = dict(dict(component='rethinkdb'))

    # --- Lecture de rdb
    if ops.rdb :
        for f_base in str(ops.config['bases.to.monitor']).split(";") :
            if f_base != "" :
                try :
                    liste_tables = r.db(f_base).table_list().run(ops.rdb)
                    if len(liste_tables) > 0 :
                        for f_table in liste_tables :
                            tmpCount = r.db(f_base).table(f_table).count().run(ops.rdb)
                            idbitem                       = dict(idbitem_entete)  # attention : le dict tag se passe par pointeur, meme si le dict idbitem est instancie a chaque fois
                            idbitem['tags']               = dict(idbitem_tags)
                            idbitem['fields']             = dict()
                            #idbitem['measurement']        = 'rowcount'
                            idbitem['measurement']        = 'number'
                            idbitem['tags']['number_of']  = 'rdb_records'
                            idbitem['tags']['base']       = f_base
                            idbitem['tags']['table']      = f_table
                            idbitem['fields']['value']    = int(tmpCount)
                            idbitems.append(idbitem)
                except Exception as e :
                    logging.warning("Exception sur rowcount rdb : %s" % str(e))
                    del ops.rdb # Pour que la connexion soit reinitialisee

    # --- Injection dans influxDB
    if len(idbitems) > 0 :
        ops.insertKPIs(liste_objets=idbitems)

    # -- Log d'activite
    time.sleep(1)  # On attend 1s pour que cela donne une duree >1 pour l'affichage grafana
    ops.insertKPI(measurement='state', value=0.0, tags={'component' : 'python'})
    logging.info("RDB Monitor executed at %s" % idbitem_ts.ctime())


if __name__ == '__main__':
    logging.addLevelName(logging.DEBUG-2, 'DEBUG_DETAILS') # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.basicConfig(level=logging.INFO, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(levelname)s|%(asctime)s | %(message)s")  # %(thread)d %(funcName)s L%(lineno)d
    logging.getLogger("requests").setLevel(logging.WARNING) # On desactive les logs pour la librairie requests
    logging.getLogger("schedule").setLevel(logging.WARNING) # On desactive les logs pour la librairie schedule

    # -- PATH
    from pathlib import Path
    if 'daem' in Path.cwd().parts[-1] :
        os.chdir("..")
    elif 'github' in Path.cwd().parts[-1] :
        os.chdir("./hdh")
    sys.path.append('./')
    logging.info("Starting from %s" % str(os.getcwd()))

    # -- Lancement
    ops = Ops4app(appli_uname='deamon.monitor.rdb')
    rdb_rowcount()  # 1 lancement immediat
    schedule.every(ops.config['period.in.minutes']).minutes.do(rdb_rowcount)
    while True:
        schedule.run_pending()
        time.sleep(5)
