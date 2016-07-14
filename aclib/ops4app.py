# coding: utf8
import rethinkdb as r
from influxdb import InfluxDBClient
import logging
import time
from datetime import datetime
from pytz import timezone
from pathlib import Path
import threading
import pytoml

# --- RDB en mode exclu (1 instance de ops par thread : 1 conn rdb ) ou avec des lock/release car 1 ptr rdb ne se partage pas quand il est en cours de query
class Ops4app :
    def __init__(self, appli_uname='default_app_uname') :
        self._my_appli_uname        = appli_uname
        self._my_rdb                = None
        self._my_rdb_lock           = threading.Lock()
        self._my_config_file        = ''
        self._my_config_from_file   = dict(id=self._my_appli_uname)
        self._my_config             = dict(id=self._my_appli_uname)
        self._isOK                  = True
        o4a_defaut = dict({ "id" : "ops4app", "rdb.ip" : "127.0.0.1", "rdb.port" : 28015, "rdb.base" : "hdh",
                            "idb.ip" : "127.0.0.1", "idb.port" : 8086, "idb.login" : "user", "idb.pwd" : "user",
                            "table_cfg_in_rdb" : "config", "table_logs_in_rdb" : "logs", "kpi_db_in_idb" : "hdhmon"})

        # # -- JSON : On regarde si fichier de conf, si oui on charge la partie pour ops4app & la partie pour l'appli du parametre
        # fichier_path = Path('../hdh.cfg.json')
        # try:
        #     if fichier_path.exists() :
        #         fichier_obj = fichier_path.open(mode='r', encoding='utf-8', errors='backslashreplace')
        #         fichier_json = json.load(fichier_obj)
        #         fichier_obj.close()
        #         self._my_config_file = fichier_path.as_posix()
        #         for i in fichier_json :
        #             if i.get('id', '') == 'ops4app' :
        #                 o4a_defaut.update(i)
        #             elif i.get('id', '') == self._my_appli_uname :
        #                 self._my_config_from_file = dict(i)
        # except Exception as e:
        #     logging.error("Error reading config file %s | %s" % (str(fichier_path), str(e)))

        # -- TOML : On regarde si fichier de conf, si oui on charge la partie pour ops4app & la partie pour l'appli du parametre
        fichier_path = Path('../hdh.cfg.toml')
        try:
            if fichier_path.exists() :
                fichier_obj = fichier_path.open(mode='r', encoding='utf-8', errors='backslashreplace')
                fichier_toml = pytoml.load(fichier_obj)
                fichier_obj.close()
                self._my_config_file = fichier_path.as_posix()
                for nom in fichier_toml.keys() :
                    if nom == 'ops4app' :
                        o4a_defaut.update(fichier_toml[nom])
                    elif nom == self._my_appli_uname :
                        self._my_config_from_file = dict(fichier_toml[nom])
        except Exception as e:
            logging.error("Error reading config file %s | %s" % (str(fichier_path), str(e)))

        # -- ops4app : Recup config defaut ou override fichier
        self._my_rdb_IP                 = o4a_defaut.get('rdb.ip')
        self._my_rdb_port               = o4a_defaut.get('rdb.port')
        self._my_rdb_base               = o4a_defaut.get('rdb.base')
        self._my_config_table_in_rdb    = o4a_defaut.get('table_cfg_in_rdb')

        # -- Connection a RDB et verif connexion, database, table et config de ops4app
        if self.rdb is not None:
            try :
                # La base
                if self._my_rdb_base not in r.db_list().run(self.rdb) :
                    r.db_create(self._my_rdb_base).run(self.rdb)
                    logging.info("Base %s not found in rdb %s, Creation" % (self._my_rdb_base, self._my_rdb_IP))
                self.rdb.use(self._my_rdb_base)

                # La table de config
                if self._my_config_table_in_rdb not in r.table_list().run(self.rdb) :
                    r.db(self._my_rdb_base).table_create(self._my_config_table_in_rdb).run(self.rdb)
                    logging.info("Table %s not found in rdb %s %s, Creation" % (self._my_config_table_in_rdb, self._my_rdb_IP,self._my_rdb_base))

                # La config de ops4app
                confOps4app = r.table(self._my_config_table_in_rdb).get('ops4app').run(self.rdb)
                if confOps4app is not None :
                    o4a_defaut.update(confOps4app)
                else:
                    # -- ops4app inconnu dans RDB : update RDB depuis le script
                    r.db(self._my_rdb_base).table(self._my_config_table_in_rdb).insert(o4a_defaut, conflict='replace').run(self.rdb)
                    logging.info("Config ops4app not found in rdb, Pushed default")

            except Exception as e :
                logging.critical("Problem d'acces a RDB : %s" % str(e))
                self._isOK = False

        # -- Ici on a la config de ops4app qui est OK : overrides = script -> fichier json -> rdb
        self._my_logs_table_in_rdb  = o4a_defaut.get('table_logs_in_rdb')
        self._my_kpi_db_in_idb      = o4a_defaut.get('kpi_db_in_idb')
        self._my_idb_IP             = o4a_defaut.get('idb.ip')
        self._my_idb_port           = o4a_defaut.get('idb.port')
        self._my_idb_log            = o4a_defaut.get('idb.login')
        self._my_idb_pwd            = o4a_defaut.get('idb.pwd')

        # -- Recup de la config pour l'appli
        if self.rdb is not None :
            confapp = r.table(self._my_config_table_in_rdb).get(self._my_appli_uname).run(self.rdb)
            if confapp is not None :
                self._my_config.update(confapp)
            else:
                # -- appli inconnue dans RDB : update RDB depuis le script
                self._my_config.update(self._my_config_from_file)
                self._my_config.update({"id" : self._my_appli_uname})
                r.db(self._my_rdb_base).table(self._my_config_table_in_rdb).insert(self._my_config, conflict='replace').run(self.rdb)
                logging.info("Config %s not found in rdb, Pushed default (%d fields)" % (self._my_appli_uname, len(self._my_config)))
    def isOK(self):
        return self._isOK

    # --- CONFIG Recuperation de la config depuis rethinkDB : dans le pire des cas, liste vide
    @property
    def config(self):
        # TODO : Faire une relecture periodique de la config depuis rethinkDB
        return self._my_config
    @config.setter
    def config(self, p):
        pass  # on ne fait rien en ecriture
    @config.deleter
    def config(self):
        self._my_config = None  # Cela forcera un reload a la prochaine demande en acces lecture

    # --- LOGS
    def addlog(self, level='default', message='default_message'):
        if self.rdb is not None:
            localtz = timezone('Europe/Paris')
            ts_created = r.iso8601(localtz.localize(datetime.now()).isoformat())
            logjson = {'ts_created': ts_created,
                       'appli_uname': self._my_appli_uname,
                       'level': level,
                       'message': message
                       }
            try:
                r.db(self._my_rdb_base).table(self._my_logs_table_in_rdb).insert(logjson, conflict='update').run(self.rdb)
            except Exception as e :
                logging.error("Log rethinkdb except : %s | %s" % (message,str(e)))
        else:
            logging.error("Log impossible rdb conn None :%s " % message)

    # --- RDB monothread : 1 instance par thread, PERSISTANCE DE LA CONNEXION DONC pas possible d'avoir connexion en thread safe Acces a la DB en get / set / delete
    @property
    def rdb(self):
        #with self._my_rdb_MT_lock :
        if self._my_rdb is None :
            nb_reconnect = 3
            while nb_reconnect > 0 :
                try :
                    self._my_rdb = r.connect(host=self._my_rdb_IP, port=self._my_rdb_port, db=self._my_rdb_base, auth_key="", timeout=10)
                    self._my_rdb.use(self._my_rdb_base)
                    nb_reconnect = 0
                    self._isOK = True
                except Exception as e :
                    logging.error("Echec connexion a RDB : %s" % str(e))
                    self._my_rdb = None
                    self._isOK = False
                    nb_reconnect -= 1
                    if nb_reconnect > 0 :
                        logging.error("Sleep avant reconnexion RDB")
                        time.sleep(20)
        return self._my_rdb
    @rdb.setter  # Acces a la db en set : dans tous les cas, on referme la connexion et on met None -> appel du delete
    def rdb(self, p):
        # with self._my_rdb_MT_lock :
        logging.warning("Tentative d'affecter une valeur au pointeur RDB : %s" % type(p))
        del self.rdb  # logging.log(logging.DEBUG-2, "Deconnexion de la DB via assignement=%s" % str(type(p)) )
    @rdb.deleter  # On referme la connexion a la db
    def rdb(self):
        # with self._my_rdb_MT_lock :
        if self._my_rdb is not None :
            try :
                self._my_rdb.close()
            except Exception as e :
                logging.warning("Erreur durant deconnexion RDB : %s" % str(e))
            self._my_rdb = None

    # --- RDB multithread / multi instances : on take et on release, sinon on attend...
    def rdb_get_lock(self):
        if self.rdb is not None :
            self._my_rdb_lock.acquire(blocking=True,timeout=60)
        return self.rdb
    def rdb_release(self):
        self._my_rdb_lock.release()

    # --- INFLUXDB : Injection dans InfluxDB tags de la forme dict({ 'cle' : 'valeur' }), c'est du REST, pas de persistance
    def insertKPI(self, measurement, value, tags=None):
        localtz = timezone('Europe/Paris')
        idbsjon = dict()
        idbsjon['time'] = localtz.localize(datetime.now()).isoformat()
        idbsjon['measurement'] = measurement
        idbsjon['fields'] = dict()
        idbsjon['fields']['value'] = value
        idbsjon['tags'] = dict() if tags is None else dict(tags)
        idbsjon['tags']['app_uname'] = self._my_appli_uname
        try :
            client = InfluxDBClient(host=self._my_idb_IP, port=self._my_idb_port, username=self._my_idb_log, password=self._my_idb_pwd, database=self._my_kpi_db_in_idb, timeout=20)
            client.write_points(list([idbsjon]))
        except Exception as e :
            logging.error("Echec insert KPI dans IDB : %s" % str(e))
    def insertKPIs(self, liste_objets=None):
        if liste_objets is not None :
            liste2 = list(liste_objets)
            try :
                for item in liste2 :
                    item['tags']['app_uname'] = self._my_appli_uname
                client = InfluxDBClient(host=self._my_idb_IP, port=self._my_idb_port, username=self._my_idb_log, password=self._my_idb_pwd, database=self._my_kpi_db_in_idb, timeout=20)
                client.write_points(liste2)
            except Exception as e :
                logging.error("Echec insert KPI massif dans IDB : %s" % str(e))
    def insertInIDB(self, db_name='', liste_objets=None):
        if liste_objets is not None :
            try :
                client = InfluxDBClient(host=self._my_idb_IP, port=self._my_idb_port, username=self._my_idb_log, password=self._my_idb_pwd, database=db_name, timeout=20)
                client.write_points(database=db_name, points=liste_objets)
            except Exception as e :
                logging.error("Echec insert massif dans IDB : %s" % str(e))
