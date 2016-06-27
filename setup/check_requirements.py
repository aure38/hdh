# coding: utf8

import logging
logging.addLevelName(logging.DEBUG-2, 'DEBUG_DETAILS') # Logging, arguments pour fichier : filename='example.log', filemode='w'
logging.basicConfig(level=logging.INFO, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(levelname)s|%(asctime)s | %(message)s")  # %(thread)d %(funcName)s L%(lineno)d
logging.getLogger("requests").setLevel(logging.WARNING) # On desactive les logs pour la librairie requests
logging.getLogger("schedule").setLevel(logging.WARNING) # On desactive les logs pour la librairie schedule


print("\n-------- Exec & Path --------------------------------------")
import os,sys
from pathlib import Path
print("Repertoire courant = %s " % str(os.getcwd()))
if 'setup' in Path.cwd().parts[-1] :
    os.chdir("..")
    print("Changement pour %s" % str(os.getcwd()))
elif 'github' in Path.cwd().parts[-1] :
    os.chdir("./hdh")
    print("Changement pour %s" % str(os.getcwd()))
print("Path Python = %s " % str(sys.path))
print("Ajout de ./ dans le path Python")
sys.path.append('./')


print("\n-------- Ops4app ------------------------------------------")
import rethinkdb as r
from aclib.ops4app import Ops4app
import json

ops = Ops4app(appli_uname="check.requirements")
print("RDB : IP=%s | Base=%s | File=%s" % (ops._my_rdb_IP, ops._my_rdb_base, ops._my_config_file))
print("RDB OK = %s" % ops.isOK())
if not ops.isOK() :
    exit()

print("\n-------- RDB ------------------------------------------")
liste_bases = list()
try :
    liste_bases = r.db_list().run(ops.rdb)
except :
    print("ERREUR Durant connexion a RDB")
    exit()
tmpT = ops.config.get('rdb.base', '')
if tmpT not in liste_bases :
    print("ERREUR Base %s non trouvee dans RDB")
    exit()
print("OK Base : %s" % tmpT)

liste_tables_a_verif = [ops.config.get('table_cfg_in_rdb', 'ERREUR-CONFIG'), ops.config.get('table_logs_in_rdb', 'ERREUR-LOGS')]
liste_tables_in_db = r.table_list().run(ops.rdb)
for tmpT in liste_tables_a_verif :
    if tmpT not in liste_tables_in_db :
        print("Table %s non trouvee dans RDB, CREATION")
        tmpRep = r.db(ops.config.get('rdb.base', '')).table_create(tmpT).run(ops.rdb)
        if tmpRep.get("tables_created", 0) == 1 :
            print(tmpRep)
        else :
            print('ERREUR Durant creation')
            exit()
print('OK Tables : %s ' % str(liste_tables_a_verif))

print('\n\n')

del ops
