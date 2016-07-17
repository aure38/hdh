from datetime import datetime
from pytz import timezone
import rethinkdb as r
import logging
import time,hashlib
import os,sys,re
from pathlib import Path
from aclib.ops4app import Ops4app


def scan_vid_files(ops) :
    # -- Log d'activite
    ops.insertKPI(measurement='state', value=1.0, tags={'component' : 'python'})

    my_vid_files = list()
    my_subtitles_files = list()

    # -- function de scan d'un repertoire qui s'appellera en recursive
    def scan_rep(ptags = list(), ppath = Path(), pDepart = list()) :
        if ppath.is_dir() :
            #logging.log(logging.DEBUG-2, "Repertoire : %s %s" % (ppath.absolute(),ptags))
            for chem in ppath.iterdir() :
                new_list = list(ptags)
                if chem.is_dir() :
                    if str(chem.name) != '_tej' :
                        new_list.append(str(chem.name))
                scan_rep(ptags=new_list, ppath=chem, pDepart=pDepart)

        elif ppath.is_file() :
            #------------ Lecture du fichier depuis le disque : TOUS LES FICHIERS
            # FORMAT D'UN FICHIER : Nom (audio lg) [sub titles].ext   OU    Saga Annee Nom ...
            nf = dict()
            nf['file.name_full'] = str(ppath.name)           # Le nom complet du fichier (sans le chemin)
            nf['file.name_stem'] = str(ppath.stem)      # Tout se qui est a gauche du dernier "." du nom complet
            nf['file.suffix'] = str(ppath.suffix)  # Le segment a droite du dernier "." du nom complet
            nf['file.pathfullname'] = str(ppath)            # Le lien complet vers le fichier : chemin + nom complet
            nf['file.path'] = str(ppath.parent)     # Le repertoire du fichier, dans le dernier "/" : pathfullname = path + "/" + name_full
            nf['file.subtitles'] = list()
            nf['file.audiotracks'] = list()

            tmpFS = ppath.stat()
            nf['file.size_GB'] = float(int(tmpFS.st_size)/1024/1024/1024)
            tmp_date = min(int(tmpFS.st_ctime), int(tmpFS.st_atime), int(tmpFS.st_mtime))   # On prend la date la plus ancienne
            localtz = timezone('Europe/Paris')
            tmp_date2 = datetime.fromtimestamp(tmp_date,tz=localtz)
            nf['file.ts_creation'] = r.iso8601(tmp_date2.isoformat())
            nf['file.ts_lastcheck'] = r.iso8601(datetime.now(tz=localtz).isoformat())
            nf['file.tags'] = list(ptags)

            ##  --- ID = chemin vers le fichier depuis le "root"
            id_str = str(pDepart[0] + Path("/").as_posix() + ppath.relative_to(Path(pDepart[1])).as_posix())
            nf['id'] = hashlib.md5(id_str.encode(encoding='utf-8', errors='ignore')).hexdigest()
            #  --- ID = fullpathname vers le fichier -> mais on met l'IP du srv dans la base...
            #nf['id'] = ppath.as_posix()
            logging.log(logging.DEBUG-2, "Fichier : %s" % nf['file.name_full']+" | "+str(nf['file.size_GB']))

            # -- On ajoute dans la liste qui convient
            if '.' not in str(nf['file.suffix'])[0] :
                logging.error("Extension du fichier incorrecte : %s" % nf['file.pathfullname'])
            elif str(nf['file.suffix']).lower()[1:] in ops.config.get('fich_vid') :
                # --- Fichier video
                # - Recherche des sous titres dans le nom du fichier
                m = re.search('\[.+\]', nf['file.name_stem'])
                if m:
                    nf['file.subtitles'].extend(m.group(0)[1:-1].split(" "))
                # - Recherche de l'audio
                m = re.search('\(.+\)', nf['file.name_stem'])
                if m:
                    nf['file.audiotracks'].extend(m.group(0)[1:-1].split(" "))
                if len(nf['file.audiotracks']) == 0 :
                    nf['file.audiotracks'].append("fr") # Par defaut la langue est le fr

                # --- INSERT DANS LIST VIDEOS
                my_vid_files.append(nf)

            elif str(nf['file.suffix']).lower()[1:] in ops.config.get('fich_soustit') :
                # --- Fichier de sous titre
                my_subtitles_files.append(nf)
            elif str(nf['file.suffix']).lower()[1:] in ops.config.get('fich_ignore') :
                pass
            else:
                logging.warning("Fichier inconnu : %s" % nf['file.pathfullname'])
        else :
            logging.error("Type non reconnu, ni repertoire, ni fichier : %s" % str(ppath))


    # --- On construit la liste des fichiers de video & de sous-titres
    cles = sorted((ops.config.get('reps') or dict()).keys())
    for nom in cles :
        logging.debug("Scanning directory %s" % nom)
        scan_rep([nom], Path(ops.config.get('reps')[nom]), [nom, ops.config.get('reps')[nom]])

    #--- On browse les sous-titres pour completer la liste des videos
    logging.debug("Matching subtitles on %d media / %d subtitles " % (len(my_vid_files), len(my_subtitles_files)))
    nbst = 0
    for subt in my_subtitles_files:
        # vid.df.dsf (en) [fr].en.srt
        if str(subt['file.suffix']).lower() in ['.srt', '.txt'] :
            tmpParts = str(subt['file.name_stem']).split('.')
            nbp = len(tmpParts)
            if nbp > 1 :
                stlang = tmpParts[nbp-1]  # On prend le "en" ou "fr" qui sera entre le nom de fichier et l'extension srt
                if len(stlang) > 0 :
                    tmpNomFichierMedia = str(subt['file.name_stem'])[:-(len(stlang)+1)]  # On enleve le .en de la fin pour avoir le nom stem du fichier video
                    for fichier in my_vid_files :
                        if fichier['file.path'] == fichier['file.path'] and fichier['file.name_stem'] == tmpNomFichierMedia :
                            fichier['file.subtitles'].append(stlang)
                            fichier['file.subtitles'] = list(set(fichier['file.subtitles']))
                            logging.log(logging.DEBUG-2,"st trouve : %s" % fichier['file.name_stem'])
                            nbst += 1
                            break
    logging.info("%d st affectes" % nbst)

    #--- Ici on a les listes : on envoie vers rdb
    if len(my_vid_files) < 1000 :
        logging.critical("%d fichiers seulement, update dans rdb annule" % len(my_vid_files))
    else :
        try :
            # Effacement prealable
            # nbindb = int(r.table(ops.config['rdb.table.vidfiles']).count().run(ops.rdb))
            # if nbindb > 0 :
            #     logging.debug("Effacement des %d fichiers de la base actuelle" % nbindb)
            #     r.table(ops.config['rdb.table.vidfiles']).delete().run(ops.rdb)
            logging.debug("Insert/update %d objects in rdb" % len(my_vid_files))
            # nbins = nbupd = 0
            # for vi in my_vid_files :
            #     reponse = r.table(ops.config['rdb.table.vidfiles']).insert(vi, conflict="update", return_changes=False).run(ops.rdb) # conflict="update" ou replace
            #     nbins += reponse.get('inserted') or 0
            #     nbupd += reponse.get('replaced') or 0
            reponse = r.table(ops.config['rdb.table.vidfiles']).insert(my_vid_files, conflict="update", return_changes=False).run(ops.rdb) # conflict="update" ou replace
            logging.info("RDB : %d Inserted | %d updated" % (reponse.get('inserted') or 0, reponse.get('replaced') or 0))
        except Exception as e :
            logging.error("Exception durant update rdb : %s" % str(e))

    # -- Log d'activite
    time.sleep(1)  # On attend 1s pour que cela donne une duree >1 pour l'affichage grafana
    ops.insertKPI(measurement='state', value=0.0, tags={'component' : 'python'})


if __name__ == '__main__':
    logging.addLevelName(logging.DEBUG-2, 'DEBUG_DETAILS') # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.addLevelName(logging.INFO-2, 'INFO2') # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.basicConfig(level=logging.DEBUG, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(levelname)s|%(asctime)s | %(message)s")  # %(thread)d %(funcName)s L%(lineno)d
    logging.getLogger("requests").setLevel(logging.WARNING) # On desactive les logs pour la librairie requests
    logging.getLogger("schedule").setLevel(logging.WARNING) # On desactive les logs pour la librairie schedule
    logging.info("Starting...")

    # -- PATH
    if 'daem' in Path.cwd().parts[-1] :
        os.chdir("..")
    elif 'github' in Path.cwd().parts[-1] :
        os.chdir("./hdh")
    sys.path.append('./')
    logging.info("Starting from %s" % str(os.getcwd()))

    myops = Ops4app(appli_uname='deamon.scan.vifiles')
    scan_vid_files(myops)

