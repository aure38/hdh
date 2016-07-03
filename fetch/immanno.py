from datetime import datetime
from pytz import timezone
from pathlib import Path
from bs4 import BeautifulSoup
import rethinkdb as r
import logging,time,os,sys,re,random
import schedule,requests,threading
import difflib,hashlib
from aclib.ops4app import Ops4app
from aclib.func4strings import Func4strings as f4s


class ImmoFetcher(threading.Thread):
    our_dico_ville_2_codepostal = dict()    # str(codepostal) -> str(nom_ville)
    def __init__(self, ops_pointeur):
        super(ImmoFetcher, self).__init__(group=None, target=None, daemon=False)  # name=None
        self.my_ops = ops_pointeur
        self.my_fetcher_name = str(self.my_ops.config.get('id', 'NO_ID')).split('.')[-1].lower()
        self.my_url_queries = self.my_ops.config.get('urls', dict())
        self.my_current_query_name = ''             # le nom de la requete tel que  dans le fichier de conf
        self.my_current_url_annonces = list()       # La liste qui contient les liens vers les annonces pour la requete courante requete (parmi celles de url_queries)
        self.my_current_annonce  = dict()           # L'annonce courante parsee depuis sa page web, un dict au format json

        # -- construction du dico commun a la 1ere isntance (sinon il faut gerer acces en //)
        if len(ImmoFetcher.our_dico_ville_2_codepostal) == 0 :
            liste_obj = list(r.table('ress_villesfr').filter(r.row['departement'].eq('38')).pluck('codepostal', 'nom_stz').run(self.my_ops.rdb))
            for obj in liste_obj:
                ImmoFetcher.our_dico_ville_2_codepostal[obj['codepostal']] = obj['nom_stz']
    def run(self):
        # -- Log d'activite
        t0 = time.perf_counter()
        self.my_ops.insertKPI(measurement='state', value=1.0, tags={'component' : 'python'})

        # Boucle sur la liste des requetes a lancer sur le site de cette instance
        nb_annonces_listed = 0
        nb_annonces_fetched = 0
        nb_annonces_inserted_in_db = 0
        nb_annonces_updated_in_db = 0

        logging.info("Start fetching from %s" % self.my_fetcher_name)
        if self.my_fetcher_name == "lbc" :
            for tmpName in self.my_url_queries :
                nb_annonces_for_this_query = self.lbc_parse_liste_annonces(url_requete=str(self.my_url_queries[tmpName]).strip())
                nb_annonces_listed += nb_annonces_for_this_query
                if nb_annonces_for_this_query == 0 :
                    logging.warning("Requete LBC %s sans resultat : %s " % (tmpName, str(self.my_url_queries[tmpName]).strip()))
                # Ici on a une liste d'url vers les annonces correspondant a la requete courante
                for url_annonce in self.my_current_url_annonces :
                    self.my_current_query_name = tmpName
                    if self.lbc_parse_annonce(url_annonce) :  # recup 1 annonce depuis le web, creation d'un json (attribut de l'instance = dict)
                        nb_annonces_fetched += 1
                        [tmpInserted, tmpUpdated] = self.push_annonce_to_rdb()
                        nb_annonces_updated_in_db += tmpUpdated
                        nb_annonces_inserted_in_db += tmpInserted
                        logging.debug("URL Parsed : %s" % url_annonce)
                    time.sleep(random.randint(2,10))    # Attente de 1 a n secondes

        elif self.my_fetcher_name == "zil" :
            for tmpName in self.my_url_queries :
                self.my_current_query_name = tmpName
                [ann_listed, ann_fetched, ann_inserted_db, ann_updated_db] = self.zil_parse_annonces(url_requete=str(self.my_url_queries[tmpName]).strip())
                nb_annonces_listed          += ann_listed
                nb_annonces_fetched         += ann_fetched
                nb_annonces_inserted_in_db  += ann_inserted_db
                nb_annonces_updated_in_db   += ann_updated_db

        elif self.my_fetcher_name == "sudi" :
            for tmpName in self.my_url_queries :
                self.my_current_query_name = tmpName
                [ann_listed, ann_fetched, ann_inserted_db, ann_updated_db] = self.sudi_parse_annonces(url_requete=str(self.my_url_queries[tmpName]).strip())
                nb_annonces_listed          += ann_listed
                nb_annonces_fetched         += ann_fetched
                nb_annonces_inserted_in_db  += ann_inserted_db
                nb_annonces_updated_in_db   += ann_updated_db

        else:
            # site indique dans le fichier de conf non implmente...
            logging.error("%s n'est pas supporte pour le fetch" % self.my_ops.config.get('id', 'NO_ID_DANS_CONFIG'))

        logging.info("Site %s had %s annnonces, %d fetched : %d new inserted and %d updated in db" % (self.my_fetcher_name, nb_annonces_listed, nb_annonces_fetched, nb_annonces_inserted_in_db, nb_annonces_updated_in_db))
        self.my_ops.insertKPI(measurement='number', value=int(nb_annonces_fetched), tags={'component' : 'python', 'payload' : 'yes', 'number_of' : 'checked_annonces'})
        self.my_ops.insertKPI(measurement='number', value=int(nb_annonces_updated_in_db), tags={'component' : 'python', 'payload' : 'yes', 'number_of' : 'updated_annonces'})
        self.my_ops.insertKPI(measurement='number', value=int(nb_annonces_inserted_in_db), tags={'component' : 'python', 'payload' : 'yes', 'number_of' : 'new_annonces'})

        # -- Log d'activite
        t1 = time.perf_counter()
        twait = 63 - int((t1-t0))  # On calcule le temps a attendre si le process a pris moins de 1 min, pour affichage grafana
        if twait>1 :
            time.sleep(twait)
        self.my_ops.insertKPI(measurement='state', value=0.0, tags={'component' : 'python'})

    def sudi_parse_annonces(self, url_requete=""):
        retour = [0, 0, 0, 0]
        self.my_current_url_annonces = []

        # ----- On parse la page du fichier de conf pour recup la liste des url des annonces renvoyees
        if len(url_requete) > 4 :
            response = requests.get(url_requete)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
            # Boucle sur les tags contenant les url vers chaque annonce matchant la requete
            for ref_immo in soup.select("div#global > div#center > div#content > div#fiche > div#annonces > div.pane > ul > li > h3 > a") :
                if ref_immo.get('href', '') != '' :
                    tmpURLann = 'http://www.immo-isere.com/' + ref_immo.get('href', '')
                    if tmpURLann not in self.my_current_url_annonces :
                        self.my_current_url_annonces.append(tmpURLann)

        # ----- On boucle sur l'url de chaque annonce
        if len(self.my_current_url_annonces) > 0 :
            envoi_2_db = True
            retour[0] = len(self.my_current_url_annonces)
            for url_ann in self.my_current_url_annonces :
                self.my_current_annonce = dict()
                response = requests.get(url_ann)
                soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])

                self.my_current_annonce['url_annonce'] = url_ann
                self.my_current_annonce['title'] = self.my_current_annonce['title_stz'] = ""
                try :
                    tmpTitLib = soup.select("div#global > div#center > div#content > div#fiche > div.photo > h1.title > a")
                    self.my_current_annonce['title']     = f4s.cleanLangueFr(tmpTitLib)
                    self.my_current_annonce['title_stz'] = f4s.cleanOnlyLetterDigit(tmpTitLib)
                except :
                    self.my_current_annonce['title'] = self.my_current_annonce['title_stz'] = ''
                    logging.error("Impossible de caster le titre sur url=%s" % url_ann)

                self.my_current_annonce['localite_stz'] = self.my_current_annonce['codepostal'] = ""
                try :
                    tmpLis = soup.select("div#global > div#center > div#content > div#fiche > div.presentation > div.infos > h4")
                    tmploc2=''
                    if len(tmpLis)>0 :
                        tmploc = f4s.cleanOnlyLetterDigit(tmpLis[0].text).lower()
                        tmploc2 = f4s.strMultiReplace([('secteur ',''), ('les deux alpes','mont de lans'), ('l alpe d huez', 'huez'),
                                                         ('trieves', 'lalley'), ('st ','saint '), ('gresse','gresse en vercors')], tmploc)
                        tmploc2 = f4s.strMultiReplace([('cornillon en lalley', 'cornillon en trieves'), ('saint maurice en lalley','saint maurice en trieves'),
                                                         ('l alpe du grand serre', 'la morte')], tmploc2)
                    self.my_current_annonce['localite_stz'] = tmploc2
                    # On recup le code postal depuis le dico rempli depuis un fichier statique
                    self.my_current_annonce['codepostal'] = ImmoFetcher.our_dico_ville_2_codepostal.get(tmploc2, "")
                    if self.my_current_annonce['codepostal'] == "" :
                        if "riouperoux" in tmploc2 :
                            self.my_current_annonce['codepostal'] = "38220"
                        else :
                            logging.error("Localite sans code postal : %s" % tmploc2)
                            envoi_2_db = False
                except :
                    self.my_current_annonce['localite_stz'] = self.my_current_annonce['codepostal'] = ""
                    logging.error("Impossible de caster la localite et le code postal=%s" % url_ann)

                self.my_current_annonce['price'] = 0
                tmpPrixBrut = ''
                tmpLis = soup.select("div#global > div#center > div#content > div#fiche > div.presentation > div.infos > h2")
                if len(tmpLis)>0 :
                    tmpPrixBrut = f4s.cleanOnlyLetterDigit(tmpLis[0].text)
                tmpPrixListe = re.findall(r'[0-9] [0-9][0-9][0-9] [0-9][0-9][0-9]', tmpPrixBrut)
                if len(tmpPrixListe) == 0 :
                    tmpPrixListe = re.findall(r'[0-9][0-9][0-9] [0-9][0-9][0-9]', tmpPrixBrut)
                if len(tmpPrixListe) == 0 :
                    tmpPrixListe = re.findall(r'[0-9][0-9] [0-9][0-9][0-9]', tmpPrixBrut)
                if len(tmpPrixListe) > 0 :
                    try :
                        self.my_current_annonce['price'] = int(tmpPrixListe[0].replace(' ',''))
                    except:
                        self.my_current_annonce['price'] = 0
                        logging.error("Impossible de caster le prix sur url=%s" % url_requete)

                self.my_current_annonce['description'] = self.my_current_annonce['description_stz'] = ""
                tmpLis = soup.select("p.justify")
                if len(tmpLis)>0 :
                    self.my_current_annonce['description']     = f4s.cleanLangueFr(tmpLis[0].decode())
                    self.my_current_annonce['description_stz'] = f4s.cleanOnlyLetterDigit(tmpLis[0].decode())

                self.my_current_annonce['type2bien'] = self.my_current_annonce['ges'] = self.my_current_annonce['classeenergie'] = ""
                tmpLis = soup.select("div#global > div#center > div#content > div#fiche > div.presentation > div.infos > h3")
                if len(tmpLis)>0 :
                    self.my_current_annonce['type2bien'] = f4s.strCleanSanitize(tmpLis[0].text, phtmlunescape=True, pLignesTabsGuillemets=True, pNormalizeASCII=True, pEnleveSignesSpeciaux=True, pLettreDigitPointTiret=True, pBagOfWords=True)

                self.my_current_annonce['surface'] = self.my_current_annonce['nbpieces'] = 0
                self.my_current_annonce['uploadby'] = "Agence SudIsere"

                tmpNow = datetime.now()
                localtz = timezone('Europe/Paris')
                self.my_current_annonce['ts_published'] = r.iso8601(localtz.localize(tmpNow).isoformat())
                self.my_current_annonce['ts_updated']   = r.iso8601(localtz.localize(tmpNow).isoformat())
                self.my_current_annonce['ts_collected'] = r.iso8601(localtz.localize(tmpNow).isoformat())
                self.my_current_annonce['ts_lastfetched'] = r.iso8601(localtz.localize(tmpNow).isoformat())

                # --- Les images
                self.my_current_annonce['url_images'] = list()
                for url_img in soup.find_all('img', attrs={'class':'fullsize'}) :
                    if url_img.get("src", None) is not None :
                        self.my_current_annonce['url_images'].append("http://www.immo-isere.com/" + url_img.get("src",""))
                for url_img in soup.select("div#global > div#center > div#content > div#fiche > ul.thumbs > li > a > img") :
                    if url_img.get("rel", None) is not None :
                        tmpUrl = "http://www.immo-isere.com/" + url_img.get("rel","")
                        if tmpUrl not in self.my_current_annonce['url_images'] :
                            self.my_current_annonce['url_images'].append(tmpUrl)

                self.my_current_annonce['sources'] = [self.my_fetcher_name, self.my_current_query_name.lower()]
                self.my_current_annonce['history'] = dict()

                # --- Construction de l'id
                self.my_current_annonce['id'] = url_ann

                retour[1] += 1 # annonce fetched
                logging.debug("URL Parsed : %s" % url_ann)
                time.sleep(random.randint(2,10))    # Attente de 1 a 5 secondes
                if envoi_2_db :
                    [tmpInserted, tmpUpdated] = self.push_annonce_to_rdb()
                    retour[2] += tmpInserted
                    retour[3] += tmpUpdated

        return retour

    def lbc_parse_liste_annonces(self, url_requete=""):
        ## Parsing de la page web en retour a la requete specifiee dans le fichier de conf : recuperation d'une liste d'annonces
        self.my_current_url_annonces = []
        if len(url_requete) > 4 :
            response = requests.get(url_requete)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
            # Boucle sur les tags contenant les url vers chaque annonce matchant la requete
            for ref_immo in soup.select('main section section section ul li a') :
                if "www.leboncoin.fr/ventes_immobilieres" in ref_immo.get("href") :
                    tmpAddr = str(ref_immo.get("href"))
                    if tmpAddr.startswith("//") :
                        tmpAddr = "http:" + tmpAddr
                    self.my_current_url_annonces.append(tmpAddr)
                    # print("Annonce : %s \t %s \t %s" % (str(ref_immo.get("title")).split(" ")[0], ref_immo.get("title"), ref_immo.get("href")))
        return len(self.my_current_url_annonces)

    def lbc_parse_annonce(self, p_url_annonce):
        retour = True
        self.my_current_annonce = dict()

        try :
            response = requests.get(p_url_annonce)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
        except:
            logging.error("LBC : Page web non trouvee : %s" % p_url_annonce)
            retour = False
        else:
            self.my_current_annonce['url_annonce'] = p_url_annonce

            try :
                tmpTitre = soup.select('section[id="adview"] h1[class="no-border"]')[0].decode()
                self.my_current_annonce['title']     = f4s.cleanLangueFr(tmpTitre)
                self.my_current_annonce['title_stz'] = f4s.cleanOnlyLetterDigit(tmpTitre)
            except :
                self.my_current_annonce['title'] = self.my_current_annonce['title_stz'] = ''
                logging.error("lbc : Impossible de caster le titre sur url=%s" % p_url_annonce)

            self.my_current_annonce['price'] = 0
            tmpPr = soup.select('div[class="line"] h2[itemprop="price"]')
            if len(tmpPr) > 0 :
                tmpPrix = f4s.cleanOnlyLetterDigit(tmpPr[0]['content'])
                try :
                    self.my_current_annonce['price'] = int(tmpPrix)
                except :
                    self.my_current_annonce['price'] = 0
                    logging.error("LBC : Impossible de caster le prix sur url=%s" % p_url_annonce)

            self.my_current_annonce['description'] = self.my_current_annonce['description_stz'] = ""
            tmpListe = soup.find_all("p", attrs={"class":"value", "itemprop": "description"})
            if len(tmpListe) > 0 :
                self.my_current_annonce['description']     = f4s.cleanLangueFr(tmpListe[0].decode())
                self.my_current_annonce['description_stz'] = f4s.cleanOnlyLetterDigit(tmpListe[0].decode())

            self.my_current_annonce['localite_stz'] = self.my_current_annonce['codepostal'] = ""
            self.my_current_annonce['type2bien'] = self.my_current_annonce['ges'] = self.my_current_annonce['classeenergie'] = ""
            self.my_current_annonce['surface'] = self.my_current_annonce['nbpieces'] = 0

            def nettoie_str(chaine_caract) :
                return f4s.strCleanSanitize(chaine_caract, phtmlunescape=True, pLignesTabsGuillemets=True, pNormalizeASCII=True,
                                            pEnleveSignesSpeciaux=True, pLettreDigitPointTiret=True, pBagOfWords=False)

            for champs in soup.select('h2[class="clearfix"]') :
                cle = f4s.cleanOnlyLetterDigit(champs.select('span[class="property"]')[0].text).lower()
                try :
                    if "ville" in cle :
                        valeur = f4s.cleanOnlyLetterDigit(champs.select('span[class="value"]')[0].text)
                        tmpCP = re.findall(r'[0-9][0-9][0-9][0-9][0-9]',valeur)
                        if len(tmpCP)>0 :
                            self.my_current_annonce['codepostal'] = str(tmpCP[0])
                        tmpLoc2 = str(valeur).replace(self.my_current_annonce['codepostal'], "")
                        self.my_current_annonce['localite_stz'] = nettoie_str(tmpLoc2)
                    elif "type de bien" in cle :
                        valeur = nettoie_str(champs.select('span[class="value"]')[0].text)
                        self.my_current_annonce['type2bien'] = valeur
                    elif "surface" in cle :
                        valeur = nettoie_str(champs.select('span[class="value"]')[0].text)
                        self.my_current_annonce['surface'] = int(valeur[:-3].replace(" ","",-1))   # on enleve le " m2" puis tous les espaces car "1 000" ne caste pas en int, il faut 1000
                    elif "pieces" in cle :
                        valeur = nettoie_str(champs.select('span[class="value"]')[0].text)
                        self.my_current_annonce['nbpieces'] = int(valeur)
                    elif "classe energie" in cle :
                        valeur = nettoie_str(champs.select('span[class="value"] a')[0].text)
                        self.my_current_annonce['classeenergie'] = valeur
                    elif "ges" in cle :
                        valeur = nettoie_str(champs.select('span[class="value"] a')[0].text)
                        self.my_current_annonce['ges'] = valeur
                except:
                    logging.error("LBC : Impossible de caster %s sur url=%s" % (cle, p_url_annonce))

            self.my_current_annonce['uploadby'] = "LBC"
            tmpListe = soup.find_all("a", attrs={"class":"uppercase bold trackable"})
            if len(tmpListe) > 0 :
                self.my_current_annonce['uploadby'] = nettoie_str(tmpListe[0].text)

            # --- Les dates
            tmpUploadTime = ""
            tmpListe = soup.find_all("p", attrs={"class":"line line_pro"})
            if len(tmpListe) > 0 :
                try:
                    tmpD = tmpListe[0].contents[0].split(sep='en ligne le ', maxsplit=1)[1]
                    tmpUploadTime = nettoie_str(tmpD).lower()
                except:
                    pass

            # Parsing de la date Date de la forme : "9 septembre a 14:00."
            tmpNow = datetime.now()
            try :
                tmpUploadTime = f4s.strMultiReplace([('janvier','01'), ('fevrier','02'), ('fvrier','02'), ('mars','03'), ('avril','04'), ('mai','05'), ('juin','06'),
                                                   ('juillet','07'),('aout','08'), ('septembre','09'), ('octobre','10'), ('novembre','11'), ('decembre','12')], tmpUploadTime)
                if len(tmpUploadTime.split(" ")[0]) == 1 :  # Si 1 seul chiffre au debut, on ajoute un "0" devant
                    tmpUploadTime = "0" + tmpUploadTime
                localtz = timezone('Europe/Paris')                                       # On localize la date comme etant en France. Le decalage d'ete ne semble pas pris en compte par contre, juste la timezone
                tmpUploadTime += " " + str(localtz.localize(tmpNow).year)
                tmpDatetime = datetime.strptime(tmpUploadTime, '%d %m a %H %M %Y')      # 9 09 a 14:00."
                self.my_current_annonce['ts_published'] = r.iso8601(localtz.localize(tmpDatetime).isoformat()) # creation d'un object serialiable rethinkDB pour ce datetime
            except :
                logging.error("LBC : Impossible de caster la date : %s" % tmpUploadTime)
                localtz = timezone('Europe/Paris')                                       # On localize la date comme etant en France. Le decalage d'ete ne semble pas pris en compte par contre, juste la timezone
                self.my_current_annonce['ts_published'] = r.iso8601(localtz.localize(tmpNow).isoformat())
            # On fixe la date de last update a now car si l'annonce est nouvelle, c'est un update dans la DB et si l'annonce est antidatee, on la verra quand meme dans les updates
            # Si l'annonce est deja dans la db, elle ne sera pas inseree, donc la date de last_update restera dans le passe
            self.my_current_annonce['ts_updated']   = r.iso8601(localtz.localize(tmpNow).isoformat())
            self.my_current_annonce['ts_collected'] = r.iso8601(localtz.localize(tmpNow).isoformat())
            self.my_current_annonce['ts_lastfetched'] = r.iso8601(localtz.localize(tmpNow).isoformat())     # Ce champ sera update en cas de doublon dans la base : pour suivre la duree de persistance des annonces

            # --- Les images
            self.my_current_annonce['url_images'] = list()
            # Les images ne sont plus extraites car appellees via du JS
            for scligne in soup.select('section[class="adview_main"] script') :
                it = re.findall(r'"//.+jpg"', str(scligne))
                for chaine in it :
                    if '/thumbs/' not in chaine :
                        chaine2 = 'http:' + chaine[1:-1]    # pour enlever les "" autour & ajouter l'url complete
                        if chaine2 not in self.my_current_annonce['url_images']:
                            self.my_current_annonce['url_images'].append(chaine2)
            self.my_current_annonce['sources'] = [self.my_fetcher_name, self.my_current_query_name.lower()]
            self.my_current_annonce['history'] = dict()

            # --- Construction de l'id
            self.my_current_annonce['id'] = self.my_current_annonce['url_annonce']  # Id unique : commune - type2bien - surface - compteur ?
        return retour

    def zil_parse_annonces(self, url_requete=""):
        retour = [0, 0, 0, 0]
        self.my_current_url_annonces = list()

        #---- Recuperation des liens vers les annonces correspondant a cette requete
        if len(url_requete) > 4 :
            response = requests.get(url_requete)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
            liste_references = [i.text for i in soup.select("html > body > div > div > div > div > div > table > tr > td > div > span")]
            liste_url_vrac = [i['href'] for i in soup.select("html > body > div > div > div > div > div > table > tr > td > div > div > a")]
            for ref in liste_references :
                for url_zil in liste_url_vrac :
                    if "/"+ref+".htm" in url_zil :
                        tmpu = "http://zilek.fr/"+url_zil
                        if tmpu not in self.my_current_url_annonces :
                            self.my_current_url_annonces.append(tmpu)
                        break
            urls = list()
            for ref in liste_references :
                for url_zil in liste_url_vrac :
                    if "/"+ref+".htm" in url_zil :
                        tmpu = "http://zilek.fr/"+url_zil
                        if tmpu not in urls :
                            urls.append(tmpu)
                        break

        # ----- On boucle sur l'url de chaque annonce : PARSING
        envoi_2_db = True
        for url_ann in self.my_current_url_annonces :
            retour[0] += 1
            self.my_current_annonce = dict()
            response = requests.get(url_ann)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])

            # -- TITRE
            self.my_current_annonce['url_annonce'] = url_ann
            self.my_current_annonce['title'] = self.my_current_annonce['title_stz'] = ""

            try :
                tmpTit = soup.select("html > body > div > div > div > div > div > h1")[0].decode()
                self.my_current_annonce['title']     = f4s.cleanLangueFr(tmpTit)
                self.my_current_annonce['title_stz'] = f4s.cleanOnlyLetterDigit(tmpTit)
            except :
                self.my_current_annonce['title'] = self.my_current_annonce['title_stz'] = ''
                logging.error("Zil : Impossible de caster le titre sur url=%s" % url_ann)

            # -- plusieurs parsing
            self.my_current_annonce['localite_stz'] = self.my_current_annonce['codepostal'] = ""
            self.my_current_annonce['price'] = 0
            self.my_current_annonce['description'] = self.my_current_annonce['description_stz'] = ""
            tmpNow = datetime.now()
            localtz = timezone('Europe/Paris')
            self.my_current_annonce['ts_published'] = r.iso8601(localtz.localize(tmpNow).isoformat())
            self.my_current_annonce['ts_updated']   = r.iso8601(localtz.localize(tmpNow).isoformat())
            self.my_current_annonce['ts_collected'] = r.iso8601(localtz.localize(tmpNow).isoformat())
            self.my_current_annonce['ts_lastfetched'] = r.iso8601(localtz.localize(tmpNow).isoformat())
            for h3 in soup.select("h3") :
                if 'prix' in str(h3.text).lower() :
                    # --- PARSING PRIX
                    ul = h3.find_next_sibling()
                    try :
                        str_prix = ul.li.contents[0]
                        str_prix = re.sub("\D", "", f4s.cleanOnlyLetterDigit(str_prix))
                        self.my_current_annonce['price'] = int(str_prix)
                    except :
                        logging.error("Zil : Impossible de caster le prix : %s in %s" % (h3,url_ann))

                    # --- PARSING DATE de la forme : "ajoute au site le 24 Mar 2016" ou " 4 Mar 2016"
                    try :
                        chaine1 = f4s.cleanOnlyLetterDigit(ul.text).lower()
                        chaine2 = f4s.strMultiReplace([('jan','01'), ('fev','02'), ('feb','02'), ('mar','03'), ('avr','04'), ('apr','04'), ('mai','05'), ('may','05'), ('juin','06'), ('jun','06'), ('juil','07'),('jul','07'), ('aou','08'), ('aug','08'), ('sep','09'), ('oct','10'), ('nov','11'), ('dec','12')], chaine1)
                        chaine3 = f4s.strMultiReplace([(' 1 ','01 '), (' 2 ','02 '), (' 3 ','03 '), (' 4 ','04 '), (' 5 ','05 '), (' 6 ','06 '), (' 7 ','07 '), (' 8 ','08 '), (' 9 ','09 ')], chaine2)
                        chaine4 = chaine3[chaine3.find("ajoute au site le")+17:]
                        cha = re.search("[0-9][0-9] [0-9][0-9] [0-9][0-9][0-9][0-9]", chaine4)
                        tmpDatetime = datetime.strptime(cha.group(0), '%d %m %Y')
                        self.my_current_annonce['ts_published'] = r.iso8601(localtz.localize(tmpDatetime).isoformat())
                    except :
                        logging.error("Zil : Impossible de caster la date : %s in %s" % (h3,url_ann))

                elif 'emplacement' in str(h3.text).lower() :
                    # --- PARSING LOCALITE et CODE POSTAL
                    try :
                        ul = h3.find_next_sibling()
                        m = re.search('commune : (.+) \((.+)\)', ul.li.text)
                        self.my_current_annonce['localite_stz'] = f4s.cleanOnlyLetterDigit(m.group(1))
                        self.my_current_annonce['codepostal'] = f4s.cleanOnlyLetterDigit(m.group(2))
                    except :
                        logging.error("Zil : Impossible de caster localite et code postal : %s in %s" % (h3,url_ann))

                elif 'description' in str(h3.text).lower() :
                    # --- PARSING DESCRIPTION
                    try :
                        ul = h3.find_next_sibling()
                        self.my_current_annonce['description']     = f4s.cleanLangueFr(ul.text)
                        self.my_current_annonce['description_stz'] = f4s.cleanOnlyLetterDigit(ul.text)
                    except :
                        logging.error("Zil : Impossible de caster description : %s in %s" % (h3,url_ann))

                elif 'le vendeur est' in str(h3.text).lower() :
                    # --- PARSING uploadby
                    try :
                        ul = h3.find_next_sibling()
                        tmpV = str(ul.contents[0].text).replace("@", "a")
                        self.my_current_annonce['uploadby'] = f4s.cleanOnlyLetterDigit(tmpV)
                    except :
                        logging.error("Zil : Impossible de caster nom du vendeur : %s in %s" % (h3,url_ann))

            # --- TYPE DE BIEN
            self.my_current_annonce['type2bien'] = 'inconnu'
            if 'maison' in self.my_current_annonce['title_stz'].lower() or 'villa' in self.my_current_annonce['title_stz'].lower() :
                self.my_current_annonce['type2bien'] = 'maison'
            elif 'terrain' in self.my_current_annonce['title_stz'].lower() :
                self.my_current_annonce['type2bien'] = 'terrain'
            elif 'appart' in self.my_current_annonce['title_stz'].lower() :
                self.my_current_annonce['type2bien'] = 'appartement'

            # --- GES + surface
            self.my_current_annonce['ges'] = self.my_current_annonce['classeenergie'] = ""
            self.my_current_annonce['surface'] = self.my_current_annonce['nbpieces'] = 0

            # --- IMAGES
            # les noms des thumbs et des grandes images sont les memes, seul le chemin change pour avoir la grande taille
            self.my_current_annonce['url_images'] = list()
            try :
                lien_img_grand = soup.find('img', attrs={'id':'pic0'})['src']
                m=re.search('http://(.+)/(.+)\.(.+)', lien_img_grand)
                chemin_full_img = 'http://' + m.group(1) + '/'
                for imm in soup.select("html > body > div > div > div > div > div > a > img") :
                    if 'thumbnail' in str(imm.get('alt','')) :
                        m2=re.search('http://(.+)/(.+)\.(.+)', imm.get('src'))
                        tmpUrl = chemin_full_img+m2.group(2)+"."+m2.group(3)
                        self.my_current_annonce['url_images'].append(tmpUrl)
            except :
                logging.error("Zil : Impossible de recup les images for %s" % url_ann)

            self.my_current_annonce['sources'] = [self.my_fetcher_name, self.my_current_query_name.lower()]
            self.my_current_annonce['history'] = dict()

            # --- Construction de l'id
            self.my_current_annonce['id'] = url_ann

            retour[1] += 1 # annonce fetched
            logging.debug("URL parsee : %s" % url_ann)
            if envoi_2_db :
                [tmpInserted, tmpUpdated] = self.push_annonce_to_rdb()
                retour[2] += tmpInserted
                retour[3] += tmpUpdated
            time.sleep(random.randint(2,10))    # Attente de 1 a 5 secondes
        return retour

    def push_annonce_to_rdb(self):
        retour = [0, 0]
        # L'id dans rethinkDB est limite a 127 caracteres
        if len(self.my_current_annonce['id']) > 125 :
            self.my_current_annonce['id'] = self.my_current_annonce['id'][:125]

        h = hashlib.new('sha')
        h.update(self.my_current_annonce['id'].encode())
        self.my_current_annonce['id_hash'] =  h.hexdigest()
        self.my_current_annonce['user_tags'] = list()

        # On recupere l'objet s'il est deja dans la db (c'est la majorite des cas si update regulier)
        tmpAnnonceInDB = r.table(self.my_ops.config['rdb.table.annonces']).get(self.my_current_annonce['id']).run(self.my_ops.rdb)
        if tmpAnnonceInDB is None :
            #--- New annonce Download + Insertion des images en binary dans la table de rdb
            self.my_current_annonce['images_ids'] = list()
            for url_image in self.my_current_annonce['url_images'] :
                obj_image = dict()
                obj_image['ts_created'] = self.my_current_annonce['ts_collected']
                obj_image['annonce_id'] = self.my_current_annonce['id']
                obj_image['url'] = url_image
                obj_image['type'] = 'jpg'
                if   '.jpg' in obj_image['url'] or '.jpeg' in obj_image['url'] :        obj_image['type'] = 'jpg'
                elif '.png' in obj_image['url'] :                                       obj_image['type'] = 'png'
                elif '.gif' in obj_image['url'] :                                       obj_image['type'] = 'gif'
                else : logging.warning("Type d'image non reconnu, fixe a jpg pour : %s" % obj_image['url'])
                try :
                    reqi = requests.get(url_image)
                    obj_image['content'] = r.binary(reqi.content)
                except:
                    logging.error("Erreur durant le download de l'image : %s" % url_image)

                reponse = r.table(self.my_ops.config['rdb.table.images']).insert(obj_image, conflict="error").run(self.my_ops.rdb) # conflict="update" ou replace ou error
                if reponse['inserted'] == 1 :
                    logging.debug("Insertion in DB Image : URL=%s" % obj_image['url'])
                    try :
                        obj_images_key = reponse['generated_keys'][0]
                        if obj_images_key != '' :
                            self.my_current_annonce['images_ids'].append(obj_images_key)
                    except:
                        logging.error("Images inserees mais pas de key recuperee, Id = %s, Reponse = %s" % (self.my_current_annonce['id'],str(reponse)))
                else:
                    logging.error("Erreur sur l'insertion d'une image, Id = %s, Reponse = %s" % (self.my_current_annonce['id'],str(reponse)))
                time.sleep(random.randint(1,3))    # Attente avant image suivante

            #--- Insertion de l'annonce
            reponse = r.table(self.my_ops.config['rdb.table.annonces']).insert(self.my_current_annonce, conflict="error").run(self.my_ops.rdb) # conflict="update" ou replace
            if reponse['inserted'] == 1 :
                logging.log(logging.INFO-2, "Added in DB : %s | %s" % (self.my_current_annonce['title'], self.my_current_annonce['id']))
                retour[0] = 1
            else:
                logging.error("Impossible d'inserer une annonce dans la db, Id = %s" % self.my_current_annonce['id'])

        else:
            #--- S'il y a deja un id dans la base : on merge certains champs comme le last time seen
            # on mergera toujours le last_seen, mais ce n'est pas un update en tant que tel : l'annonce est la meme
            tmpAnnonceInDB['ts_lastfetched'] = self.my_current_annonce['ts_lastfetched']
            # Nouveau prix ?
            if tmpAnnonceInDB['price'] != self.my_current_annonce['price'] :
                tmpHistoKey   = self.my_current_annonce['ts_lastfetched'].run(self.my_ops.rdb).strftime('%y%m%d-%H%M%S') + " - Price"
                tmpHistoValue = "%d" % tmpAnnonceInDB['price']
                tmpAnnonceInDB['price'] = self.my_current_annonce['price']
                tmpAnnonceInDB['history'][tmpHistoKey] = tmpHistoValue
                tmpAnnonceInDB['ts_updated']   = self.my_current_annonce['ts_lastfetched']
                retour[1] = 1

            # Nouveau titre : on calcule un taux de difference pour ne pas genere des updates sur un detail.
            if difflib.SequenceMatcher(a=tmpAnnonceInDB['title_stz'].lower(), b=self.my_current_annonce['title_stz'].lower()).ratio() < 0.85 :
                tmpHistoKey   = self.my_current_annonce['ts_lastfetched'].run(self.my_ops.rdb).strftime('%y%m%d-%H%M%S') + " - Titre"
                tmpHistoValue = "%s" % tmpAnnonceInDB['title']
                tmpAnnonceInDB['title']     = self.my_current_annonce['title']
                tmpAnnonceInDB['title_stz'] = self.my_current_annonce['title_stz']
                tmpAnnonceInDB['history'][tmpHistoKey] = tmpHistoValue
                tmpAnnonceInDB['ts_updated']   = self.my_current_annonce['ts_lastfetched']
                retour[1] = 1
            # Nouvelle description ?
            if difflib.SequenceMatcher(a=tmpAnnonceInDB['description_stz'].lower(), b=self.my_current_annonce['description_stz'].lower()).ratio() < 0.8 :
                tmpHistoKey                             = self.my_current_annonce['ts_lastfetched'].run(self.my_ops.rdb).strftime('%y%m%d-%H%M%S') + " - Description"
                tmpHistoValue                           = "%s" % tmpAnnonceInDB['description']
                tmpAnnonceInDB['description']           = self.my_current_annonce['description']
                tmpAnnonceInDB['description_stz']       = self.my_current_annonce['description_stz']
                tmpAnnonceInDB['history'][tmpHistoKey]  = tmpHistoValue
                tmpAnnonceInDB['ts_updated']            = self.my_current_annonce['ts_lastfetched']
                retour[1] = 1

            # Update de l'objet dans la db
            try :
                reponse = r.table(self.my_ops.config['rdb.table.annonces']).get(self.my_current_annonce['id']).replace(tmpAnnonceInDB).run(self.my_ops.rdb)
            except Exception as e :
                logging.error("Echec d'update d'annonce dans RDB : %s | %s" % (str(e), tmpAnnonceInDB))
                reponse = { 'replaced' : 0 }

            if reponse['replaced'] != 1 :
                logging.error("Impossible de remplacer un doc dans la db, Id=%s" % self.my_current_annonce['id'])
                retour[1] = 0
            elif retour[1] > 0 :
                logging.log(logging.INFO-2, "Update in DB : URL=%s" % str(self.my_current_annonce['id']))
            else :
                logging.debug("Refresh in DB : Pub=%s Collec=%s Fetch=%s Update=%s URL=%s" % (str(self.my_current_annonce['ts_published']), str(self.my_current_annonce['ts_collected']), str(self.my_current_annonce['ts_lastfetched']), str(self.my_current_annonce['ts_updated']), self.my_current_annonce['id']))
        return retour

def launch_fetcher(ops_ptr):
    tmpObj = ImmoFetcher(ops_ptr)
    tmpObj.start()

if __name__ == '__main__':
    logging.addLevelName(logging.DEBUG-2, 'DEBUG_DETAILS') # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.addLevelName(logging.INFO-2, 'INFO2') # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.basicConfig(level=logging.DEBUG, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(levelname)s|%(asctime)s | %(message)s")  # %(thread)d %(funcName)s L%(lineno)d
    logging.getLogger("requests").setLevel(logging.WARNING) # On desactive les logs pour la librairie requests
    logging.getLogger("schedule").setLevel(logging.WARNING) # On desactive les logs pour la librairie schedule
    logging.info("Starting...")

    # -- PATH
    if 'fetch' in Path.cwd().parts[-1] :
        os.chdir("..")
    elif 'github' in Path.cwd().parts[-1] :
        os.chdir("./hdh")
    sys.path.append('./')
    logging.info("Starting from %s" % str(os.getcwd()))

    # -- Lancement : https://github.com/dbader/schedule/blob/master/schedule/__init__.py
    ops1 = Ops4app(appli_uname='fetch.imm.lbc')
    # launch_fetcher(ops1)
    # exit()
    schedule.every(ops1.config.get('period.in.days', 1)).day.at('19:02').do(launch_fetcher, ops_ptr=ops1)

    ops2 = Ops4app(appli_uname='fetch.imm.zil')
    #launch_fetcher(ops2)
    #exit()
    schedule.every(ops2.config.get('period.in.days', 2)).days.at('19:03').do(launch_fetcher, ops_ptr=ops2)

    ops3 = Ops4app(appli_uname='fetch.imm.sudi')
    # launch_fetcher(ops3)
    # exit()
    schedule.every(ops3.config.get('period.in.days', 2)).days.at('19:04').do(launch_fetcher, ops_ptr=ops3)

    while True:
        schedule.run_pending()
        time.sleep(10)
