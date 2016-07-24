import logging
import cherrypy
import os,sys
from pathlib import Path
from datetime import datetime
from pytz import timezone
from aclib.ops4app import Ops4app
from aclib.func4strings import Func4strings as f4s
import rethinkdb as r

class ServStatic(object):
    def __init__(self):
        pass

class ServVid(object):
    def __init__(self):
        self.myops = Ops4app(appli_uname='websrv.vid')

    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def json_liste(self, _=""):
        selected_usr = cherrypy.session.get('selected_usr', default="007")
        if cherrypy.session.get(selected_usr+'liste_files', default=None) is None :
            retourObj = dict()
            if self.myops.rdb_get_lock() is not None :

                # --- Requete avec JOINTURE sur les user tags
                # En JS : r.db('hdh').table(self.myops.config['rdb.table.vids']).outerJoin(r.db('hdh').table("vids_users"), function(rowA,rowB) { return rowB('id').eq(rowA('id')).and(rowB.hasFields('tags_usr'))}).zip().limit(2)
                curseur = r.table(self.myops.config['rdb.table.vids']).outer_join(r.table(self.myops.config['rdb.table.vids.users']), lambda rowA,rowB: rowB['id'].eq(rowA['id']).and_(rowB.has_fields('tags_usr'))).zip()
                curseur = curseur.pluck('file.ts_creation', 'file.name_stem', 'file.tags', 'file.audiotracks', 'file.subtitles','file.size_GB', 'id', 'tags_usr')
                curseur = curseur.order_by(r.desc('file.ts_creation'), 'file.name_stem')
                curseur = curseur.run(self.myops.rdb)
                # curseur = r.table(self.myops.config['rdb.table.vids']).pluck('file.ts_creation', 'file.name_stem', 'file.tags', 'file.audiotracks', 'file.subtitles','file.size_GB', 'id')
                # curseur = curseur.order_by(r.desc('file.ts_creation'), 'file.name_stem')

                data = list()
                for doc in curseur :
                    objj = dict()

                    # -- FIELD : Name
                    objj['media_title'] = f4s.strCleanSanitize(doc['file.name_stem'])

                    # -- Recup des tags pour les users au format de selectize : # dict( idusr -> [ tags ] )
                    liste_tags_str = liste_tags_selected = "["
                    if (doc.get('tags_usr') or None) is not None :
                        if selected_usr in doc.get('tags_usr') :
                            tagcount=0
                            for tag in doc['tags_usr'][selected_usr] :
                                if tagcount > 0 :
                                    liste_tags_str += ','
                                    liste_tags_selected += ','
                                tagcount += 1
                                liste_tags_str += "{value:'%s', text:'%s'}" % (tag, tag)
                                liste_tags_selected += "'%s'" % tag
                    liste_tags_str += ']'
                    liste_tags_selected += ']'

                    # -- FIELD : Tags with JS
                    id_of_input = "input_usr_" + str(doc['id'])
                    code_htmljs = '<div style="max-width:250px ;"><input type="text" id="%s">' % id_of_input
                    # code_htmljs += '<button onclick="button_updt_tags_usr("%s", "%s",$(\'#%s\').val())">Svg</button>' % (selected_usr, str(doc['id']), id_of_input)
                    code_htmljs += '<script type="text/javascript">'
                    code_htmljs += "$('#%s').selectize({" % id_of_input
                    code_htmljs += "plugins: ['restore_on_backspace','remove_button', 'drag_drop'], "
                    code_htmljs += "delimiter: ',', "
                    code_htmljs += "options : %s, " % liste_tags_str
                    code_htmljs += "items : %s, " % liste_tags_selected
                    code_htmljs += "persist: true, "
                    code_htmljs += "create: function(input) { return { value: input, text: input } }"
                    code_htmljs += "});</script></div>"
                    objj['tags_usr'] = code_htmljs

                    # -- FIELD : Save button
                    code_htmljs  = '<div style="max-width:20 ; text-align: center ;">'
                    code_htmljs += '<button onclick="button_save_object(\'%s\', \'%s\', \'%s\')">Save</button>' % (selected_usr, str(doc['id']), id_of_input)
                    code_htmljs += '</div>'
                    objj['button_save'] = code_htmljs

                    # -- FIELD : langs
                    objj['langs'] = '<div style="max-width:20 ; text-align: center ;">' + str(" ".join(sorted(doc['file.audiotracks'])))
                    if len(doc['file.subtitles']) > 0 :
                        objj['langs'] += " | " + str(" ".join(sorted(doc['file.subtitles'])))
                    objj['langs'] += '</div>'

                    # -- FIELDS : les autres champs recues
                    for dkey in doc : # sorted(doc) :
                        if dkey in ['tags_usr', 'file.audiotracks', 'file.subtitles', 'file.name_stem'] :
                            pass
                        elif type(doc[dkey]) is str :
                            objj[str(dkey).replace(".","_")] = str(doc[dkey])
                        elif type(doc[dkey]) is datetime :
                            objj[str(dkey).replace(".","_")] = '<div STYLE="Text-ALIGN:center">' + doc[dkey].strftime('%y-%m-%d') + '</div>'
                        elif type(doc[dkey]) is float :
                            objj[str(dkey).replace(".","_")] = round(doc[dkey], 2)
                        elif type(doc[dkey]) is int :
                            objj[str(dkey).replace(".","_")] = int(doc[dkey])
                        elif type(doc[dkey]) is list :
                            objj[str(dkey).replace(".","_")] = str(" ".join(sorted(doc[dkey])))
                        elif type(doc[dkey]) is dict :
                            objj[str(dkey).replace(".","_")] = str(doc[dkey])
                        else :
                            logging.error("Type de colonne non reconnu pour vid: %s" % str(type(doc[dkey])))
                            objj[str(dkey).replace(".","_")] = str(doc[dkey])
                    data.append(objj)
                retourObj["data"] = data
                self.myops.rdb_release()
            cherrypy.session[selected_usr+'liste_files'] = retourObj
        else :
            logging.debug("From cache pour liste")
        return cherrypy.session[selected_usr+'liste_files']

    # ------- Recup distinct des tags system de la table
    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def json_tags_systm(self, _=""):
        selected_usr = cherrypy.session.get('selected_usr', default="007")
        if cherrypy.session.get(selected_usr+'tags_systm', default=None) is None :
            retourObj = list()
            if self.myops.rdb_get_lock() is not None :
                listetags = r.table(self.myops.config['rdb.table.vids'])['file.tags'].distinct().reduce(lambda left,right : left+right).distinct().run(self.myops.rdb)
                for tag in listetags :
                    retourObj.append({'value':tag, 'text':tag})
                self.myops.rdb_release()
            cherrypy.session[selected_usr+'tags_systm'] = retourObj
        else :
            logging.debug("From cache pour tags syst")
        return cherrypy.session.get(selected_usr+'tags_systm', default=[])

    # ------- Recup distinct des tags user
    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def json_tags_user(self, _="", usr_uname=""):
        selected_usr = cherrypy.session.get('selected_usr', default="007")
        if cherrypy.session.get(selected_usr+'tags_user', default=None) is None :
            retourObj = list()
            if self.myops.rdb_get_lock() is not None :
                if r.table(self.myops.config['rdb.table.vids.users'])['tags_usr'][usr_uname].count().run(self.myops.rdb) > 0 :
                    listetags = r.table(self.myops.config['rdb.table.vids.users'])['tags_usr'][usr_uname].distinct().reduce(lambda left,right : left+right).distinct().run(self.myops.rdb)
                    for tag in listetags :
                        retourObj.append({'value':tag, 'text':tag})
                self.myops.rdb_release()
            cherrypy.session[selected_usr+'tags_user'] = retourObj
        else :
            logging.debug("From cache pour tags user")
        return cherrypy.session.get(selected_usr+'tags_user', default=[])

    # ------- update les tags sur un objet pour un user specifique
    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def js_updt_tags_usr(self, _="", usr_uname="", object_id="", str_tags_comma=""):
        if usr_uname != "" and object_id != "" :
            tags_obj = str_tags_comma.split(',')
            if tags_obj[0] == "" :
                tags_obj = []

            if self.myops.rdb_get_lock() is not None :
                curseur = r.table(self.myops.config['rdb.table.vids.users']).insert({"id": object_id, "tags_usr": {usr_uname: tags_obj}}, conflict='update').run(self.myops.rdb)
                # # Update qui update un des items du dict qui contient les tags, sans effacer le dict en entier
                # curseur = r.table(self.myops.config['rdb.table.vids.users']).get(object_id).update({"tags_usr": {usr_uname: tags_obj}}).run(self.myops.rdb)
                # # -- Si l'objet n'est pas connu pas d'insert
                # if curseur['skipped'] == 1 :
                #     curseur = r.table(self.myops.config['rdb.table.vids.users']).insert({"id": object_id, "tags_usr": {usr_uname: tags_obj}})
                logging.debug('Update tags : %s' % str(curseur))
                self.myops.rdb_release()

            # -- delete du cache de session
            selected_usr = cherrypy.session.get('selected_usr', default="007")
            cherrypy.session[selected_usr+'liste_files'] = None
            cherrypy.session[selected_usr+'tags_systm'] = None
            cherrypy.session[selected_usr+'tags_user'] = None
        return { 'answer' : "ok"}

    # ------- update une session
    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def js_updt_session(self, _="", ch_key="", ch_value=None):
        cherrypy.session[ch_key] = ch_value
        return { 'answer' : "ok"}

    # ------- read une session
    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def js_read_session(self, _="", ch_key=""):
        return cherrypy.session.get(ch_key, default="")

    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def dump_session(self, _=""):
        retour = dict()
        for i in cherrypy.session.keys() :
            retour[i] = str(cherrypy.session[i])
        return retour




class ServImm(object):
    def __init__(self):
        self.myops = Ops4app(appli_uname='websrv.immo')

    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def js_imm(self, _="", pNbJours="0", pTagIncludeOnly=",", pTagExclude=","):
        retourObj = { }
        dbconn = self.myops.rdb_get_lock()
        if dbconn is not None :   # Get documents from DB
            # -- QUERY : Creation curseur puis Date, en ne garde que n jours
            curseur = r.table(self.myops.config.get('rdb.table.annon'))  # .max('ts_collected').to_json()
            if int(pNbJours) > 0 :
                curseur = curseur.filter(lambda row : row["ts_updated"].to_epoch_time().gt(r.now().to_epoch_time().add(r.expr(-3600*24*int(pNbJours)))))

            # -- QUERY : on enleve les objets qui ne contiennent pas les includesOnly (si vide, on enleve rien)
            p_liste_tags = list()
            for itag in pTagIncludeOnly.split(",") :
                if len(itag) > 2 :
                    p_liste_tags.append(str(itag))
            if len(p_liste_tags) > 0 :
                curseur = curseur.filter(r.row['user_tags'].contains(lambda jstag: r.expr(p_liste_tags).contains(jstag)))

            # -- QUERY : on enleve les objets qui contiennent les excludes
            p_liste_tags2 = list()
            for itag in pTagExclude.split(",") :
                if len(itag) > 2 :
                    p_liste_tags2.append(str(itag))
            if len(p_liste_tags2) > 0 :
                curseur = curseur.filter(r.row['user_tags'].contains(lambda jstag: r.expr(p_liste_tags2).contains(jstag)).not_())

            curseur = curseur.order_by(r.desc('ts_updated'), 'codepostal')
            curseur = curseur.run(dbconn)

            # -- Recup du retour
            retourObj = { }
            data = list()
            for doc in curseur :
                objj = dict()
                objj['localite'] = '<p align="center"><a href="https://www.google.fr/maps/place/%s" target="_blank">%s</a><br><b>%s</b></p>' % (doc.get('codepostal','00000'), doc.get('codepostal','00000'), doc.get('localite_stz','Ville inconnue'))

                tmpURLinterne = "./js_imm_dump_annonce?pIDH=%s" % doc.get('id_hash', 'None')
                objj['title'] =  '<p><b><a href="%s" title="%s" target="_blank">%s</a></b>&nbsp;' \
                                 '<a href="%s" target="_blank">webs</a>' \
                                 '<br><i>par %s</i></p>' % (tmpURLinterne, doc.get('description','No description'), doc.get('title','Titre non specifie'), doc.get('url_annonce','Url non trouvee'), doc.get('uploadby',''))
                user_tags_defaut = ['arch', 'fav', 'jit', 'aur', 'ma1', 'ma2', 'ma3', 'ma4']
                liste_user_tags = sorted(list(set(user_tags_defaut + doc.get('user_tags', []))))
                for tag in liste_user_tags :
                    if tag in doc.get('user_tags', []) :
                        objj['title'] += '<button class="ac_button_usertag_full" onclick="javascript:submitForm(\'%s\', \'toggleusertag\', \'%s\');"><b>%s</b></button>&nbsp;' % (doc.get('id_hash', 'None'), tag,tag)
                    else :
                        objj['title'] += '<button class="ac_button_usertag_empty" onclick="javascript:submitForm(\'%s\', \'toggleusertag\', \'%s\');"><b>%s</b></button>&nbsp;' % (doc.get('id_hash', 'None'), tag,tag)

                objj['price'] = '<p align="center">{:,d} k</p>'.format(int(round(doc.get('price',0)/1000,0)))
                objj['surface'] = '<p align="center">%d</p>' % doc.get('surface',0)

                # -- Champ Description avec Images & Historique
                retour="<p>"
                tmpNbImg=0
                for img_id in doc.get('images_ids', []) :
                    lien = "./js_imm_image?pID=%s" % img_id
                    tmpNbImg += 1
                    if tmpNbImg > 8 :
                        retour += '.'
                    else :
                        retour += '<a href="%s" target="_blank"><img src="%s" alt="" height="80" width="80"></a> ' % (lien, lien)

                # Insertion des commentaires sous les images
                retour += '\n<script type="text/javascript">function ShowHide(id) { var obj = document.getElementById(id); if(obj.className == "showobject") { obj.className = "hideobject"; } else { obj.className = "showobject"; } }</script>\n'
                retour += '<style type="text/css"> .hideobject{ display: none; } .showobject{ display: block; } </style>\n'

                # Description
                tmpId1 = doc['ts_updated'].astimezone(tz=timezone('Europe/Paris')).strftime('%a%d%b%H%M') + doc['title_stz'] + "description"
                retour += """<div onclick="ShowHide('""" + tmpId1 + """')"><b>Description</b></div>"""
                retour += '<div id="%s" class="hideobject">%s</div>\n' % (tmpId1, doc['description'])

                # Historique
                tmpHist = ''
                for histoKey in sorted(doc.get('history', {}).keys(),reverse=True) :
                    tmpHist += '<p><b>%s</b><br>%s</p>' % (histoKey, doc['history'][histoKey])
                if tmpHist != '' :
                    tmpId2 = doc['ts_updated'].astimezone(tz=timezone('Europe/Paris')).strftime('%a%d%b%H%M') + doc['title_stz'] + "history"
                    retour += """<div onclick="ShowHide('""" + tmpId2 + """')"><b>History</b></div>"""
                    retour += '<div id="%s" class="hideobject">%s</div>\n' % (tmpId2, tmpHist)
                retour+="</p>"
                objj['description'] = str(retour)

                for dkey in sorted(doc) :
                    if str(dkey) in ['ts_updated', 'ts_published', 'type2bien', 'sources'] :
                        if type(doc[dkey]) is str :         objj[str(dkey).replace(".","_")] = str(doc[dkey])
                        elif type(doc[dkey]) is datetime :  objj[str(dkey).replace(".","_")] = doc[dkey].strftime('%y-%m-%d<br>%H:%M')  # doc[dkey].strftime('%y-%m-%d %a')
                        elif type(doc[dkey]) is float :     objj[str(dkey).replace(".","_")] = round(doc[dkey], 1)
                        elif type(doc[dkey]) is int :       objj[str(dkey).replace(".","_")] = int(doc[dkey])
                        elif type(doc[dkey]) is list :      objj[str(dkey).replace(".","_")] = str(" ".join(sorted(doc[dkey])))
                        elif type(doc[dkey]) is dict :      objj[str(dkey).replace(".","_")] = str(doc[dkey])
                        else :
                            logging.error("Type de colonne non reconnu pour immo : %s" % str(type(doc[dkey])))
                            objj[str(dkey).replace(".","_")] = str(doc[dkey])

                data.append(objj)
            self.myops.rdb_release()
            retourObj["data"] = data
        return retourObj

    @cherrypy.expose()
    def js_imm_image(self, pID=''):
        retourObj = ''
        if pID != '' :
            dbconn = self.myops.rdb_get_lock()
            if dbconn is not None :
                curseur = r.table(self.myops.config.get('rdb.table.phot', 'immophotos'))
                curseur = curseur.get(pID)
                curseur = curseur.run(dbconn)

                cherrypy.response.headers['Content-Type'] = "image/" + curseur['type']
                retourObj = curseur['content']
            self.myops.rdb_release()

        return retourObj

    @cherrypy.expose()
    def js_imm_tag_annonce(self, pIDH='', pAction='', pParam=''):
        retourObj = ''
        if pIDH != '' :
            dbconn = self.myops.rdb_get_lock()
            if dbconn is not None :
                if 'toggleusertag' in pAction :
                    curseur = r.table(self.myops.config.get('rdb.table.annon'))  # .max('ts_collected').to_json()
                    curseur = curseur.filter(r.row["id_hash"].eq(pIDH)).get_field("user_tags")
                    curseur = curseur.run(dbconn)
                    tags0 = curseur.next()
                    curseur.close()
                    if pParam.lower() in tags0 :
                        # -- le tag est deja dans l'objet : on l'enleve de la liste
                        tags1 = list(tags0)
                        tags1.remove(pParam.lower())
                        curseur = r.table(self.myops.config.get('rdb.table.annon'))  # .max('ts_collected').to_json()
                        curseur = curseur.filter(r.row["id_hash"].eq(pIDH))
                        curseur = curseur.update({"user_tags": tags1})
                        curseur.run(dbconn)
                    else :
                        # -- on ajoute le tag a la liste
                        curseur = r.table(self.myops.config.get('rdb.table.annon'))  # .max('ts_collected').to_json()
                        curseur = curseur.filter(r.row["id_hash"].eq(pIDH))
                        curseur = curseur.update({"user_tags": r.row["user_tags"].append(pParam.lower()).distinct()})
                        curseur.run(dbconn)
            self.myops.rdb_release()
        return retourObj

    @cherrypy.expose()
    def js_imm_dump_annonce(self, pIDH=''):  # , pParam=''):
        retourObj = '<html lang="en"><head><meta charset="UTF-8"><title>Annonce</title></head><body>'
        retourObj += '<table>'
        tmpImages = ""
        tmpFin = ""

        if pIDH != '' :
            dbconn = self.myops.rdb_get_lock()
            if dbconn is not None :
                curseur = r.table(self.myops.config.get('rdb.table.annon'))  # .max('ts_collected').to_json()
                curseur = curseur.filter(r.row["id_hash"].eq(pIDH))
                curseur = curseur.run(dbconn)
                tags0 = curseur.next()

                if tags0 :
                    curseur.close()

                    # -- Le html pour les champs importants
                    retourObj += '<tr><td><b>Titre</b></td><td><b>%s</b></td></tr>\n' % tags0.get('title', 'Pas de titre...')
                    retourObj += '<tr><td><b>Localite</b></td><td>%s - %s</td></tr>\n' % (tags0.get('localite_stz', 'Inconnu'), tags0.get('codepostal', '00000'))
                    retourObj += '<tr><td><b>Prix</b></td><td>%s</td></tr>\n' % tags0.get('price', '0')
                    retourObj += '<tr><td><b>Auteur</b></td><td>%s</td></tr>\n' % tags0.get('uploadby', 'Inconnu')
                    retourObj += '<tr><td><b>Description</b></td><td>%s</td></tr>\n' % tags0.get('description', 'Vide')

                    # -- Le html pour les images
                    for cle in sorted(tags0) :
                        if tags0[cle] != '' and tags0[cle] != '0' :
                            if 'images_ids' in cle :
                                for img_id in tags0[cle] :
                                    lien = "./js_imm_image?pID=%s" % img_id
                                    tmpImages += '<img src="%s" alt="">&nbsp;' % lien
                            elif cle not in ['codepostal', 'description', 'title', 'localite_stz', 'price', 'uploadby']:
                                tmpFin += '<tr><td>'
                                tmpFin += cle + "</td><td>" + str(tags0[cle]) + "</td></tr>"

                    if len(tmpImages) > 3 :
                        retourObj += '</table><br>' + tmpImages + '<table>'

                    # -- Le html pour les autres champs
                    if len(tmpFin) > 3 :
                        retourObj += tmpFin

            self.myops.rdb_release()
        retourObj += '</table>\n'
        retourObj += "</body></html>"
        return retourObj




if __name__ == '__main__':
    # --- Logs Definition  logging.Logger.manager.loggerDict.keys()
    Level_of_logs = level=logging.INFO
    logging.addLevelName(logging.DEBUG-2, 'DEBUG_DETAILS') # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.basicConfig(level=logging.DEBUG, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(name)s|%(levelname)s|%(asctime)s | %(message)s")  # %(thread)d %(funcName)s L%(lineno)d

    # -- PATH
    if 'websrv' in Path.cwd().parts[-1] :
        os.chdir("..")
    elif 'github' in Path.cwd().parts[-1] :
        os.chdir("./hdh")
    sys.path.append('./')
    logging.info("Starting from %s" % str(os.getcwd()))

    # -------- CherryPy et web statique
    ops = Ops4app(appli_uname='websrv.static')
    cherrypy.config.update({'server.socket_port': ops.config.get('network.port'), 'server.socket_host': '0.0.0.0',
                            'log.screen': False , 'log.access_file': '' , 'log.error_file': '',
                            'engine.autoreload.on': False  # Sinon le server se relance des qu'un fichier py est modifie...
                            })

    # -------- SERVER ROOT --------
    config_root= { '/' :            { 'tools.staticdir.on'  : True, 'tools.staticdir.index'     : "index.html", 'tools.staticdir.dir' : Path().cwd().joinpath("websrv").joinpath("wstatic").as_posix() } }
    cherrypy.tree.mount(ServStatic(), "/", config_root)


    # -------- SERVER VIDS --------
    config_vids = { '/' :            { 'tools.staticdir.on'  : True, 'tools.staticdir.index'     : "index.html", 'tools.staticdir.dir' : Path().cwd().joinpath("websrv").joinpath("wvid").as_posix(),
                                       'tools.sessions.on'  : True } }
    cherrypy.tree.mount(ServVid(), "/vid", config_vids)

    # -------- SERVER IMMO --------
    config_immo = { '/' :            { 'tools.staticdir.on'  : True, 'tools.staticdir.index'     : "index.html", 'tools.staticdir.dir' : Path().cwd().joinpath("websrv").joinpath("wimm").as_posix() } }
    cherrypy.tree.mount(ServImm(), "/imm", config_immo)

    # --- Loglevel pour CherryPy : A FAIRE ICI, une fois les serveurs mounted et avant le start
    # logging.getLogger('cherrypy.access').setLevel(logging.WARNING)
    # logging.getLogger('cherrypy.error').setLevel(logging.WARNING)
    for log_mgt in logging.Logger.manager.loggerDict.keys() :
        if 'cherrypy.access' in log_mgt :
            logging.getLogger(log_mgt).setLevel(logging.WARNING)

    # -------- Lancement --------
    cherrypy.engine.start()
    cherrypy.engine.block()

