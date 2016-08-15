// http://underscorejs.org/
// http://selectize.github.io/selectize.js/
// https://github.com/selectize/selectize.js/blob/master/docs/usage.md
// http://www.w3schools.com/js/default.asp
// https://datatables.net/
var nbrefresh   = 0 ;
var p_nom       = "" ;
var dict_table_stats = {} ;
var data4tbl = [] ;
var madatatable ;

// ----- Changing user
function button_change_user(username)  {
    if ( localStorage.getItem('current_user') != username ) {
        localStorage.setItem('current_user', username) ;
    }
    init_menu_one() ;
}

// ------ bouton load
function button_load() {
    localStorage.setItem(localStorage.getItem('current_user')+'_nb_days_history', $('#input_nbdays').val()) ;
    if ( nbrefresh == 0 ) {
        init_menu_two() ;       // Fresh page : on continue a creer le menu et les objets
    } else {
        localStorage.setItem(localStorage.getItem('current_user')+'_reload', "1") ;
        location.reload(forceGet=true);      // C'est un reload d'une page deja remplie, on fait un refresh global pour reinit de tout
    }
}

// ------ Refresh ou creation de la datatable
function button_filter() {
    localStorage.setItem(localStorage.getItem('current_user')+'_liste_str_tags_show', $('#input_tags_show').val())
    localStorage.setItem(localStorage.getItem('current_user')+'_liste_str_tags_hide', $('#input_tags_hide').val())
    if (nbrefresh == 0) {
        creation_datatable() ;
    }
    else {
        madatatable.draw(true) ;
    }
    nbrefresh = nbrefresh + 1
}

// ----- Save d'un article
function button_sav_tags_obj(username, doc_id, input_id_selectize, todel=false)  {
    var tags_new = $('#'+input_id_selectize).val() ;
    if ( todel == true ) {
        if ( tags_new != "" ) {
            tags_new += ",---" ;
        } else {
            tags_new = "---" ;
        }
        // On ajoute aussi dans le selectize de la page en direct
        var selectize_tags_ligne = $('#'+input_id_selectize)[0].selectize ;
        selectize_tags_ligne.addItem("---") ;
    }
    //console.log("Update = " + tags_new) ;
    $.ajax({
        "url": "./upd_obj_tags",
        "data": {
            "usr_uname" : username,
            "object_id" : doc_id,
            "str_tags_comma" : tags_new
        }
    });
}

// ----- Tag from show to hide
function tag_from_show_to_hide(itemname) {
    var selectize_tags = $("#input_tags_hide")[0].selectize ; // normalement : tous les items dans dans la liste generale des options : selectize_tags.addOption({ text:itemname, value: itemname });
    selectize_tags.addItem(itemname) ;
    var selectize_tags = $("#input_tags_show")[0].selectize ;
    selectize_tags.removeItem(itemname) ;
}

// ----- Tag from hide to show
function tag_from_hide_to_show(itemname) {
    var selectize_tags = $("#input_tags_show")[0].selectize ;
    selectize_tags.addItem(itemname) ;
    var selectize_tags = $("#input_tags_hide")[0].selectize ; // normalement : tous les items dans dans la liste generale des options : selectize_tags.addOption({ text:itemname, value: itemname });
    selectize_tags.removeItem(itemname) ;
}

// ----- Fill in user button
function init_menu_one() {
    // -- Button user from the current_user in Local store
    $('#user_button').html(localStorage.getItem('current_user') + ' <span class="caret"></span>')

    // -- Valeur history si dans la session
    if ( localStorage.getItem(localStorage.getItem('current_user')+'_nb_days_history') == null ) {
        localStorage.setItem(localStorage.getItem('current_user')+'_nb_days_history', 7)     // SI Rien dans localstorage ALORS ON assigne le parametre de l'URL
    }
    $('#input_nbdays').val(localStorage.getItem(localStorage.getItem('current_user')+'_nb_days_history')) ;

    if ( localStorage.getItem(localStorage.getItem('current_user')+'_reload') == '1' ) {
        localStorage.setItem(localStorage.getItem('current_user')+'_reload', "2") ;
        init_menu_two() ;
    }
}

// ------ Fill in menu line 2
function init_menu_two() {
    // ------ Recup des stats
    $.getJSON(url="./get_stats", data={"nb_days"    : $('#input_nbdays').val(),
                                       "usr_uname"  : localStorage.getItem('current_user') },
              success=function( data )
    {
        dict_table_stats = data ; // Object {DateMin: "16-03-27 23:31", DateMax: "16-08-13 19:03", CountAll: 643, CountSelected: 25}
        // --- Update de la GUI
        text_dates  = dict_table_stats['DateMin'].split(' ')[0] ; // + dict_table_stats['DateMin'].split(' ')[1] ;
        text_dates  += "&nbsp;|&nbsp;" + "<b>"+dict_table_stats['DateMinSelect'].split(' ')[0]+"</b>" ; // + dict_table_stats['DateMax'].split(' ')[1] ;
        text_dates  += "&nbsp;|&nbsp;" + "<b>"+dict_table_stats['DateMax'].split(' ')[0]+"</b>" ; // + dict_table_stats['DateMax'].split(' ')[1] ;
        text_counts = "<b>"+dict_table_stats['CountSelected'] + "</b> on <b>" + dict_table_stats['CountAll'] + "</b>"
        $('#intervaldates').html(text_dates)
        $('#labelnbdocs').html(text_counts)

        // ------ Recup des stats
        $.getJSON(url="./get_tags", data={"usr_uname" : localStorage.getItem('current_user') },
                  success=function( liste_arr_tags )
        {
            // console.log("Tags reload : " + liste_arr_tags) ;
            // --- Recup des tags SHOW et HIDE depuis le cache local
            liste_str_tags_show = "" ; // Liste de tags tag1,tag2,tag3...
            if ( localStorage.getItem(localStorage.getItem('current_user')+'_liste_str_tags_show') != null ) { liste_str_tags_show = localStorage.getItem(localStorage.getItem('current_user')+'_liste_str_tags_show') ; }
            liste_str_tags_hide = "---" ; // Liste de tags tag1,tag2,tag3...
            if ( localStorage.getItem(localStorage.getItem('current_user')+'_liste_str_tags_hide') != null ) { liste_str_tags_hide = localStorage.getItem(localStorage.getItem('current_user')+'_liste_str_tags_hide') ; }
            // console.log("Tags from local : " + liste_str_tags_show + " | " + liste_str_tags_hide)

            // parsing liste HIDE
            liste_arr_tags_hide = liste_str_tags_hide.split(",") ;
            if ( (liste_arr_tags_hide.length > 0) && (liste_arr_tags_hide[0] != '') ) {
                // tags present dans hide mais pas dans la liste globale recue -> a enlever de hide
                tags_a_eff = _.difference(liste_arr_tags_hide, liste_arr_tags) ; // Soustraction de liste : returns the values from array that are not present in the other arrays
                liste_arr_tags_hide = _.difference(liste_arr_tags_hide, tags_a_eff) ;
                // console.log("TAGS HIDE CLEANED : ", liste_arr_tags_hide) ;
            }

            // parsing liste SHOW
            liste_arr_tags_show = liste_str_tags_show.split(",")
            if ( (liste_arr_tags_show.length > 0) && (liste_arr_tags_show[0] != '') ) {
                // tags present dans show mais pas dans la liste globale recue -> a enlever de show
                tags_a_eff = _.difference(liste_arr_tags_show, liste_arr_tags) ; // returns the values from array that are not present in the other arrays
                liste_arr_tags_show = _.difference(liste_arr_tags_show, tags_a_eff) ;
                // console.log("TAGS SHOW CLEANED : ", liste_arr_tags_show) ;
            }

            // verif si tag dans show et hide, on enleve du hide
            if ( _.intersection(liste_arr_tags_show,liste_arr_tags_hide).length > 0 ) {
                liste_arr_tags_hide = _.difference(liste_arr_tags_hide, liste_arr_tags_show) ;
            }

            // tags recus inconnus de hide et de show -> a ajouter dans show
            new_tags = _.difference(liste_arr_tags, _.union(liste_arr_tags_show, liste_arr_tags_hide)) ;
            //console.log("NEW TAGS DANS SHOW : ", new_tags) ;
            liste_arr_tags_show = _.union(liste_arr_tags_show, new_tags) ;

            liste_tags_all_selectize = [] ;
            fLen = liste_arr_tags.length ;
            for (i = 0; i < fLen; i++) {
                liste_tags_all_selectize.push({'value': liste_arr_tags[i], 'text': liste_arr_tags[i]}) ;
            }
            $('#input_tags_show').selectize({
                plugins: ['restore_on_backspace','remove_button', 'drag_drop'],
                delimiter: ',',
                options : liste_tags_all_selectize,             //options : [{value:'choix1', text:'choix1'}, {value:'choix2', text:'choix2'}],
                items : liste_arr_tags_show,
                persist: true,
                create: false, // function(input) { return { value: input, text: input } },
                onItemRemove: function(value)           { tag_from_show_to_hide(value) ; }, // sera appele sur chaque item en cas de delete de selection
                onItemAdd:    function(value, $item)    { tag_from_hide_to_show(value) ; }
            });

            $('#input_tags_hide').selectize({
                plugins: ['restore_on_backspace','remove_button', 'drag_drop'],
                delimiter: ',',
                options : liste_tags_all_selectize,             //options : [{value:'choix1', text:'choix1'}, {value:'choix2', text:'choix2'}],
                items : liste_arr_tags_hide,
                persist: true,
                create: false, // function(input) { return { value: input, text: input } }
                onItemRemove: function(value)           { tag_from_hide_to_show(value) ; },
                onItemAdd:    function(value, $item)    { tag_from_show_to_hide(value) ; }
            });

            if ( localStorage.getItem(localStorage.getItem('current_user')+'_reload') == '2' ) {
                localStorage.setItem(localStorage.getItem('current_user')+'_reload', "3") ;
                button_filter() ;
            }
        } ) ;
    }) ;
}

// PARSING EST OK, MAL INTERPRETE DANS EDITEUR A CAUSE EXPR REGULIERE
// -- Load des data et creation de la table
function creation_datatable() {
    // --- Ajout fonction FILTRE de plus (PUSH) qui sera testee pour afficher ou pas chaque ligne de la table : TAGS USER
    $.fn.dataTable.ext.search.push(function( settings, data, dataIndex ) {
        // data = le array de la ligne, donc data[0], data[1] ... pour les valeur de chaque champ
        var pos1 = data[4].indexOf(").selectize(",0) ;
        var pos2 = data[4].indexOf("items : [",pos1) + 9 ;
        var pos3 = data[4].indexOf("]",pos2) ; // "" ou "'test','test2'" avec les ' dedans
        var tags_ligne_str = data[4].substring(pos2,pos3).replace(/'/g,"") ;
        if ( tags_ligne_str == "" ) {
            return true ; // Par defaut on affiche les lignes sans tag
        } else {
            // Si "---" est bien dans le HIDE (par defaut c'est le cas) et si "---" dans les tags de l'objet alors HIDE
            if ( ($('#input_tags_hide').val().indexOf("---") >= 0 ) && (tags_ligne_str.indexOf("---") >= 0) ) {
                return false ;  // On n'affiche pas les tags "---" correspondant au "discarded"
            } else {
                // S'il y a des tags pour la ligne, on compare a ceux fixes dans le filtre
                var tags_ligne_arr = tags_ligne_str.split(',') ;
                var tags_a_montrer_arr = $('#input_tags_show').val().split(',') ;

                if ( _.intersection(tags_ligne_arr,tags_a_montrer_arr).length > 0 ) {
                    return true ;
                } else {
                    return false ;
                }
            }
        }
    } );

    // --- Creation de la Datatable
    textaj = "./get_liste?usr_uname=" + localStorage.getItem('current_user') + "&nb_days=" + $('#input_nbdays').val()
    madatatable = $('#tbl_liste').DataTable( {
        "ajax"          : textaj,
        "info"          : true,
        "lengthChange"  : true,
        "paging"        : true,
        "pageLength"    : 15,
        "ordering"      : true,
        "order"         : [[ 0, 'desc' ], [ 1, 'asc' ]],

        "columns"   : [
            { "data": "ts_updated" },
            { "data": "localite" },
            { "data": "price" },
            { "data": "surface" },
            { "data": "title" },
            { "data": "commandes" },
            { "data": "description" }
        ]
    } ) ;
}

// --- Quand la page est prete
$(document).ready(function() {

    // -- Si pas le user dans le local store, on assign par defaut
    if ( localStorage.getItem('current_user') == null ) {
        // -- Recup Parametres HTML
        p_nom = location.search.split('username=')[1]
        if ( !( p_nom == "aur" || p_nom == "jit" ) ) {
            p_nom = "john" ;
        }
        localStorage.setItem('current_user', p_nom)     // SI Rien dans localstorage ALORS ON assigne le parametre de l'URL
        //console.log("New session " + localStorage.getItem('current_user'))
    } else {
        //console.log("Existing session " + localStorage.getItem('current_user'))
    }

    // -- Lancement de la construction de la GUI
    init_menu_one()
} ) ;
