// http://underscorejs.org/
// http://selectize.github.io/selectize.js/
// https://github.com/selectize/selectize.js/blob/master/docs/usage.md
// http://www.w3schools.com/js/default.asp
// https://datatables.net/
var nbrefresh   = 0 ;

// -- Nouveau user selectionne : on MaJ le serveur et on reload la page
function user_selected()  {
    $.ajax({
        "url": "./js_updt_session",
        "data": {
            "ch_key" : "selected_usr",
            "ch_value" : $('#input_user').val(),
        }
    });
    //console.log("update " + $('#input_user').val())
    location.reload();
}

// -- Load des data et creation de la barre du haut
function init_barre_haut() {
    $.getJSON("./json_tags_systm", function( data )
    {
        // console.log("JSON OK") ;
        $('#input_tags').selectize({
            plugins: ['restore_on_backspace','remove_button', 'drag_drop'],
            delimiter: ',',
            options : data,             //options : [{value:'choix1', text:'choix1'}, {value:'choix2', text:'choix2'}],
            items : [],
            persist: true,
            create: function(input) { return { value: input, text: input } },
            onChange        : eventHandler('onChange'),
        });
    })

    $.getJSON(url="./json_tags_user", data={"usr_uname" : $('#input_user').val()}, success=function( data2 )
    {
        $('#input_tags_usr').selectize({
            plugins: ['restore_on_backspace','remove_button', 'drag_drop'],
            delimiter: ',',
            options : data2,             //options : [{value:'choix1', text:'choix1'}, {value:'choix2', text:'choix2'}],
            items : [],
            persist: true,
            create: function(input) { return { value: input, text: input } },
            onChange        : eventHandler('onChange'),
        });
    })

    // --- LE CHAMP DE SELECTION DES TAGS
    var eventHandler = function(lavaleur) {
        return function() {
        //console.log(lavaleur, arguments);
        };
    };
}

// -- Update des tags user
function button_save_object(usr_uname, obj_id, id_dom) {
    //console.log(usr_uname, obj_id, $('#'+id_dom).val())
    $.ajax({
        "url": "./js_updt_tags_usr",
        "data": {
            "usr_uname" : usr_uname,
            "object_id" : obj_id,
            "str_tags_comma" : $('#'+id_dom).val()
        }
    });
}

// -- Load des data et creation de la table
function creation_datatable() {
    // On ajoute une fonction FILTRE de plus (PUSH) qui sera testee pour afficher ou pas chaque ligne de la table : DATE
    $.fn.dataTable.ext.search.push(
        function( settings, data, dataIndex ) {
            var min = $('#input_dateMin').val() ;
            var max = $('#input_dateMax').val() ;
            var dateligne = data[0] || "" ;

            if (( dateligne >= min ) && (dateligne <= max) )
            {
                return true;
            }
            return false;
        }
    );
    // On ajoute une fonction FILTRE de plus (PUSH) qui sera testee pour afficher ou pas chaque ligne de la table : TAGS SYST
    $.fn.dataTable.ext.search.push(
        function( settings, data, dataIndex ) {

            var tags_select_str = $('#input_tags').val()
            if ( tags_select_str == "" ) {
                return true ;
            }
            var tags_ligne_str = data[1] || ""
            var tags_select_liste = tags_select_str.split(",")
            var tags_ligne_liste = tags_ligne_str.split(" ")
            var encommun = _.intersection(tags_select_liste,tags_ligne_liste)

            if ( encommun.length > 0 )
            {
                return true;
            }
            return false;
        }
    );
    // On ajoute une fonction FILTRE de plus (PUSH) qui sera testee pour afficher ou pas chaque ligne de la table : TAGS USER
    $.fn.dataTable.ext.search.push(
        function( settings, data, dataIndex ) {
            var tags_select_str = $('#input_tags_usr').val()
            if ( tags_select_str == "" ) {
                return true ;
            } else {
                var tags_ligne_str = data[2] || "" ;
                var tags_select_liste = tags_select_str.split(",") ;
                var tlen = tags_select_liste.length ;
                for (i = 0; i < tlen; i++) {
                    if ( tags_ligne_str.search(tags_select_liste[i]) > -1 ) {
                        return true ;
                    }
                }
                return false;
            }
        }
    );

    madatatable = $('#tbl_liste').DataTable( {
        //"ajax" : "./json_liste",
        "ajax": {
            "url": "./json_liste",
            "data": {
            //"usr_uname": $('#input_user').val()
            }
        },
        "info"          : true,
        "lengthChange"  : true,
        "paging"        : true,
        "pageLength"    : 30,
        "ordering"      : true,
        "order"         : [[ 0, 'desc' ], [ 1, 'asc' ]],
        "autoWidth"     : false,
        "columns"       : [
            { "data": "file_ts_creation",   "width": "7%" },
            { "data": "file_tags",          "width": "18%" },
            { "data": "tags_usr",           "width": "18%" },
            { "data": "media_title"},
            { "data": "langs",              "width": "8%" },
            { "data": "file_size_GB",       "width": "6%" },
            { "data": "button_save",        "width": "6%" }
        ]
    } ) ;
}

// -- Refresh ou creation de la datatable
function button_refresh() {
    if (nbrefresh == 0) {
        creation_datatable() ;
    }
    else {
        var dtable = $('#tbl_liste').DataTable();
        dtable.draw();
    }
    nbrefresh = nbrefresh + 1
}

// --- Quand la page est prete
$(document).ready(function() {
    // -- Get user from session server
    $.getJSON(url="./js_read_session", data={"ch_key" : "selected_usr"}, success=function( data2 )
    {
        if ( data2 != "" ) {  // Si user dans session server : on vient de lancer un reload de la page
            $('#input_user').val(data2)
        } else {
            user_selected()   // Si rien dans session alors on y pousse le user par defaut
        }
        // -- Launch init of the menu bar
        init_barre_haut()
    })
} ) ;
