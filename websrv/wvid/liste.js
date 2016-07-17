// http://underscorejs.org/
// http://selectize.github.io/selectize.js/
// https://github.com/selectize/selectize.js/blob/master/docs/usage.md

function button_refresh() {
    console.log("GO PRESSE");

    var dtable = $('#tbl_liste').DataTable();
    dtable.draw();
}


$(document).ready(function() {

    // On ajoute une fonction de plus (PUSH) qui sera testee pour afficher ou pas chaque ligne de la table
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

    $.fn.dataTable.ext.search.push(
        function( settings, data, dataIndex ) {

            var tags_select_str = $('#input_tags').val()
            if ( tags_select_str == "" )
            {
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






    $('#tbl_liste').DataTable( {
    "ajax" : "./json_liste",
    "info" : true,
    "lengthChange" : true,
    "paging" : true,
    "pageLength": 30,
    "ordering" : true,
    "order" : [[ 0, 'desc' ], [ 1, 'asc' ]],

    "columns": [
        { "data": "file_ts_creation" },
        { "data": "file_tags" },
        { "data": "media_title" },
        { "data": "file_audiotracks" },
        { "data": "file_subtitles" },
        { "data": "file_size_GB" }
    ]
    } ) ;

   var eventHandler = function(lavaleur) {
        return function() {
            console.log(lavaleur, arguments);
            document.getElementById('le_label').innerHTML = document.getElementById('id_du_input').value
        };
    };

    $.getJSON("./json_tags_distinct", function( data )
    {
        // console.log("JSON OK") ;
        $('#input_tags').selectize({
            plugins: ['restore_on_backspace','remove_button', 'drag_drop'],
            delimiter: ',',
            options : data,             //options : [{value:'choix1', text:'choix1'}, {value:'choix2', text:'choix2'}],
            items : [],
            persist: true,
            create: function(input) {
                return { value: input, text: input }
            },
            onChange        : eventHandler('onChange'),
        });
    })

} ) ;