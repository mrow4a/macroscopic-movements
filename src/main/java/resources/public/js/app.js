// jQuery
$(document).ready( function () {
    var mymap = L.map('mapid').setView([52.51476, 13.34981], 12);

    L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoibWFwYm94IiwiYSI6ImNpejY4NXVycTA2emYycXBndHRqcmZ3N3gifQ.rJcFIG214AriISLbB6B5aw', {
        maxZoom: 18,
        attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, ' +
        '<a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, ' +
        'Imagery © <a href="http://mapbox.com">Mapbox</a>',
        id: 'mapbox.streets'
    }).addTo(mymap);

    L.marker([52.51476, 13.34981]).addTo(mymap)
        .bindPopup("<b>Grosser Stern</b>").openPopup();

    var popup = L.popup();

    function onMapClick(e) {
        popup
            .setLatLng(e.latlng)
            .setContent(e.latlng.toString())
            .openOn(mymap);
    }

    mymap.on('click', onMapClick);

    function block() {
        var blockMeta = {
            /*
             message displayed when blocking (use null for no message)
             */
            message: '<h1>Processing...</h1>',
            /*
             styles for the message when blocking; if you wish to disable
             these and use an external stylesheet then do this in your code:
             $.blockUI.defaults.css = {};
             */
            css: { border: '3px solid #a00' }
        };
        $('div.blockMe').block(blockMeta);
    }

    function unblock() {
        $('div.blockMe').unblock();
    }

    $( "#file_check" ).click(function() {
        var text = $( "#file_input" ).val();
        $.ajax({
            url: '/api/check_file?file=' + text,
            beforeSend: function() {
                // setting a timeout
                block();
            },
            success: function(data) {
                $('#file_check_label').text(data);
                if( data.indexOf('Error:') >= 0){
                    // ERROR
                } else {
                    // OK
                }
            },
            error: function(xhr) { // if error occured
                alert("Error occured - please try again");
            },
            complete: function() {
                unblock();
            }
        });
    });

    $( "#file_run" ).click(function() {
        var filePath = $( "#file_input" ).val();
        var sparkAddress = $( "#spark_input" ).val();
        $.ajax({
            url: '/api/get_hotspots?file=' + filePath+ '&spark='+sparkAddress,
            beforeSend: function() {
                // setting a timeout
                block();
            },
            success: function(data) {
                if( data.indexOf('Error:') >= 0){
                    // ERROR
                    $('#file_run_label').text(data);
                } else {
                    // OK
                    $('#file_run_label').text("OK");
                    alert(data);
                }
            },
            error: function(xhr) { // if error occured
                alert("Error occured - please try again");
            },
            complete: function() {
                unblock();
            }
        });
    });

})
