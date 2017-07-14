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

    // L.marker([52.51476, 13.34981]).addTo(mymap)
    //     .bindPopup("<b>Grosser Stern</b>").openPopup();

    var popup = L.popup();

    function onMapClick(e) {
        popup
            .setLatLng(e.latlng)
            .setContent(e.latlng.toString())
            .openOn(mymap);
    }

    mymap.on('click', onMapClick);

    function block(text) {
        var blockMeta = {
            /*
             message displayed when blocking (use null for no message)
             */
            message: '<h1>'+text+'</h1>',
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

    function toGreen(data) {
        return '<span style="color:green;">'+data+'</span>';
    }
    function toRed(data) {
        return '<span style="color:red;">'+data+'</span>';
    }

    function checkInputFile() {
        var endpoint = $( "#s3_input" ).val();
        var filePath = $( "#file_input" ).val();
        var spark = $( "#spark_input" ).val();
        $.ajax({
            url: '/api/check_file?file='+filePath + '&spark='+spark + '&endpoint='+endpoint,
            beforeSend: function() {
                // setting a timeout
                $('#spark_run_label').html("Setup not ready");
                block('Checking...');
            },
            success: function(data) {
                if( data.indexOf('Exception') >= 0) {
                    // EXCEPTION
                    var highlinded = data.replace(/Exception/g, toRed("Exception"));
                    $('#spark_run_label').html(highlinded);
                } else if( data.indexOf('Error:') >= 0){
                    // ERROR
                    var highlinded = toRed(data);
                    $('#spark_run_label').html(highlinded);
                } else {
                    // OK
                    $('#spark_run_label').html(toGreen("Setup ready"));
                }
            },
            error: function(xhr) { // if error occured
                $('#spark_run_label').html(toRed("Please reupload"));
                alert("Error occured - please try again");
            },
            complete: function() {
                unblock();
            }
        });
    }

    $('#spark_run').click(function(e){
        checkInputFile();
    });

    var mapMarkers = []
    function removeMarkers() {
        for(var i = 0; i < mapMarkers.length; i++){
            mymap.removeLayer(mapMarkers[i]);
        }
    }

    function getHotspots() {
        var endpoint = $( "#s3_input" ).val();
        var filePath = $( "#file_input" ).val();
        var spark = $( "#spark_input" ).val();
        $.ajax({
            url: '/api/get_hotspots?file='+filePath + '&spark='+spark + '&endpoint='+endpoint,
            beforeSend: function() {
                // setting a timeout
                mapMarkers = [];
                block('Loading hotspots...');
            },
            success: function(data) {
                try {
                    var jsonArray = JSON.parse(data);
                    $('#read_run_label').html(toGreen("Job Finished"));

                    removeMarkers();
                    for(var i = 0; i < jsonArray.length; i++) {
                        var obj = jsonArray[i];
                        var marker = L.marker([obj.lat, obj.long]).addTo(mymap)
                            .bindPopup("<b>"+obj.id+"</b>");
                        mapMarkers.push(marker);
                    }
                } catch (e) {
                    $('#read_run_label').html(toRed("Received wrong content"));
                }
            },
            error: function(xhr) { // if error occured
                alert("Error occured - please try again");
                $('#read_run_label').text(toRed("Error"));
            },
            complete: function() {
                unblock();
            }
        });
    }

    $( "#read_run" ).click(function() {
        getHotspots();
    });

    // $( "#file_run" ).click(function() {
    //     var filePath = $( "#file_input" ).val();
    //     var jarPath = $( "#jar_input" ).val();
    //     var sparkAddress = $( "#spark_input" ).val();
    //
    //     etlHeatSpots(filePath, jarPath, sparkAddress);
    // });
    //
    // function etlHeatSpots(filePath, jarPath, sparkAddress) {
    //     $.ajax({
    //         url: '/api/extract?file=' + filePath + '&jar='+jarPath,
    //         beforeSend: function() {
    //             // setting a timeout
    //             block('Extracting...');
    //         },
    //         success: function(data) {
    //             $('#file_run_label').text(data);
    //             if( data.indexOf('Error:') >= 0){
    //                 // ERROR
    //                 unblock();
    //             } else {
    //                 // OK
    //                 unblock();
    //                 tlHeatSpots(filePath, jarPath, sparkAddress);
    //             }
    //         },
    //         error: function(xhr) { // if error occured
    //             alert("Error occured - please try again");
    //             $('#file_run_label').text("Error");
    //             unblock();
    //         }
    //     });
    // }
    //
    // function tlHeatSpots(filePath, jarPath, sparkAddress) {
    //     $.ajax({
    //         url: '/api/tl_hotspots?spark=' + sparkAddress + '&file=' + filePath + '&jar='+jarPath,
    //         beforeSend: function() {
    //             // setting a timeout
    //             block('Transforming...');
    //         },
    //         success: function(data) {
    //             if( data.indexOf('0') >= 0){
    //                 // OK
    //                 $('#file_run_label').text("Job Finished");
    //                 getHeatSpots()
    //             } else {
    //                 // ERROR - exit code non-zero
    //                 $('#file_run_label').text("Error, finished with non-zero code");
    //                 unblock();
    //             }
    //         },
    //         error: function(xhr) { // if error occured
    //             alert("Error occured - please try again");
    //             $('#file_run_label').text("Error");
    //             unblock();
    //         }
    //     });
    // }
})
