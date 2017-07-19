// jQuery
$(document).ready(function () {
    var mymap = L.map('mapid').setView([52.51476, 13.34981], 12);

    L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoibWFwYm94IiwiYSI6ImNpejY4NXVycTA2emYycXBndHRqcmZ3N3gifQ.rJcFIG214AriISLbB6B5aw', {
        maxZoom: 18,
        attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, ' +
        '<a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, ' +
        'Imagery © <a href="http://mapbox.com">Mapbox</a>',
        id: 'mapbox.streets'
    }).addTo(mymap);
    var popup = L.popup();

    var spark_input = "spark_input";
    var file_input = "file_input";
    var s3_input = "s3_input";

    // Check browser support
    if (typeof(Storage) !== "undefined") {
        // Retrieve data
        try {
            document.getElementById(spark_input).value = localStorage.getItem(spark_input);
            document.getElementById(file_input).value = localStorage.getItem(file_input);
            document.getElementById(s3_input).value = localStorage.getItem(s3_input);
            console.log("Retrieved data:" + localStorage.getItem(s3_input))
        } catch (e) {
            console.error("Cannot find saved data : " + e)
        }
    }

    function storeData() {
        if (typeof(Storage) !== "undefined") {
            try {
                localStorage.setItem(spark_input, document.getElementById(spark_input).value);
                localStorage.setItem(file_input, document.getElementById(file_input).value);
                localStorage.setItem(s3_input, document.getElementById(s3_input).value);

                console.log("Stored data:" + localStorage.getItem(s3_input))
            } catch (e) {
                console.error("Cannot store data : " + e)
            }
        }
    }

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
            message: '<h1>' + text + '</h1>',
            /*
             styles for the message when blocking; if you wish to disable
             these and use an external stylesheet then do this in your code:
             $.blockUI.defaults.css = {};
             */
            css: {border: '3px solid #a00'}
        };
        $('div.blockMe').block(blockMeta);
    }

    function unblock() {
        $('div.blockMe').unblock();
    }

    function toGreen(data) {
        return '<span style="color:green;">' + data + '</span>';
    }

    function toRed(data) {
        return '<span style="color:red;">' + data + '</span>';
    }

    var spark_run_label = "#spark_run_label";
    var read_run_label = "#read_run_label";

    function checkInputFile() {
        var endpoint = $("#" + s3_input).val();
        var filePath = $("#" + file_input).val();
        var spark = $("#" + spark_input).val();
        $.ajax({
            url: '/api/check_file?file=' + filePath + '&spark=' + spark + '&endpoint=' + endpoint,
            beforeSend: function () {
                // setting a timeout
                $(spark_run_label).html("Setup not ready");
                block('Checking...');
            },
            success: function (data) {
                if (data.indexOf('Exception') >= 0) {
                    // EXCEPTION
                    $(spark_run_label).html(data.replace(/Exception/g, toRed("Exception")));
                } else if (data.indexOf('Error:') >= 0) {
                    // ERROR
                    $(spark_run_label).html(toRed(data));
                } else {
                    // OK
                    $(spark_run_label).html(toGreen("OK"));
                }
            },
            // if error occurred
            error: function () {
                $(spark_run_label).html(toRed("ERROR"));
                alert("Error occured - please try again");
            },
            complete: function () {
                unblock();
            }
        });
    }

    var mapMarkers = [];

    function removeMarkers() {
        for (var i = 0; i < mapMarkers.length; i++) {
            mymap.removeLayer(mapMarkers[i]);
        }
    }

    function getHotspots() {
        var endpoint = $("#" + s3_input).val();
        var filePath = $("#" + file_input).val();
        var spark = $("#" + spark_input).val();
        $.ajax({
            url: '/api/get_hotspots?file=' + filePath + '&spark=' + spark + '&endpoint=' + endpoint,
            beforeSend: function () {
                // setting a timeout
                mapMarkers = [];
                block('Loading...');
            },
            success: function (data) {
                try {
                    var jsonArray = JSON.parse(data);
                    $(read_run_label).html(toGreen("Job done!"));

                    removeMarkers();
                    for (var i = 0; i < jsonArray.length; i++) {
                        var obj = jsonArray[i];
                        var marker = L.marker([obj.lat, obj.long],
                            {id: obj.id, lat: obj.lat, long: obj.long, duration: obj.duration})
                            .addTo(mymap)
                            .on('click', showStatistics);
                        // .bindPopup("<b>" + obj.id + "</b>" + "</br><b>" + obj.duration + "</b>");
                        mapMarkers.push(marker);
                    }
                } catch (e) {
                    $(read_run_label).html(toRed("Received wrong content"));
                }
            },
            error: function (xhr) { // if error occured
                alert("Error occured - please try again");
                $(read_run_label).text(toRed("Error"));
            },
            complete: function () {
                unblock();
            }
        });
    }

    <!-- Menu Toggle Script -->
    $("#menu-toggle").click(function (e) {
        console.log("clicked menu");
        e.preventDefault();
        $("#wrapper").toggleClass("toggled");
    });
    $("#form1").submit(function (e) {
        return false;
    });

    function showStatistics(e) {
        var marker = e.target.options;
        $('#id').text(marker.id);
        $('#lat').text(marker.lat);
        $('#long').text(marker.long);
        $('#duration').text(marker.duration);
    }


    $('#spark_run').click(function () {
        console.log("Clicked spark run");
        storeData();
        checkInputFile();
    });

    $("#read_run").click(function () {
        storeData();
        getHotspots();
    });

})
;
