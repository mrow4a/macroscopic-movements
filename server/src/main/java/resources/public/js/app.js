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
    var hashmapMarkers = {};

    function removeMarkers() {
        for (var i = 0; i < mapMarkers.length; i++) {
            mymap.removeLayer(mapMarkers[i]);
        }
        mapMarkers.length = 0;
    }

    function getHotspots() {
        var endpoint = $("#" + s3_input).val();
        var filePath = $("#" + file_input).val();
        var spark = $("#" + spark_input).val();
        $.ajax({
            url: '/api/get_hotspots?file=' + filePath + '&spark=' + spark + '&endpoint=' + endpoint,
            beforeSend: function () {
                // setting a timeout

                removeMarkers();
                mapMarkers = [];
                block('Loading...');
            },
            success: function (data) {
                try {
                    var jsonArray = JSON.parse(data);
                    $(read_run_label).html(toGreen("Job done!"));

                    hashmapMarkers = {};
                    removeMarkers();
                    for (var i = 0; i < jsonArray.length; i++) {
                        var obj = jsonArray[i];
                        var marker = L.marker([obj.lat, obj.long],
                            {
                                icon: defaultIcon,
                                id: obj.id,
                                lat: obj.lat, // .toFixed crashes
                                long: obj.long,
                                duration: Math.round((parseInt(obj.duration)/3600) * 100) / 100,
                                neighborsIn: obj.neighborsin,
                                neighborsOut : obj.neighborsout,
                                outDegrees: obj.outdegrees, // TODO add
                                inDegrees: obj.indegrees,
                                pagerank: obj.pagerank,
                                clusterSize: Math.round(obj.clusterSize)
                            })
                            .addTo(mymap)
                            .on('click', showStatistics);
                        // .bindPopup("<b>" + obj.id + "</b>" + "</br><b>" + obj.duration + "</b>");

                        hashmapMarkers[obj.id] = [obj.lat,obj.long];

                        mapMarkers.push(marker);
                    }
                    defaultIcon = marker.getIcon();

                } catch (e) {
                    console.log(e);
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


    var LeafIcon = L.Icon.extend({
        options: {
            iconSize: [25, 41]
        }
    });
    var clickedIcon = new LeafIcon({
        iconUrl: 'https://camo.githubusercontent.com/82f10ed32b4252324cd714ffbd31cedc47b1cc72/68747470733a2f2f7261772e6769746875622e636f6d2f706f696e7468692f6c6561666c65742d636f6c6f722d6d61726b6572732f6d61737465722f696d672f6d61726b65722d69636f6e2d32782d79656c6c6f772e706e673f7261773d74727565'
    });
    var defaultIcon = new LeafIcon({
        iconUrl: 'https://camo.githubusercontent.com/1c5e8242c57d3b712ed654e3bc9fe2f0717a7200/68747470733a2f2f7261772e6769746875622e636f6d2f706f696e7468692f6c6561666c65742d636f6c6f722d6d61726b6572732f6d61737465722f696d672f6d61726b65722d69636f6e2d32782d626c75652e706e673f7261773d74727565'
    });
    var currMarker;

    function showStatistics(e) {
        console.log(e);
        var marker = e.target;

        if(currMarker !== undefined) {
            currMarker.setIcon(defaultIcon);
        }
        marker.setIcon(clickedIcon);
        var options = marker.options;

        // mymap.fitBounds(marker.getBounds());
        // if(marker !== undefined) {
        //     marker.options.icon = marker.options.icon;
        // }
        // marker.setIcon(yellowIcon);
        // marker.addTo(mymap);
        // var marker2 = L.marker([options.long, options.lat]).addTo(mymap);

        $('#id').text(options.id);
        $('#lat').text(options.lat);
        $('#long').text(options.long);
        $('#duration').text(options.duration);
        $('#clusterSize').text(options.clusterSize);

        createPolylines(marker);

        currMarker = marker;
    }

    var polyline;

    function createPolylines(marker) {
        for(var i = 0; i < marker.options.neighborsOut.length; i++) {
            var id = marker.options.neighborsOut[i];
            polyline = L.polyline([hashmapMarkers[id], marker.getLatLng()], {color: 'red'}).addTo(mymap);
        }

        //latlngs.push(marker.getLatLng());
        //latlngs.push(marker2.getLatLng());

        //   mymap.fitBounds(polyline.getBounds());

    }

    $('#spark_run').click(function () {
        console.log("Clicked spark run");
        checkInputFile();
        storeData();
    });

    $("#read_run").click(function () {
        getHotspots();
        storeData();
    });

})
;
