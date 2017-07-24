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
    // var popup = L.popup();

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

    // function onMapClick(e) {
    //     popup
    //         .setLatLng(e.latlng)
    //         .setContent(e.latlng.toString())
    //         .openOn(mymap);
    // }


    //
    // mymap.on('click', onMapClick);

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
    var hashmapPageRank = [];

    $("#pageranktopdropdown").on("show.bs.dropdown", function(event){
        var list = document.getElementById("pageranktop");
        while (list.firstChild) {
            list.removeChild(list.firstChild);
        }

        var sortedPageRanks = hashmapPageRank.sort();
        var endLength = Math.max(0,sortedPageRanks.length-10);
        var startLength = sortedPageRanks.length-1;
        for (var i = startLength; i >= endLength; i--){
            var li = document.createElement("li");

            var id = sortedPageRanks[i].split("-")[1];

            var optText = "Cluster "+id;
            var text = document.createTextNode(optText);
            li.appendChild(text);

            var att = document.createAttribute("id");
            att.value = id;
            li.setAttributeNode(att);

            li.classList.add("list-group-item");

            li.addEventListener('click', function(e) {
                var id = e.target.id;

                for (var i = 0; i < mapMarkers.length; i++){
                    if (mapMarkers[i].options.id == id) {
                        var markerBounds = L.latLngBounds([ mapMarkers[i].getLatLng() ]);
                        mymap.fitBounds(markerBounds, {
                            padding: [50, 50],
                            maxZoom: 18,
                            animate: true,
                            pan: {
                                duration: 1
                            }
                        });
                        break;
                    }
                }
            });

            list.appendChild(li);
        }
    });

    function destroyClickedElement(event)
    {
        document.body.removeChild(event.target);
    }

    function exportToCsv() {
        var fileContents = "";
        for (var i = 0; i < mapMarkers.length; i++){
            var options = mapMarkers[i].options;
            fileContents += options.id+"|"+options.lat+"|"+options.long
                +"|"+options.duration+"|"+options.count+"|"+options.neighborsIn.join(",")
                +"|"+options.neighborsOut.join(",")+"|"+options.outDegrees+"|"+options.inDegrees+"|"+options.pagerank+"\n";
        }

        var textToSaveAsBlob = new Blob([fileContents], {type:"text/plain"});
        var textToSaveAsURL = window.URL.createObjectURL(textToSaveAsBlob);
        var fileNameToSaveAs = "macromovements-" + Math.round(new Date().getTime()/1000) + ".result";

        var downloadLink = document.createElement("a");
        downloadLink.download = fileNameToSaveAs;
        downloadLink.innerHTML = "Download File";
        downloadLink.href = textToSaveAsURL;
        downloadLink.onclick = destroyClickedElement;
        downloadLink.style.display = "none";
        document.body.appendChild(downloadLink);

        downloadLink.click();
    }

    var button = document.getElementById('exportToCSVButton');
    button.addEventListener('click', exportToCsv);

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
                    hashmapPageRank = [];
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
                                count: obj.count
                            })
                            .addTo(mymap)
                            .on('click', showStatistics);

                        var id = obj.pagerank+"-"+obj.id;
                        hashmapPageRank.push(id);

                        hashmapMarkers[obj.id] = [obj.lat,obj.long];

                        mapMarkers.push(marker);
                    }

                } catch (e) {
                    console.log(e);
                    $(read_run_label).html(toRed("Received wrong content (see web console in browser)"));
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

    var modeInOut = true;

    function showStatistics(e) {
        console.log(e);
        var marker = e.target;

        if(currMarker !== undefined) {
            currMarker.setIcon(defaultIcon);
        }
        marker.setIcon(clickedIcon);
        var options = marker.options;

        // mymap.fitBounds(marker.getBounds());

        $('#id').text(options.id);
        $('#lat').text(options.lat);
        $('#long').text(options.long);
        $('#duration').text(options.duration);
        $('#clusterSize').text(options.count);
        $('#pagerank').text(options.pagerank);
        $('#inDeg').text(options.inDegrees);
        $('#outDeg').text(options.outDegrees);

        removePolylines();
        if (modeInOut) {
            createPolylines(marker, marker.options.neighborsIn, 'blue');
            modeInOut = false;
        } else {
            createPolylines(marker, marker.options.neighborsOut, 'red');
            modeInOut = true;
        }

        currMarker = marker;
    }

    var polylines = [];

    function removePolylines() {
         if(polylines.length > 0) {
            for(var i = 0; i < polylines.length; i++) {
                 mymap.removeLayer(polylines[i]);
            }
            polylines = [];
        }
    }

    function createPolylines(marker, neighbors, lineColor) {       
        console.log("creaing polyline for marker " + marker.getLatLng());

        var neighborCountMap = {};
        for(var i = 0; i <  neighbors.length; i++) {
            var id = neighbors[i];
            if (id in neighborCountMap) {
                neighborCountMap[id]++;
            } else {
                neighborCountMap[id] = 1;
            }
        }

        for(var i = 0; i <  neighbors.length; i++) {
            var id = neighbors[i];
            var percentage = Math.floor(neighborCountMap[id]/neighbors.length*100)+"%";
            var polyline = L.polyline([hashmapMarkers[id], marker.getLatLng()], {color: lineColor}).addTo(mymap);
            polyline.bindTooltip(percentage, {permanent: true, interactive: true}).openTooltip();
            polylines.push(polyline);
        }
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
