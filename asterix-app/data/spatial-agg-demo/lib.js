function main() {
    // setup query dialog box
    var dialog = $("#dialog").dialog({
        width: "auto",
        title: "AQL Query"
    });
    dialog.dialog("close");
    $("input[name=showquery]:button").click(function (event) {
        $("#dialog").dialog("open");
    });
    $("#location-button").removeAttr("checked");
    $("#selection-button").attr("checked", "checked");

    // setup grid sliders
    sliderOptions = {
        max: 20,
        min: .1,
        step: .1,
        value: 3.0,
        slidechange: updateSliderDisplay,
        slide: updateSliderDisplay,
        start: updateSliderDisplay,
        stop: updateSliderDisplay
    };
    $(".grid-slider").slider(sliderOptions);

    function updateSliderDisplay(event, ui) {
        if (event.target.id == "grid-lat-slider") {
            $("#gridlat").text(""+ui.value);
        } else {
            $("#gridlng").text(""+ui.value);
        }
    };

    // setup datepickers
    var dateOptions = {
        dateFormat: "yy-mm-dd",
        defaultDate: "2011-05-15",
        navigationAsDateFormat: true,
        constrainInput: true
    };
    var start_dp = $("#start-date").datepicker(dateOptions);
    start_dp.val(dateOptions.defaultDate);
    dateOptions['defaultDate'] = "2011-05-16";
    var end_dp= $("#end-date").datepicker(dateOptions);
    end_dp.val(dateOptions.defaultDate);

    // setup map
    var mapOptions = {
        center: new google.maps.LatLng(37.0625, -95.6770),
        zoom: 4,
        mapTypeId: google.maps.MapTypeId.ROADMAP,
        streetViewControl: false,
        draggable : false
    };
    var map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);

    // setup location autocomplete
    var input = document.getElementById('location-text-box');
    var autocomplete = new google.maps.places.Autocomplete(input);
    autocomplete.bindTo('bounds', map);

    google.maps.event.addListener(autocomplete, 'place_changed', function() {
        var place = autocomplete.getPlace();
        if (place.geometry.viewport) {
            map.fitBounds(place.geometry.viewport);
        } else {
            map.setCenter(place.geometry.location);
            map.setZoom(17);  // Why 17? Because it looks good.
        }
        var address = '';
        if (place.address_components) {
            address = [(place.address_components[0] && place.address_components[0].short_name || ''),
              (place.address_components[1] && place.address_components[1].short_name || ''),
              (place.address_components[2] && place.address_components[2].short_name || '') ].join(' ');
        }
    });

    // handle selection rectangle drawing
    var shouldDraw;
    var startLatLng;
    var selectionRect;
    var selectionRadio = $("#selection-button");
    var firstClick = true;
    
    google.maps.event.addListener(map, 'mousedown', function (event) {
        // only allow drawing if selection is selected
        if (selectionRadio.attr("checked") == "checked") {
            startLatLng = event.latLng;
            shouldDraw = true;
        }
    });

    google.maps.event.addListener(map, 'mousemove', drawRect);
    function drawRect (event) {
        if (shouldDraw) {
            clearMap();
            if (!selectionRect) {
                var selectionRectOpts = {
                    bounds: new google.maps.LatLngBounds(startLatLng, event.latLng),
                    map: map,
                    strokeWeight: 1,
                    strokeColor: "blue",
                    fillColor: "blue"
                };
                selectionRect = new google.maps.Rectangle(selectionRectOpts);
                google.maps.event.addListener(selectionRect, 'mouseup', function () {
                    shouldDraw = false;
                    submitQuery();
                });
            } else {
                if (startLatLng.lng() < event.latLng.lng()) {
                    selectionRect.setBounds(new google.maps.LatLngBounds(startLatLng, event.latLng));
                } else {
                    selectionRect.setBounds(new google.maps.LatLngBounds(event.latLng, startLatLng));
                }
            }
        }
    };

    // toggle location search style: by location or by map selection
    $("input[name=location-option]:radio").click(function (btn) {
        if (btn.target.id == "selection-button") {
            $("#location-text-box").attr("disabled", "disabled");
            if (selectionRect) {
                selectionRect.setMap(map);
            }
        } else {
            $("#location-text-box").removeAttr("disabled");
            if (selectionRect) {
                selectionRect.setMap(null);
            }
        }
    });


    // handle ajax calls
    $("#submit-query").submit(submitQuery);
    function submitQuery (event) {
        $("input[name=submit]:submit").spinner({
            position: 'center',
            hide: true
        });
        if (event) {
            event.preventDefault();
        }

        // gather all of the data from the inputs
        var kwterm = $("#keyword-textbox").val();
        var startdp = $("#start-date").datepicker("getDate");
        var enddp = $("#end-date").datepicker("getDate");
        var startdt = $.datepicker.formatDate("yy-mm-dd", startdp)+"T00:00:00Z";
        var enddt = $.datepicker.formatDate("yy-mm-dd", enddp)+"T23:59:59Z";

        var formData = {
            "keyword": kwterm,
            "startdt": startdt,
            "enddt": enddt,
            "gridlat": $("#grid-lat-slider").slider("value"),
            "gridlng": $("#grid-lng-slider").slider("value")
        };

        var bounds;

        if (selectionRect) {
            bounds = selectionRect.getBounds();
        } else {
            bounds = map.getBounds();
        }

        formData["swLat"] = bounds.getSouthWest().lat();
        formData["swLng"] = bounds.getSouthWest().lng();
        formData["neLat"] = bounds.getNorthEast().lat();
        formData["neLng"] = bounds.getNorthEast().lng();

        // send ajax request to servlet
        $.ajax({
            type: 'POST',
            url: "http://localhost:14602",
            data: formData,
            dataType: "json",
            success: updateUIWithData, 
            error: function (xhr, ajaxOptions, thrownError) {
                alert(xhr.statusText);
                alert(xhr.responseText);
                alert(xhr.status);
                alert(thrownError);
            }
        });
    };

    // handle UI updates
    var cells = [];

    function updateUIWithData(data) {
        $("input[name=submit]:submit").spinner("remove");
        $("#dialog").html("<pre>"+data[data.length-2].query+"</pre>");
        if (selectionRect) {
            selectionRect.setMap(null);
            selectionRect = null;
        }
        for (var i = 0; i < cells.length; i++) {
            cells[i].setMap(null);
        }
        cells = [];

        var cell;
        var cellOptions;
        var maxcount = data[data.length - 1].maxcount;

        for (var i = 0; i < data.length - 2; i++) {
            cellOptions = {
                clickable: true,
                map: map,
                paths: [
                    new google.maps.LatLng(data[i].x1, data[i].y1),
                    new google.maps.LatLng(data[i].x2, data[i].y1),
                    new google.maps.LatLng(data[i].x2, data[i].y2),
                    new google.maps.LatLng(data[i].x1, data[i].y2),
                    new google.maps.LatLng(data[i].x1, data[i].y1)
                ],
                strokeColor: "black",
                strokeWeight: 1,
                strokeOpacity: 1,
                fillOpacity: .5,
                fillColor: data[i].cellColor
            };

            cell = new google.maps.Polygon(cellOptions);
            attachCountWindow(cell, data[i].count);
            cells.push(cell);
        }
    }

    var infowindows = [];

    function attachCountWindow(theCell, theCount) {
        var infowindow = new google.maps.InfoWindow({
            content: "<span>" + theCount + "</span>",
            size: new google.maps.Size(75, 75)
        });
        infowindows.push(infowindow);
        google.maps.event.addListener(theCell, 'click', function (event) {
            for (i in infowindows) {
                infowindows[i].close();
            }
            infowindow.setPosition(event.latLng);
            infowindow.open(map);
        });
    }


    $("input[name=clear]:button").click(clearMap);
    function clearMap (event) {
        for (c in cells) {
            cells[c].setMap(null);
        }
        for (i in infowindows) {
            infowindows[i].close();
        }
    };
}
