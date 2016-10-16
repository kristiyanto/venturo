


var mymap = L.map('mapid').setView([41.8781, -87.6298], 13);
L.tileLayer('https://api.mapbox.com/styles/v1/kristiyanto/citktoe5o000e2iqxokgem2ic/tiles/256/{z}/{x}/{y}?access_token=pk.eyJ1Ijoia3Jpc3RpeWFudG8iLCJhIjoiY2l0a3Q1b2VmMGNuaTJubnZjOWN3NWp0dSJ9.9en-5YLbMPvG7hnoGt35XA', {
    //attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
    maxZoom: 18,
    id: 'insight',
    accessToken: 'pk.eyJ1Ijoia3Jpc3RpeWFudG8iLCJhIjoiY2l0a3Q1b2VmMGNuaTJubnZjOWN3NWp0dSJ9.9en-5YLbMPvG7hnoGt35XA'
}).addTo(mymap);

var distance = Morris.Bar({
    element: 'morris-area-chart',
    xkey: 'id',
    ykeys: ['trip_distance'],
    labels: ['name', 'trip_distance'],
    pointSize: 2,
    hideHover: 'auto',
    resize: true
});

var waitTime = Morris.Area({
    element: 'morris-area-chart2',
    xkey: 'id',
    ykeys: ['ptime'],
    labels: ['name', 'ptime'],
    pointSize: 2,
    hideHover: 'auto',
    resize: true
});