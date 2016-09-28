


var mymap = L.map('mapid').setView([40.7828, -73.9500], 12);
L.tileLayer('https://api.mapbox.com/styles/v1/kristiyanto/citktoe5o000e2iqxokgem2ic/tiles/256/{z}/{x}/{y}?access_token=pk.eyJ1Ijoia3Jpc3RpeWFudG8iLCJhIjoiY2l0a3Q1b2VmMGNuaTJubnZjOWN3NWp0dSJ9.9en-5YLbMPvG7hnoGt35XA', {
    //attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
    maxZoom: 18,
    id: 'insight',
    accessToken: 'pk.eyJ1Ijoia3Jpc3RpeWFudG8iLCJhIjoiY2l0a3Q1b2VmMGNuaTJubnZjOWN3NWp0dSJ9.9en-5YLbMPvG7hnoGt35XA'
}).addTo(mymap);

