var $ = document.querySelector.bind(document);

// go to https://www.transtats.bts.gov/DL_SelectFields.asp

// set fields


var fields = ['YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'OP_UNIQUE_CARRIER', 'ORIGIN', 'ORIGIN_CITY_NAME', 
'DEST', 'DEST_CITY_NAME', 'DEP_TIME', 'DEP_DELAY', 'DEP_DEL15', 'ARR_TIME', 'ARR_DELAY', 'ARR_DEL15', 'AIR_TIME', 'DISTANCE',
'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY',

// unclick
'ORIGIN_AIRPORT_ID',
'ORIGIN_AIRPORT_SEQ_ID',
'ORIGIN_CITY_MARKET_ID',
'DEST_AIRPORT_ID',
'DEST_AIRPORT_SEQ_ID',
'DEST_CITY_MARKET_ID'
];

fields.forEach(name => {
    let input = $('input[value='+name+']');
    if(!input) console.error(name + ' field does not exit!');
    input.click();
})


function range(start, stop, step) {
    var a = [start], b = start;
    while (b < stop) {
        a.push(b += step || 1);
    }
    return a;
}

var years = range(2003, 2010); //XYEAR
years = years.reverse();

var months = range(1, 12); // FREQUENCY
var targets = [];

years.forEach(year => {
    months.forEach(month => {
        targets.push([year, month]);
    })
})

var p = 0;

function run() {
    if(p >= targets.length) {
        clearInterval(timer);
        return;
    }
    var y = targets[p][0];
    var m = targets[p][1];
    $('#XYEAR').value = y;
    $('#FREQUENCY').value = m;

    console.log('downloading ', y, ' ', m);
    tryDownload();
    p += 1;
}

var timer = setInterval(run, 60 * 10 * 1000);
run();
