/* A JavaScript code for crawling datasets from transtats.bts.gov 

As an example, let's download the carrier on-time performance dataset.

1) Go to https://www.transtats.bts.gov/tables.asp?Table_ID=236&SYS_Table_Name=T_ONTIME_REPORTING.
2) Click on the "Download" button below the dataset description.
3) Change the code (the `fields` variable) below to choose the field names that you want to include.
Note that the field name (in the database) is different from the name on the web page.
You should right-click on the checkbox next to each field and click on "Inspect". 
The value at the "value" attribute of the <input> tag is the name you want.
4) Set up the delay between downloads (default: 10 minutes) 
4) Copy this script to the DevTools console and run.

NOTE: you cannot download multiple files at the same time. The website seems to check your session information.

*/

var $ = document.querySelector.bind(document);

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

var delay = 10 * 60 * 1000; // 10 minutes

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

var timer = setInterval(run, delay);
run();
