const fs = require("fs");
const { parse } = require("csv-parse");
const moment = require("moment");

// 0: provider: 'check',
// 1: 'check:a9a36819-c021-429f-a0a8-78c868a92d46',
// 2: rental_start: '2022-11-30 18:40:14.95493',
// 3: rental_end: '2022-11-30 19:07:45.948693',
// 4: '53218279',
// 5: '1',
// 6: lat_start: '51.90731',
// 7: lng_start: '4.47333',
// 8: lat_end: '51.89153',
// 9: lng_end: '4.49378',

let allCsvData = [], aggregatedPerWeek = [];

const aggregatePerWeek = (data) => {
  data.forEach(x => {
    const weekNumber = moment(x[2]).isoWeek();
    const provider = x[0];
    if(! aggregatePerWeek[weekNumber]) aggregatePerWeek[weekNumber] = [];
    if(! aggregatePerWeek[weekNumber][provider]) aggregatePerWeek[weekNumber][provider] = 0;
    aggregatePerWeek[weekNumber][provider]++;
  });
  return aggregatePerWeek;
}

fs.createReadStream("./import/20221201_export_trips_Kievitslaan-2022-06-01-to-2022-12-01.csv")
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
    console.log(row);
    allCsvData.push(row);
  })
  .on("end", function () {
    console.log("finished");
    const perWeek = aggregatePerWeek(allCsvData);
    console.log(perWeek)
  })
  .on("error", function (error) {
    console.log(error.message);
  });
