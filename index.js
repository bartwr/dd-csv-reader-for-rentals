const fs = require("fs");
const { parse } = require("csv-parse");
const moment = require("moment");
const { stringify } = require("csv-stringify");

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

const fileNameToImport = '20221201_export_trips_Kievitslaan-2022-06-01-to-2022-12-01.csv';

let allCsvData = [], aggregatedPerWeek = [];

const providers = [
  "check",
  "felyx",
  "gosharing",
  "donkey",
  "baqme"
];

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

const generateCsvDataPerWeek = (data) => {
  let csvData = [];
  Object.keys(data).forEach(key => {
    let row = [], providerCounts = data[key];
    row.week_number = key;
    Object.keys(providerCounts).forEach((provider) => {
      row[provider] = providerCounts[provider];
    });

    csvData.push(row);
  })
  return csvData;
}

const writeToFile = (data) => {
  const filename = "./export/"+fileNameToImport;
  const writableStream = fs.createWriteStream(filename);

  const columns = [
    "week_number",
    "check",
    "felyx",
    "gosharing",
    "donkey",
    "baqme",
    "lime"
  ];

  const stringifier = stringify({ header: true, columns: columns });

  const getRowDataForCsv = (row) => {
    let rowDataForCsv = [];
    rowDataForCsv.push(row.week_number);
    // Loop all providers
    providers.forEach(providerName => {
      // Loop all row values and check if provider is in it
      let didFoundValue = false;
      Object.keys(row).forEach(key => {
        const val = row[key];
        if(key === providerName) {
          rowDataForCsv.push(val);
          didFoundValue = true;
        }
      });
      if(! didFoundValue) {
        rowDataForCsv.push(0);
      }
    });
    return rowDataForCsv;
  }

  Object.keys(data).forEach(key => {
    const rowDataForCsv = getRowDataForCsv(data[key]);
    stringifier.write(rowDataForCsv);
  });

  stringifier.pipe(writableStream);
  console.log("Finished writing data");
}

fs.createReadStream("./import/"+fileNameToImport)
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
    allCsvData.push(row);
  })
  .on("end", function () {
    console.log("finished");
    const perWeek = aggregatePerWeek(allCsvData);
    const csvDataPerWeek = generateCsvDataPerWeek(perWeek);
    writeToFile(csvDataPerWeek);
  })
  .on("error", function (error) {
    console.log(error.message);
  });
