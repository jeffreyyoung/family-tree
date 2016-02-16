var mongoose = require("mongoose");
var parse = require("./parser");
var lineReader = require('readline').createInterface({
  input: require('fs').createReadStream('testdata.txt')
});
var hasDoneOne = false;
lineReader.on('line', function (line) {
  if (!hasDoneOne) {
      var personId = line.substring(0, 23);
      parse(personId, JSON.parse(line.substring(24)))
      hasDoneOne = true
  }
});
