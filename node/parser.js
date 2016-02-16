_ = require('lodash')

function checkNested(obj /*, level1, level2, ... levelN*/) {
  var args = Array.prototype.slice.call(arguments, 1);
  for (var i = 0; i < args.length; i++) {
    if (!obj || !obj.hasOwnProperty(args[i])) {
      return false;
    }
    obj = obj[args[i]];
  }
  return true;
}

function parser(personId, json) {
  console.log(_.keys(json))
  _.forEach(json.persons, function(person){
    console.log(_.keys(person))
    if (person.id == personId) {
      console.log(person.facts)
    }
  });
}

module.exports = parser
